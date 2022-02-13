from re import search
import botocore.exceptions

from functools import lru_cache
from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.document import DocumentHelper


class WdFilter:
    def __init__(self, userquery=None, foldernames=[], folderpattern=None) -> None:
        self.userquery = userquery
        self.foldernames = foldernames
        self.folderpattern = folderpattern

    def is_user_matching_query(self, username):
        # Still no docs on how the userquery works, so here's what we do
        return self.userquery is None or self.userquery == username


class WdDirectory:
    actionable_activity_types = [
        "DOCUMENT_RENAMED",
        "DOCUMENT_VERSION_UPLOADED",
        "DOCUMENT_VERSION_DELETED",
        "DOCUMENT_RECYCLED",
        "DOCUMENT_RESTORED",
        "DOCUMENT_REVERTED",
        "DOCUMENT_MOVED",
        "FOLDER_CREATED",
        "FOLDER_DELETED",
        "FOLDER_RENAMED",
        "FOLDER_RECYCLED",
        "FOLDER_RESTORED",
        "FOLDER_MOVED",
    ]

    def __init__(self, organization_id: str, clients: AwsClients) -> None:
        self.organization_id = organization_id
        self.clients = clients

    def generate_users(self, filter: WdFilter = None, include_all: bool = False):
        request = {"OrganizationId": self.organization_id}
        if filter is not None and filter.userquery is not None:
            request["Query"] = filter.userquery
        if include_all:
            request["Include"] = "ALL"
        client = self.clients.docs_client()
        while True:
            response = client.describe_users(**request)
            if "Users" in response:
                for u in response["Users"]:
                    yield u
            if "Marker" in response:
                request["Marker"] = response["Marker"]
            else:
                break

    def generate_activities(
        self, start_time, activity_types=actionable_activity_types, limit=None
    ):
        request = {"OrganizationId": self.organization_id, "StartTime": start_time}
        if activity_types is not None:
            if isinstance(activity_types, str):
                request["ActivityTypes"] = activity_types
            elif isinstance(activity_types, list):
                request["ActivityTypes"] = ",".join(activity_types)
            else:
                raise RuntimeWarning(
                    "Activity types not of type being handled. Ignoring parameter and passing all acitivies"
                )
        # if limit is not None:
        #     request["Limit"] = limit
        client = self.clients.docs_client()
        itemcount = 0
        while True:
            response = client.describe_activities(**request)
            if "UserActivities" in response:
                for a in response["UserActivities"]:
                    yield a
                    if limit is not None:
                        itemcount += 1
                        if itemcount >= limit:
                            return
            if "Marker" in response:
                request["Marker"] = response["Marker"]
            else:
                break


class WorkdocsFolderTree:
    def __init__(self, clients: AwsClients, collect_folders=False) -> None:
        self.clients = clients
        self.collect_folders = collect_folders
        if self.collect_folders:
            self.folders = set()

    def _add_to_folders(self, folderid):
        if self.collect_folders:
            self.folders.add(folderid)

    def generate_subfolders(self, folderid):
        # if folderid in self.folders:
        #     return
        self._add_to_folders(folderid)
        request = {"FolderId": folderid, "Type": "FOLDER"}
        client = self.clients.docs_client()
        while True:
            response = client.describe_folder_contents(**request)
            for f in response["Folders"]:
                if f["ResourceState"] == "ACTIVE":
                    yield f
                    yield from self.generate_subfolders(f["Id"])
            if "Marker" in response:
                request["Marker"] = response["Marker"]
            else:
                break


class S3FolderTree:
    def __init__(self, clients: AwsClients, bucket, prefix) -> None:
        self.clients = clients
        self.bucket = bucket
        self.prefix = prefix
        self.already_generated = set()

    def isrootfolder(self, folderinfo):
        metadata = folderinfo.get("Metadata", None)
        return (
            metadata is not None
            and "CreatorId" in metadata
            and metadata["CreatorId"] == metadata["ParentFolderId"]
        )

    def folderinfokey(self, folderid):
        return f"{self.prefix}/{folderid}/{DocumentHelper.FOLDERINFONAME}"

    def folderinfo(self, folderid):
        client = self.clients.bucket_client()
        metadata = {}
        try:
            response = client.head_object(
                Bucket=self.bucket, Key=self.folderinfokey(folderid)
            )
            metadata = DocumentHelper.folder_metadata_s32dict(response["Metadata"])
        except s3client.exceptions.NoSuchKey:  # type: ignore
            pass  # falling back on empty metadata
        except botocore.exceptions.ClientError as err:
            # NOTE: This case is required because of https://github.com/boto/boto3/issues/2442
            if err.response["Error"]["Code"] == "404":
                pass  # falling back on empty metadata
        return {"Metadata": metadata}

    def generate_folderinfos(self, folderid):
        if folderid in self.already_generated:
            return
        folderinfo = self.folderinfo(folderid)
        self.already_generated.add(folderid)
        if "ParentFolderId" in folderinfo.get("Metadata", {}) and not self.isrootfolder(
            folderinfo
        ):
            yield from self.generate_folderinfos(
                folderinfo["Metadata"]["ParentFolderId"]
            )
        yield folderinfo

    def generate_folders(self):
        lister = Listings(self.clients)
        s3folderids = lister.list_s3_subfoldernames(self.bucket, self.prefix)
        for fid in s3folderids:
            yield from self.generate_folderinfos(fid)


class Listings:
    def __init__(self, clients: AwsClients) -> None:
        self.clients = clients

    def list_wd_folder(self, folderid):
        request = {"FolderId": folderid, "Type": "ALL"}
        documents = []
        folders = []
        client = self.clients.docs_client()
        while True:
            response = client.describe_folder_contents(**request)
            documents.extend(
                [d for d in response["Documents"] if d["ResourceState"] == "ACTIVE"]
            )
            folders.extend(
                [d for d in response["Folders"] if d["ResourceState"] == "ACTIVE"]
            )
            if "Marker" in response:
                request["Marker"] = response["Marker"]
            else:
                return {"Documents": documents, "Folders": folders}

    def list_wd_documents(self, folderid):
        request = {"FolderId": folderid, "Type": "DOCUMENT"}
        documents = []
        client = self.clients.docs_client()
        while True:
            response = client.describe_folder_contents(**request)
            documents.extend(
                [d for d in response["Documents"] if d["ResourceState"] == "ACTIVE"]
            )
            if "Marker" in response:
                request["Marker"] = response["Marker"]
            else:
                return documents

    def list_s3_documents(self, bucket, folderprefix):
        request = {"Bucket": bucket, "Prefix": folderprefix}
        return self.list_s3_objects(request)

    def list_s3_folders(self, bucket, prefix):
        slashed_prefix = (prefix if prefix.endswith("/") else prefix + "/").lstrip("/")
        request = {"Bucket": bucket, "Prefix": slashed_prefix, "Delimiter": "/"}
        return self.list_s3_objects(request)

    def list_s3_subfoldernames(self, bucket, prefix):
        s3_objects = self.list_s3_folders(bucket, prefix)
        folderkeys = [
            o["Prefix"]
            for o in s3_objects
            if "Prefix" in o and o["Prefix"].endswith("/")
        ]
        return [
            Listings._extract_subfoldername(prefix.strip("/"), k) for k in folderkeys
        ]

    @staticmethod
    def _extract_subfoldername(unslashed_prefix, key):
        return search(f"({unslashed_prefix})/(.*)/([^/]*)", key).groups(0)[1]

    def list_s3_objects(self, request):
        objects = []
        client = self.clients.bucket_client()
        while True:
            response = client.list_objects_v2(**request)
            if "Delimiter" in request:
                objects.extend(response["CommonPrefixes"])
            else:
                if "Contents" in response:
                    objects.extend(response["Contents"])
            if response["IsTruncated"]:
                request["ContinuationToken"] = response["NextContinuationToken"]
            else:
                return objects


class WdItemApexOwner:
    def __init__(self, clients: AwsClients, users: list, result_map: dict) -> None:
        self.clients = clients
        self.users = users
        self.result_map = result_map
        self.root_folder_map = {
            u["RootFolderId"]: result_map[u["Id"]] for u in self.users
        }

    @lru_cache(maxsize=1024)
    def get_by_document_id(self, document_id: str):
        doc_request = {"DocumentId": document_id}
        doc_def = self.clients.docs_client().get_document(**doc_request)
        folder_id = doc_def.get("Metadata", {}).get("ParentFolderId", None)
        return self.root_folder_map.get(folder_id, None) or self.get_by_folder_id(
            folder_id
        )

    @lru_cache(maxsize=1024)
    def get_by_folder_id(self, folder_id: str):
        if folder_id in self.root_folder_map:
            return self.root_folder_map[folder_id]
        folder_request = {"FolderId": folder_id}
        folder_def = self.clients.docs_client().get_folder(**folder_request)
        parent_id = folder_def["Metadata"]["ParentFolderId"]
        if parent_id.startswith("S"):
            # Seems we have treed ourselves. Let's see what we can do
            if parent_id in self.result_map:
                return self.result_map[parent_id]
            else:
                raise RuntimeError(
                    f"Stuck on folder {folder_id} with parent {parent_id}"
                )
        return self.root_folder_map.get(parent_id, None) or self.get_by_folder_id(
            parent_id
        )
