import requests
from tempfile import TemporaryFile
import logging
import datetime

from yaml import dump
from botocore.exceptions import ClientError
import botocore.exceptions

# from botocore.exceptions import EntityNotExistsException

from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.document import DocumentHelper
from workdocs_dr.listings import Listings
from workdocs_dr.user import UserHelper, UserKeyHelper


class WorkDocs2BucketSync:
    """
    Class to sync a given list of folders for a given user from WorkDocs to S3 Bucket.
    This is the "worker" class
    """

    def __init__(
        self, clients: AwsClients, user: UserHelper, userkeys: UserKeyHelper
    ) -> None:
        self.clients = clients
        self.listings = Listings(self.clients)
        self.user = user
        self.userkeys = userkeys

    def bucket_documentkey(self, folder_id, document_id):
        return self.userkeys.bucket_documentkey(folder_id, document_id)

    # def backupfolders(self, folderlist):
    #     """Backs up a list of folders for a user. This is the level at which dask parallelizes"""
    #     results = []
    #     logging.debug(f"Start of folderlist")
    #     for folder in folderlist:
    #         actions = self.get_folder_syncactions(folder)
    #         results.extend([act() for act in actions])
    #     logging.debug(f"End of folderlist")
    #     return results

    # def get_folder_syncactions(self, folderid):
    #     wdfolder = self.listings.list_wd_folder(folderid)
    #     wddocuments = wdfolder["Documents"]
    #     wdfolders = wdfolder["Folders"]
    #     s3documents = self.listings.list_s3_documents(self.userkeys.bucket, self.userkeys.bucket_folderprefix(folderid))
    #     actions = self.make_syncactions(folderid, wdfolders, wddocuments, s3documents)
    #     return actions

    def get_folder_syncactions(self, folder_def, wdfolders, wddocuments):
        s3documents = self.listings.list_s3_documents(
            self.userkeys.bucket, self.userkeys.bucket_folderprefix(folder_def["Id"])
        )
        actions = self.make_syncactions(folder_def, wdfolders, wddocuments, s3documents)
        return actions

    def make_syncactions(self, folder_def, wdfolders, wddocuments, s3documents):
        folder_id = folder_def["Id"]
        wds = {d["Id"]: d for d in wddocuments}
        s3s = {k["Key"].split("/")[-1]: k for k in s3documents if "/" in k["Key"]}
        common = set(wds.keys()).intersection(s3s.keys())
        s3only = set(s3s.keys()).difference(wds.keys())
        wdonly = set(wds.keys()).difference(s3s.keys())
        insertreqs = [
            {
                "folder_id": doc["ParentFolderId"],
                "document_id": id,
                "version_id": doc["LatestVersionMetadata"]["Id"],
            }
            for id, doc in wds.items()
            if id in wdonly
        ]
        updatereqs = [
            {
                "folder_id": doc["ParentFolderId"],
                "document_id": doc["Id"],
                "version_id": doc["LatestVersionMetadata"]["Id"],
            }
            for id, doc in wds.items()
            if id in common
            and (
                wds[id]["LatestVersionMetadata"]["Size"] != s3s[id]["Size"]
                or wds[id]["LatestVersionMetadata"]["ModifiedTimestamp"]
                > s3s[id]["LastModified"]
            )
        ]

        deletions = [
            lambda f=folder_id, s=s3id: self.remove_from_bucket(f, s)
            for s3id in s3only
            if s3id != DocumentHelper.FOLDERINFONAME
        ]
        inserts = [
            lambda r=req: self.copy_to_bucket(**r) for req in insertreqs + updatereqs
        ]
        actions = deletions + inserts
        writenewfolderinfo = actions or (
            DocumentHelper.FOLDERINFONAME not in s3s and (wdfolders or wddocuments)
        )
        folder_lastmodified = max(
            [datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)]
            + [folder_def["ModifiedTimestamp"]]
            + [f["ModifiedTimestamp"] for f in wdfolders]
        )
        writenewfolderinfo = writenewfolderinfo or (
            DocumentHelper.FOLDERINFONAME in s3s
            and s3s[DocumentHelper.FOLDERINFONAME]["LastModified"]
            <= folder_lastmodified
        )
        if writenewfolderinfo:
            actions.append(
                lambda f=folder_id, w=wdfolders, d=wddocuments: self.update_folder_summary(
                    f, w, d
                )
            )
            logging.info(f"Doing {len(actions)} on folder {folder_id}")
        else:
            logging.debug(f"Skipped folder {folder_id}")
        return actions

    def sync_document_to_bucket(
        self, document_id, folder_id=None, version_id=None, old_folder_ids=[]
    ):
        """Checks status of document in WorkDocs. returns a delete or copy depending on status.
        `old_folder_ids` is used to indicated folders the document might have been moved out of"""
        wdrequest = {
            "DocumentId": document_id,
        }
        client = self.clients.docs_client()
        try:
            wdresponse = client.get_document(**wdrequest)
            wdmetadata = wdresponse["Metadata"]
            if wdmetadata["ResourceState"] in ["RECYCLING", "RECYCLED"]:
                # TODO: We mostly don't have a good idea of where the document was deleted from. The activity event is not useful and neither is the history of document versions
                # The wdmetadata will usually hold the location of the recycle-bin, so no idea where it was previously
                guess_folder_id = folder_id or wdmetadata.get("ParentFolderId", None)
                logging.info(
                    f"Would like to delete document {wdmetadata} from location {self.userkeys.bucket_documentkey(folder_id, document_id)} or maybe {self.userkeys.bucket_documentkey(guess_folder_id, document_id)}"
                )
                if guess_folder_id is not None:
                    return self.remove_from_bucket(
                        folder_id=folder_id, document_id=document_id
                    )
            v_id = version_id or wdmetadata["LatestVersionMetadata"]["Id"]
            f_id = folder_id or wdmetadata["ParentFolderId"]
            s3headrequest = {
                "Bucket": self.userkeys.bucket,
                "Key": self.userkeys.bucket_documentkey(f_id, document_id),
            }
            s3client = self.clients.bucket_client()
            try:
                s3response = s3client.head_object(**s3headrequest)
                s3metadata = DocumentHelper.metadata_s32dict(
                    s3response.get("Metadata", {})
                )
            except s3client.exceptions.NoSuchKey:  # type: ignore
                s3metadata = {}
            except botocore.exceptions.ClientError as err:
                # NOTE: This case is required because of https://github.com/boto/boto3/issues/2442
                if err.response["Error"]["Code"] == "404":
                    s3metadata = {}
            should_copy = wdmetadata["LatestVersionMetadata"]["Size"] != s3metadata.get(
                "Size", -1
            ) or wdmetadata["LatestVersionMetadata"][
                "ModifiedTimestamp"
            ] > s3metadata.get(
                "ModifiedTimestamp",
                datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
            )

            clear_from_folders = [f for f in old_folder_ids if f != f_id]
            removes = [
                self.remove_from_bucket(f, document_id) for f in clear_from_folders
            ]
            if not should_copy:
                return {}
            return self.copy_to_bucket(f_id, document_id, v_id)
        except (
            client.exceptions.EntityNotExistsException,
            client.exceptions.UnauthorizedResourceAccessException,
        ):
            if folder_id is not None:
                return self.remove_from_bucket(
                    folder_id=folder_id, document_id=document_id
                )
            else:
                return {
                    "CustomStatus": "Could not determine where to remove {document_id=} from"
                }
        except:
            raise

    def remove_folder_from_bucket(self, folder_id):
        folder_prefix = self.userkeys.bucket_folderprefix(folder_id)
        documents = self.listings.list_s3_documents(self.userkeys.bucket, folder_prefix)
        # Not bothering to make individual deletes async here
        results = []
        for doc in documents:
            document_key = doc["Key"]
            request = {"Bucket": self.userkeys.bucket, "Key": document_key}
            response = self.clients.bucket_client().delete_object(**request)
            results.append(response)
        return results

    def remove_from_bucket(self, folder_id, document_id):
        """Removes object from bucket idempotently (i.e. no error if object was already deleted)"""
        documentpath = self.userkeys.bucket_documentkey(folder_id, document_id)
        request = {"Bucket": self.userkeys.bucket, "Key": documentpath}
        return self.clients.bucket_client().delete_object(**request)

    def copy_to_bucket(self, folder_id, document_id, version_id):
        """Copies specific version of document to bucket"""

        def cleaned_metadata(wdr):
            metda = wdresponse.get("Metadata", {})
            return {**metda, **{"Source": {}}}

        wdrequest = {
            "DocumentId": document_id,
            "VersionId": version_id,
            "Fields": "SOURCE",
        }
        wdresponse = self.clients.docs_client().get_document_version(**wdrequest)
        metadata = DocumentHelper.metadata_dict2s3(wdresponse["Metadata"])
        s3request = {
            "Bucket": self.userkeys.bucket,
            "Key": self.userkeys.bucket_documentkey(folder_id, document_id),
        }
        documentdownloadurl = wdresponse["Metadata"]["Source"]["ORIGINAL"]
        logging.info(
            f"Uploading document to {s3request=} from {cleaned_metadata(wdresponse)}"
        )
        r = requests.get(documentdownloadurl, stream=True)
        content_length = (
            int(r.headers["content-length"])
            if "content-length" in r.headers
            else 1_000_000
        )
        bucket_client = self.clients.bucket_client()
        content_type = (
            metadata.get("ContentType", None)
            or metadata.get("content_type", None)
            or "application/octet-stream"
        )
        if content_length > 1_000_000:
            with TemporaryFile() as fp:
                for chunk in r.iter_content(8192):
                    fp.write(chunk)
                fp.seek(0)
                response = bucket_client.upload_fileobj(
                    fp,
                    ExtraArgs={"Metadata": metadata, "ContentType": content_type},
                    **s3request,
                )
                return response
        else:
            responsebytes = r.content
            response = bucket_client.put_object(
                Body=responsebytes,
                Metadata=metadata,
                ContentType=content_type,
                **s3request,
            )
            return response

    def update_folder_summary(self, folder_id, wdfolders=[], wddocuments=[]):
        infodump = dict()
        # Optional info on documents and subfolders
        infodump["Documents"] = [
            {"Id": d["Id"], "Name": d["LatestVersionMetadata"]["Name"]}
            for d in wddocuments
        ]
        infodump["Folders"] = [{"Id": f["Id"], "Name": f["Name"]} for f in wdfolders]
        s3request = {
            "Bucket": self.userkeys.bucket,
            "Key": self.userkeys.bucket_documentkey(
                folder_id, DocumentHelper.FOLDERINFONAME
            ),
        }
        wdresponse = self.clients.docs_client().get_folder(FolderId=folder_id)
        metadata = DocumentHelper.metadata_dict2s3(wdresponse["Metadata"])
        if metadata.get("ResourceState", metadata.get("resource_state", None)) in [
            "RECYCLING",
            "RECYCLED",
        ]:
            response = self.clients.bucket_client().delete_object(**s3request)
            return response
        dirinfo = dump(infodump).encode("utf-8")
        response = self.clients.bucket_client().put_object(
            Body=dirinfo, Metadata=metadata, **s3request
        )
        return response

    def update_user_info(self):
        userinfokey = self.userkeys.bucket_folderprefix(DocumentHelper.USERINFONAME)
        lastuserupdate = self.user.modified_timestamp
        client = self.clients.bucket_client()
        update_user_info = False
        # Uhm, yeah. I am myself horrified, but alternatives are also not great :)
        try:
            s3_object_info = client.head_object(
                Bucket=self.userkeys.bucket,
                Key=userinfokey,
                IfModifiedSince=lastuserupdate,
            )
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ["304", "404"]:
                update_user_info = True
        if update_user_info:
            contents = dump(vars(self.user)).encode("utf-8")
            metadata = DocumentHelper.metadata_dict2s3(vars(self.user))
            response = client.put_object(
                Bucket=self.userkeys.bucket,
                Key=userinfokey,
                Body=contents,
                Metadata=metadata,
            )
            pass
