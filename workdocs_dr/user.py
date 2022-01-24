from urllib.parse import urlparse


class UserHelper:
    def __init__(self, wduser) -> None:
        self.organization_id = wduser["OrganizationId"]
        self.username = wduser["Username"]
        self.root_folder_id = wduser["RootFolderId"]
        self.modified_timestamp = wduser["ModifiedTimestamp"]


class UserKeyHelper():
    def __init__(self, userhelper: UserHelper, bucket_url: str) -> None:
        self.s3_fragments = urlparse(bucket_url)
        self.bucket = self.s3_fragments.hostname
        self.prefix = self.s3_fragments.path.strip("/")
        self.organization_id = userhelper.organization_id
        self.folder_user = userhelper.username

    @staticmethod
    def org_prefix(prefix, organization_id):
        return f"{prefix}/{organization_id}"

    def bucket_userprefix(self):
        return f"{UserKeyHelper.org_prefix(self.prefix, self.organization_id)}/{self.folder_user}"

    def bucket_folderprefix(self, folderid: str) -> str:
        return f"{self.bucket_userprefix()}/{folderid}"

    def bucket_documentkey(self, folderid: str, documentid: str) -> str:
        return f"{self.bucket_folderprefix(folderid)}/{documentid}"
