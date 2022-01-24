import logging
import queue
from pathlib import Path
from urllib.parse import urlparse

from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.document import DocumentHelper
from workdocs_dr.queue_restore import GenerateRestoreTasks, RunRestoreTasks
from workdocs_dr.listings import S3FolderTree, WdFilter
from workdocs_dr.user import UserHelper, UserKeyHelper


class UserRestoreInfo:
    def __init__(self, clients: AwsClients, organization_id, bucket_url) -> None:
        self.clients = clients
        self.organization_id = organization_id
        self.bucket_url = bucket_url
        self.s3_fragments = urlparse(bucket_url)
        self.bucket = self.s3_fragments.hostname
        self.prefix = self.s3_fragments.path.strip("/")

    def userhelper_userkeyhelper_from_username(self, username):
        baseprefix = UserKeyHelper.org_prefix(self.prefix, self.organization_id)
        userinfokey = f"{baseprefix}/{username}/{DocumentHelper.USERINFONAME}"
        client = self.clients.bucket_client()
        s3_object_info = client.head_object(Bucket=self.bucket, Key=userinfokey)
        user_metadata = DocumentHelper.user_metadata_s32dict(s3_object_info["Metadata"])
        uh = UserHelper(user_metadata)
        ukh = UserKeyHelper(uh, self.bucket_url)
        return (uh, ukh)


class UserRestoreRunner:

    def __init__(self, user: UserHelper, userkeys: UserKeyHelper, clients: AwsClients, restore_path: Path) -> None:
        self.userhelper = user
        self.userkeyhelper = userkeys
        self.clients = clients
        self.restore_path = Path(restore_path)
        self.lost_and_found = self.restore_path / "lost and found"
        self.foldergenerator = None
        self.folderpaths = None

    def get_folderpath(self, folderinfo):
        if self.folderpaths is None:
            self.folderpaths = {folderinfo["Metadata"]["Id"]: self.restore_path}
            return self.restore_path
        parentpath = self.folderpaths.get(folderinfo["Metadata"]["ParentFolderId"], self.lost_and_found)
        path = parentpath / folderinfo["Metadata"]["Name"]
        self.folderpaths[folderinfo["Metadata"]["Id"]] = path
        return path

    def generate_restoredefs(self):
        # TODO: Implement handling of filters
        foldertree = S3FolderTree(self.clients, self.userkeyhelper.bucket, self.userkeyhelper.bucket_userprefix())
        for finfo in foldertree.generate_folders():
            yield {**finfo, **{"Path": self.get_folderpath(finfo), "FallbackBasePath": self.lost_and_found}}

    def restore_user_queued(self, filter: WdFilter = None):
        summary = []
        folder_queue = queue.Queue()
        file_queue = queue.Queue()
        grt = GenerateRestoreTasks(folder_queue=folder_queue, restore_file_queue=file_queue,
                                   clients=self.clients, userkeyhelper=self.userkeyhelper)
        rrt = RunRestoreTasks(restore_queue=file_queue, clients=self.clients, userkeyhelper=self.userkeyhelper)
        for restore_folder_def in self.generate_restoredefs():
            folder_queue.put(restore_folder_def)
        grt.start_generating()
        rrt.start_restoring()
        folder_queue.put(None)
        grt.finish_generating()
        file_queue.put(None)
        rrt.finish_restoring()
        summary = rrt.results
        return summary
