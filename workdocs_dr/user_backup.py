import logging
import queue
from timeit import default_timer as timer

from workdocs_dr.user import UserHelper, UserKeyHelper
from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.listings import Listings, S3FolderTree, WdFilter
from workdocs_dr.workdocs_bucket_sync import WorkDocs2BucketSync
from workdocs_dr.queue_backup import ListWorkdocsFolders, RecordSyncTasks, RunSyncTasks


class UserBackupRunner():
    """
    This coordinates the sync from workdocs to s3 bucket of documents for a single user.
    """

    "Number of folders to preload for processing"
    chunksize = 100
    starttime = timer()

    def __init__(self, user: UserHelper, userkeys: UserKeyHelper, clients: AwsClients) -> None:
        self.userhelper = user
        self.userkeyhelper = userkeys
        self.clients = clients

    def backup_user_queue(self, filter: WdFilter = None):
        br = WorkDocs2BucketSync(self.clients, self.userhelper, self.userkeyhelper)
        br.update_user_info()
        folder_queue = queue.Queue()
        action_queue = queue.Queue()
        # TODO: Make filter work
        foldertree = ListWorkdocsFolders(self.clients, collect_folders=True, downstream_queue=folder_queue)
        record_st = RecordSyncTasks(self.clients, self.userhelper, self.userkeyhelper,
                                    task_queue=folder_queue, downstream_queue=action_queue)
        run_st = RunSyncTasks(task_queue=action_queue)
        foldertree.start_walk(self.userhelper.root_folder_id)
        record_st.start_recording()
        run_st.start_syncing()

        foldertree.finish_walk()
        folder_queue.put(None)
        record_st.finish_recording()
        action_queue.put(None)
        run_st.finish_syncing()
        results = run_st.results
        # TODO: Something to clear out deleted folders goes here
        if foldertree.collect_folders and filter.folderpattern is None and \
                (filter.foldernames is None or len(filter.foldernames) == 0):
            self.prune_inactive_folders(br, foldertree.folders)

        return results

    def prune_inactive_folders(self, syncer: WorkDocs2BucketSync, active_folders: set):
        lister = Listings(self.clients)
        s3folderids = lister.list_s3_subfoldernames(self.userkeyhelper.bucket, self.userkeyhelper.bucket_userprefix())
        actions = []
        for folder_id in s3folderids:
            if not folder_id in active_folders:
                logging.info(f"Would like to clear out folder {folder_id} for user {self.userhelper.username}")
                actions.append(lambda fid=folder_id: syncer.remove_folder_from_bucket(fid))

    def folderidfrompath(self, path):
        """Need this later for implementing filters"""
        subfolderlist = path.strip('/').split('/')
        currentfolderid = self.userhelper.root_folder_id
        client = self.clients.docs_client()
        for folder in subfolderlist:
            response = client.describe_folder_contents(
                FolderId=currentfolderid)
            matchingfolder = next(
                (d for d in response["Folders"] if d["Name"] == folder), None)
            if matchingfolder is None:
                raise RuntimeError("Bad path")
            currentfolderid = matchingfolder["Id"]
        return currentfolderid
