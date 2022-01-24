
import logging
from collections import defaultdict
from datetime import datetime
import queue

from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.directory_minder import DirectoryBackupMinder
from workdocs_dr.listings import WdDirectory, WdItemApexOwner
from workdocs_dr.queue_backup import RunSyncTasks
from workdocs_dr.user import UserHelper, UserKeyHelper
from workdocs_dr.workdocs_bucket_sync import WorkDocs2BucketSync


class ActivityBackupRunner():
    """
    Pulls a list of activities, and syncs state of documents and folder that have been touched in
    in the time interval of the activities
    """

    def __init__(self, clients: AwsClients, organization_id: str, bucket_url: str, directory: WdDirectory, minder: DirectoryBackupMinder) -> None:
        self.clients = clients
        self.organization_id = organization_id
        self.bucket_url = bucket_url
        self.minder = minder
        self.directory = directory

    def backup_activity_queue(self):
        activity_start_time = self.minder.get_activities_cutoff()
        action_queue = queue.Queue()
        actitity_tasks = ActivityTasks(self.clients, self.organization_id,
                                       self.bucket_url, self.directory, activity_start_time)
        actitity_tasks.fill_queue(action_queue)
        run_st = RunSyncTasks(task_queue=action_queue)
        run_st.start_syncing()
        action_queue.put(None)
        run_st.finish_syncing()
        results = run_st.results
        return results


class ActivityTasks():
    def __init__(self, clients: AwsClients, organization_id: str, bucket_url: str, directory: WdDirectory, activity_start_time: datetime) -> None:
        self.clients = clients
        self.organization_id = organization_id
        self.bucket_url = bucket_url
        self.directory = directory
        self.activity_start_time = activity_start_time
        self.apex_syncer = None

    def get_apex_syncer(self):
        if self.apex_syncer is None:
            users = list(self.directory.generate_users(include_all=True))
            user_helpers = {u["Id"]: UserHelper(u) for u in users}
            user_keyhelpers = {uid: UserKeyHelper(uh, self.bucket_url) for uid, uh in user_helpers.items()}
            user_syncers = {u["Id"]: WorkDocs2BucketSync(
                self.clients, user_helpers[u["Id"]], user_keyhelpers[u["Id"]]) for u in users}
            self.apex_syncer = WdItemApexOwner(self.clients, users, user_syncers)
        return self.apex_syncer

    def get_consolidated_updates(self):
        activities = list(self.directory.generate_activities(self.activity_start_time))
        doc_finalevents = dict()  # Keyed by documentid, value is final event
        doc_moves = defaultdict(list)  # Keyed by documentid, value is list of move events
        folder_finalevents = dict()  # Keyed by folderid, value is final event
        folder_moves = defaultdict(list)  # Keyed by folderid, value is list of move events
        for a in activities:
            move_map = folder_moves if a["Type"].startswith("FOLDER_") else doc_moves
            item_map = folder_finalevents if a["Type"].startswith("FOLDER_") else doc_finalevents
            item_id = a["ResourceMetadata"]["Id"]
            if a["Type"].endswith("_MOVED"):
                move_map[item_id].append(a)

            if item_id in item_map:
                if a["TimeStamp"] >= item_map[item_id]["TimeStamp"]:
                    item_map = a
                continue
            item_map[item_id] = a

        # update folder descriptions
        folder_updates = [{"folder_id": k, "user": a["ResourceMetadata"]["Owner"]["Id"], "activity_type": a["Type"], }
                          for k, a in folder_finalevents.items()]

        # Patch old folders into document activities
        for doc_id, moves in doc_moves.items():
            old_folders = [a["OriginalParent"]["Id"] for a in moves]
            doc_finalevents[doc_id]["OldFolderIds"] = old_folders

        doc_updates = [{
            "document_id": k,
            "user": a["ResourceMetadata"]["Owner"]["Id"],
            "old_folder_ids": a.get("OldFolderIds", []),
            "activity_type": a["Type"],
        } for k, a in doc_finalevents.items()]
        return {"FolderUpdates": folder_updates, "DocumentUpdates": doc_updates}

    def create_sync_action(self, owner_guess: str, syncer_args, activity_type, folder_id=None, document_id=None):
        if document_id is None and folder_id is None:
            raise RuntimeError("folder_id and document_id can't both be absent")
        syncer = None
        syncer_mapper = self.get_apex_syncer()
        try:
            if document_id is not None:
                syncer = syncer_mapper.get_by_document_id(document_id)
            else:
                syncer = syncer_mapper.get_by_folder_id(folder_id)
        except (self.clients.docs_client().exceptions.EntityNotExistsException,
                self.clients.docs_client().exceptions.UnauthorizedResourceAccessException):
            logging.debug(
                f"Found missing item with possible owner guess. Data are {syncer_args=}, {folder_id=}, {document_id=}")
        if syncer is None:
            if owner_guess in syncer_mapper.result_map:
                syncer = syncer_mapper.result_map[owner_guess]
            else:
                return lambda args=syncer_args: {}
        if document_id is not None:
            return lambda args=syncer_args: syncer.sync_document_to_bucket(**args)
        # Seems like we have a folder
        if activity_type in ["FOLDER_DELETED", "FOLDER_RECYCLED"]:
            return lambda fid=folder_id: syncer.remove_folder_from_bucket(fid)
        return lambda args=syncer_args: syncer.update_folder_summary(**args)

    def fill_queue(self, downstream_queue: queue.Queue):
        limit = 5000
        actions = 0
        updates = self.get_consolidated_updates()
        for upd in updates["DocumentUpdates"]:
            act = self.create_sync_action(
                owner_guess=upd["user"],
                syncer_args={"document_id": upd["document_id"], "old_folder_ids": upd["old_folder_ids"]},
                activity_type=upd["activity_type"],
                document_id=upd["document_id"])
            downstream_queue.put(act)
            actions += 1
            if actions >= limit:
                return
        for upd in updates["FolderUpdates"]:
            act = self.create_sync_action(owner_guess=upd["user"],
                                          syncer_args={"folder_id": upd["folder_id"]},
                                          activity_type=upd["activity_type"],
                                          folder_id=upd["folder_id"])
            downstream_queue.put(act)
            actions += 1
            if actions >= limit:
                return
