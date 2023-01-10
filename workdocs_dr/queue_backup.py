import logging
import queue
from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.listings import Listings
from workdocs_dr.queue_pool import QueueWorkPool
from workdocs_dr.user import UserHelper, UserKeyHelper
from workdocs_dr.workdocs_bucket_sync import WorkDocs2BucketSync


class ListWorkdocsFolders:
    worker_count = 4

    def __init__(
        self,
        clients: AwsClients,
        downstream_queue: queue.LifoQueue = None,
        collect_folders=False,
    ) -> None:
        self.clients = clients
        self.downstream_queue = downstream_queue
        self.collect_folders = collect_folders
        if self.collect_folders:
            self.folders = set()
        self._threads = []
        self.queue_walktree = queue.LifoQueue()
        logging.info("Using very new Tree implementation")
        self.listings = Listings(self.clients)
        self.queue_helper = None

    def _setup(self):
        # def task_work(folder_id, lock):
        #     subfolders = self.get_subfolderdefs(folder_id)
        #     if self.downstream_queue is not None:
        #         for fdef in subfolders:
        #             self.downstream_queue.put(fdef)
        #     for fdef in subfolders:
        #         self.queue_walktree.put(fdef["Id"])
        #     if self.collect_folders:
        #         with lock:
        #             self._add_to_folders(folder_id)
        def task_work(folder_def, lock):
            # Describe the folder
            # Place self metadata + list of folders/documents on downstream queue, and place subfolders on tree walking queue
            contents = self.listings.list_wd_folder(folder_def["Id"])
            if self.downstream_queue is not None:
                if contents.get("Folders") or contents.get("Documents"):
                    self.downstream_queue.put(
                        {"Metadata": folder_def, "Contents": contents}
                    )
            for subfolder_def in contents.get("Folders", []):
                self.queue_walktree.put(subfolder_def)
            if self.collect_folders:
                with lock:
                    self._add_to_folders(folder_def["Id"])

        self.queue_helper = QueueWorkPool(
            task_queue=self.queue_walktree,
            worker_count=self.worker_count,
            worker_action=task_work,
        )

    def start_walk(self, rootfolderid):
        self._setup()
        self.queue_helper.start_tasks()
        # Get root folder metadata so we can start the walk
        client = self.clients.docs_client()
        request = {"FolderId": rootfolderid}
        response = client.get_folder(**request)
        # And this starts the task work, which
        self.queue_walktree.put(response["Metadata"])

    def finish_walk(self):
        self.queue_helper.finish_tasks()

    def _add_to_folders(self, folderid):
        if self.collect_folders:
            self.folders.add(folderid)

    # def get_subfolderdefs(self, folderid):
    #     request = {"FolderId": folderid, "Type": "FOLDER"}
    #     retval = []
    #     client = self.clients.docs_client()
    #     while True:
    #         response = client.describe_folder_contents(**request)
    #         for f in response["Folders"]:
    #             if f["ResourceState"] == "ACTIVE":
    #                 retval.append(f)
    #         if "Marker" in response:
    #             request["Marker"] = response["Marker"]
    #         else:
    #             break
    #     return retval


class RecordSyncTasks:
    worker_count = 4

    def __init__(
        self,
        clients: AwsClients,
        user: UserHelper,
        userkeys: UserKeyHelper,
        task_queue: queue.Queue,
        downstream_queue: queue.Queue = None,
    ) -> None:
        self.clients = clients
        self.listings = Listings(self.clients)
        self.user = user
        self.userkeys = userkeys
        self.task_queue = task_queue
        self.downstream_queue = downstream_queue
        self.queue_helper = None

    def _setup(self):
        wd2bs = WorkDocs2BucketSync(self.clients, self.user, self.userkeys)
        # def task_work(fdef, lock):
        #     actions = wd2bs.get_folder_syncactions(fdef["Id"])
        #     if self.downstream_queue is not None:
        #         for act in actions:
        #             self.downstream_queue.put(act)
        #     if len(actions) > 0:
        #         with lock:
        #             logging.info(f"Discovered {len(actions)} sync items in folder {fdef['Name']} / {fdef['Id']}")

        def task_work(folder_data, lock):
            fdef = folder_data["Metadata"]
            actions = wd2bs.get_folder_syncactions(
                fdef,
                folder_data["Contents"].get("Folders", []),
                folder_data["Contents"].get("Documents", []),
            )
            if self.downstream_queue is not None:
                for act in actions:
                    self.downstream_queue.put(act)
            if actions:
                with lock:
                    logging.info(
                        f"Discovered {len(actions)} sync items in folder {fdef['Name']} / {fdef['Id']}"
                    )

        self.queue_helper = QueueWorkPool(
            task_queue=self.task_queue,
            worker_count=self.worker_count,
            worker_action=task_work,
        )

    def start_recording(self):
        self._setup()
        self.queue_helper.start_tasks()

    def finish_recording(self):
        self.queue_helper.finish_tasks()


class RunSyncTasks:
    worker_count = 6

    def __init__(self, task_queue: queue.Queue) -> None:
        self.task_queue = task_queue
        self.results = []
        self.queue_helper = None

    def _setup(self):
        def task_work(act, lock):
            result = act()
            if result is not None:
                with lock:
                    self.results.append(result)

        self.queue_helper = QueueWorkPool(
            self.task_queue, self.worker_count, worker_action=task_work
        )

    def start_syncing(self):
        self._setup()
        self.queue_helper.start_tasks()

    def finish_syncing(self):
        self.queue_helper.finish_tasks()
