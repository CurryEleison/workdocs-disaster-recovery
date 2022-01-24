

import logging
from collections import defaultdict
from workdocs_dr.document import DocumentHelper
from workdocs_dr.item_restore import scribble_file
from workdocs_dr.listings import Listings
from workdocs_dr.queue_pool import QueueWorkPool


class GenerateRestoreTasks:
    worker_count = 2

    def __init__(self, folder_queue, restore_file_queue, clients, userkeyhelper) -> None:
        self.task_queue = folder_queue
        self.downstream_queue = restore_file_queue
        self.clients = clients
        self.userkeyhelper = userkeyhelper

        self.queue_helper = None

    def _setup(self):
        def task_work(folderdef, lock):
            try:
                path = folderdef["Path"]
                if path.is_file():
                    raise RuntimeError(f"Can't restore to directory {path} as it's a file!")
                is_folder_new = not path.is_dir()
                with lock:
                    path.mkdir(parents=True, exist_ok=True)
                folderid = folderdef["Metadata"]["Id"]
                folderprefix = self.userkeyhelper.bucket_folderprefix(folderid)
                lister = Listings(self.clients)
                s3objects = lister.list_s3_documents(self.userkeyhelper.bucket, folderprefix)
                # TODO:
                # - Only mark files skippable if a file with same size/date exists in destination dir
                if self.downstream_queue is not None:
                    task_list = self.create_task_list(folderpath=path, folder_id=folderid,
                                                      is_folder_new=is_folder_new, s3objects=s3objects)
                    for task in task_list:
                        self.downstream_queue.put(task)
                with lock:
                    for s3obj in s3objects:
                        logging.debug(s3obj)
            except Exception as err:
                logging.warning(err)
        self.queue_helper = QueueWorkPool(task_queue=self.task_queue, worker_count=self.worker_count,
                                          worker_action=task_work)

    def create_task_list(self, folderpath, folder_id, is_folder_new, s3objects):
        if is_folder_new:  # If folder is newly created all files will need to be fetched from source
            return [{"Path": folderpath, "S3Object": s3obj, "FolderId": folder_id, "FetchMetadataFirst": False} for s3obj in s3objects]
        # Looks like the folder was already there. See if we can supply the download process with
        # hints on when to do head requests first
        # Get stats for all files in the directory on disk (since we know the folder is already there)
        file_stats = [p.stat() for p in folderpath.iterdir() if p.is_file()]
        sizes = {stat.st_size for stat in file_stats}

        def matching_size_exists(s3obj):
            return s3obj.get("Size", -1) in sizes
        # If we see a file in destination dir with matching size it's worthwhile to fetch metadata first
        return [{"Path": folderpath, "S3Object": s3obj, "FolderId": folder_id, "FetchMetadataFirst": matching_size_exists(s3obj)} for s3obj in s3objects]

    def start_generating(self):
        self._setup()
        self.queue_helper.start_tasks()

    def finish_generating(self):
        self.queue_helper.finish_tasks()


class RunRestoreTasks:
    worker_count = 6

    def __init__(self, restore_queue, clients, userkeyhelper) -> None:
        self.task_queue = restore_queue
        self.clients = clients
        self.userkeyhelper = userkeyhelper
        self.results = []

    def _setup(self):
        def task_work(restoredef, lock):
            try:
                client = self.clients.bucket_client()
                s3obj = restoredef["S3Object"]
                path = restoredef["Path"]
                folder_id = restoredef["FolderId"]
                fetch_metadata_first = restoredef.get("FetchMetadataFirst", False)
                request_kwargs = {"Bucket": self.userkeyhelper.bucket, "Key": s3obj["Key"]}
                if s3obj["Key"] == self.userkeyhelper.bucket_documentkey(folder_id, DocumentHelper.FOLDERINFONAME):
                    return
                if s3obj["Size"] < 1_000_000:
                    # Just a straight get_object

                    head_request = None if not fetch_metadata_first else lambda: client.head_object(**request_kwargs)
                    def req(): return client.get_object(**request_kwargs)
                    def writer(r, f): return f.write(r["Body"].read())
                else:
                    def req(): return client.head_object(**request_kwargs)
                    head_request = req
                    def writer(_, f): return client.download_fileobj(self.userkeyhelper.bucket, s3obj["Key"], f)
                documentinfo = scribble_file(path, req, writer, head_request)
                self.results.append({**restoredef, **{"Status": "OK"}, **{"DocumentInfo": documentinfo}})
            except Exception as err:
                self.results.append({**restoredef, **{"Status": "Error", "ErrorInfo": err}})

        self.queue_helper = QueueWorkPool(task_queue=self.task_queue, worker_count=self.worker_count,
                                          worker_action=task_work)

    def start_restoring(self):
        self._setup()
        self.queue_helper.start_tasks()

    def finish_restoring(self):
        self.queue_helper.finish_tasks()
