import logging
from pathlib import Path
import queue
from tempfile import TemporaryDirectory
import boto3
import pytest

from time import sleep

from workdocs_dr.cli_arguments import bucket_url_from_input, clients_from_input, organization_id_from_input
from workdocs_dr.listings import WdDirectory
from workdocs_dr.queue_backup import ListWorkdocsFolders, RecordSyncTasks, RunSyncTasks
from workdocs_dr.queue_restore import GenerateRestoreTasks, RunRestoreTasks
#from workdocs_dr.listing_queue import GenerateRestoreTasks, RecordSyncTasks, RunRestoreTasks, RunSyncTasks, ListWorkdocsFolders
from workdocs_dr.user import UserHelper, UserKeyHelper
from workdocs_dr.user_backup import UserBackupRunner
from workdocs_dr.user_restore import UserRestoreInfo, UserRestoreRunner
from tests.helpers import get_complex_user, get_simple_user


class TestTreeTraversal:

    wddir_kwargs = {
        "clients": clients_from_input(),
        "organization_id": organization_id_from_input(),
    }
    std_wdb_kwargs = {**wddir_kwargs, **{"bucket_url": bucket_url_from_input()}}


    def test_traverseuser_parallel(self):
        directory = WdDirectory(**self.wddir_kwargs)
        users = [UserHelper(u) for u in directory.generate_users()]
        simple_user = get_simple_user()
        uh = next(u for u in users if u.username == simple_user)
        clients = self.wddir_kwargs["clients"]

        fdef_queue = queue.Queue()
        fdefs = []
        foldertree = ListWorkdocsFolders(clients, collect_folders=True, downstream_queue=fdef_queue)
        foldertree.start_walk(uh.root_folder_id)
        foldertree.finish_walk()
        fdef_queue.put(None)
        for fdef in iter(fdef_queue.get, None):
            logging.info(fdef)

    def test_makesynctasks_parallel(self):
        directory = WdDirectory(**self.wddir_kwargs)
        users = [UserHelper(u) for u in directory.generate_users()]
        simple_user = get_simple_user()
        uh = next(u for u in users if u.username == simple_user)
        ukh = UserKeyHelper(uh, self.std_wdb_kwargs["bucket_url"])
        clients = self.wddir_kwargs["clients"]

        folder_queue = queue.Queue()
        foldertree = ListWorkdocsFolders(clients, collect_folders=True, downstream_queue=folder_queue)
        rst = RecordSyncTasks(clients, uh, ukh, task_queue=folder_queue, downstream_queue=None)
        foldertree.start_walk(uh.root_folder_id)
        rst.start_recording()
        
        foldertree.finish_walk()
        folder_queue.put(None)
        rst.finish_recording()


    def test_runsynctasks_parallel(self):
        directory = WdDirectory(**self.wddir_kwargs)
        users = [UserHelper(u) for u in directory.generate_users()]
        simple_user = get_simple_user()
        uh = next(u for u in users if u.username == simple_user)
        ukh = UserKeyHelper(uh, self.std_wdb_kwargs["bucket_url"])
        clients = self.wddir_kwargs["clients"]

        folder_queue = queue.Queue()
        action_queue = queue.Queue()
        foldertree = ListWorkdocsFolders(clients, collect_folders=True, downstream_queue=folder_queue)
        record_st = RecordSyncTasks(clients, uh, ukh, task_queue=folder_queue, downstream_queue=action_queue)
        run_st = RunSyncTasks(task_queue=action_queue)
        foldertree.start_walk(uh.root_folder_id)
        record_st.start_recording()
        run_st.start_syncing()
        
        foldertree.finish_walk()
        folder_queue.put(None)
        record_st.finish_recording()
        action_queue.put(None)
        run_st.finish_syncing()

    @pytest.mark.integration
    def test_queued_backupuser(self):
        directory = WdDirectory(**self.wddir_kwargs)
        users = [UserHelper(u) for u in directory.generate_users()]
        simple_user = get_simple_user()
        uh = next(u for u in users if u.username == simple_user)
        awsclients = self.wddir_kwargs["clients"]
        ubr = UserBackupRunner(uh, UserKeyHelper(uh, self.std_wdb_kwargs["bucket_url"]), awsclients)
        logging.debug("Starting log")
        boto3.set_stream_logger('', logging.INFO)
        results = ubr.backup_user_queue(filter=None)
        pass

    @pytest.mark.integration
    def test_generate_restoretasks(self):
        awsclients = self.std_wdb_kwargs["clients"]
        username = get_simple_user()
        uri = UserRestoreInfo(**self.std_wdb_kwargs)
        uh, ukh = uri.userhelper_userkeyhelper_from_username(username)
        with TemporaryDirectory() as tempdir:
            folder_queue = queue.Queue()
            grt = GenerateRestoreTasks(folder_queue=folder_queue, restore_file_queue=None, 
                        clients=awsclients, userkeyhelper=ukh)
            ur = UserRestoreRunner(uh, ukh, awsclients, tempdir)
            for restore_folder_def in ur.generate_restoredefs():
                folder_queue.put(restore_folder_def)
            grt.start_generating()
            folder_queue.put(None)
            grt.finish_generating()

    @pytest.mark.integration
    def test_queued_restore_manual(self):
        awsclients = self.std_wdb_kwargs["clients"]
        username = get_simple_user()
        uri = UserRestoreInfo(**self.std_wdb_kwargs)
        uh, ukh = uri.userhelper_userkeyhelper_from_username(username)
        with TemporaryDirectory() as tempdir:
            folder_queue = queue.Queue()
            file_queue = queue.Queue()
            grt = GenerateRestoreTasks(folder_queue=folder_queue, restore_file_queue=file_queue, 
                        clients=awsclients, userkeyhelper=ukh)
            rrt = RunRestoreTasks(restore_queue=file_queue, clients=awsclients, userkeyhelper=ukh)
            ur = UserRestoreRunner(uh, ukh, awsclients, tempdir)
            for restore_folder_def in ur.generate_restoredefs():
                folder_queue.put(restore_folder_def)
            grt.start_generating()
            rrt.start_restoring()
            folder_queue.put(None)
            grt.finish_generating()
            file_queue.put(None)
            rrt.finish_restoring()

    @pytest.mark.integration
    def test_restore_queued(self):
        awsclients = self.std_wdb_kwargs["clients"]
        username = get_simple_user()
        uri = UserRestoreInfo(**self.std_wdb_kwargs)
        uh, ukh = uri.userhelper_userkeyhelper_from_username(username)
        with TemporaryDirectory() as tempdir:
            ur = UserRestoreRunner(uh, ukh, awsclients, tempdir)
            ur.restore_user_queued(filter=None)

    @pytest.mark.integration
    def test_restore_queued_towindows(self):
        awsclients = self.std_wdb_kwargs["clients"]
        username = get_simple_user()
        uri = UserRestoreInfo(**self.std_wdb_kwargs)
        uh, ukh = uri.userhelper_userkeyhelper_from_username(username)
        windowsdir = Path("/mnt/c/123/tmp")
        ur = UserRestoreRunner(uh, ukh, awsclients, windowsdir)
        ur.restore_user_queued(filter=None)
