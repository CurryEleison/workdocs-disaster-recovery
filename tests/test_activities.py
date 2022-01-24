from argparse import ArgumentError
from collections import defaultdict
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from re import I

import boto3
import pytest

from workdocs_dr.cli_arguments import bucket_url_from_input, clients_from_input, organization_id_from_input
from workdocs_dr.directory_minder import DirectoryBackupMinder, RunEvent, RunStyle
from workdocs_dr.listings import WdDirectory, WdItemApexOwner
from workdocs_dr.user import UserHelper, UserKeyHelper
from workdocs_dr.workdocs_bucket_sync import WorkDocs2BucketSync

class TestActivities:

    wddir_kwargs = {
        "clients": clients_from_input(),
        "organization_id": organization_id_from_input(),
    }
    std_wdb_kwargs = {**wddir_kwargs, **{"bucket_url": bucket_url_from_input()}}

    @pytest.mark.integration
    def test_get_activities_raw(self):
        directory = WdDirectory(**self.wddir_kwargs)
        # start_time is 7 hours ago
        start_time = datetime.now(tz=timezone.utc) + timedelta(seconds=-3600*7)
        # types = "DOCUMENT_MOVED"
        act_gen = directory.generate_activities(start_time=start_time)
        activities = [a for a in act_gen]
        assert isinstance(activities, list)
    
    @pytest.mark.integration
    def test_run_styles(self):
        dbm = DirectoryBackupMinder(**self.std_wdb_kwargs)
        run_style = dbm.get_best_run_style()
        assert run_style in list(RunStyle)

    # @pytest.mark.integration
    # def test_update_runtimes_full(self):
    #     dbm = DirectoryBackupMinder(**self.std_wdb_kwargs)
    #     full_start = datetime(2022, 1, 12, 9, 30, 0, tzinfo=timezone.utc)
    #     full_end = datetime(2022, 1, 12, 12, 30, 0, tzinfo=timezone.utc)
    #     dbm.update_last_event_time(RunStyle.FULL, RunEvent.START, extra_info=None, event_time=full_start)
    #     dbm.update_last_event_time(RunStyle.FULL, RunEvent.END, extra_info=None, event_time=full_end)
    #     metadata = dbm.get_last_metadata(dbm.get_key(RunStyle.FULL, RunEvent.START))
    #     assert "StartTime" in metadata

    @pytest.mark.integration
    def test_update_runtimes_activities_2022_01_11(self):
        dbm = DirectoryBackupMinder(**self.std_wdb_kwargs)
        activity_start = datetime(2022, 1, 11, 9, 30, 0, tzinfo=timezone.utc)
        activity_end = datetime(2022, 1, 11, 12, 30, 0, tzinfo=timezone.utc)
        dbm.update_last_event_time(RunStyle.ACTIVITIES, RunEvent.START, extra_info=None, event_time=activity_start)
        dbm.update_last_event_time(RunStyle.ACTIVITIES, RunEvent.END, extra_info=None, event_time=activity_end)
        metadata = dbm.get_last_metadata(dbm.get_key(RunStyle.FULL, RunEvent.START))
        assert "StartTime" in metadata

    @pytest.mark.integration
    def test_update_runtimes_activities_2022_01_20(self):
        dbm = DirectoryBackupMinder(**self.std_wdb_kwargs)
        activity_start = datetime(2022, 1, 20, 7, 30, 0, tzinfo=timezone.utc)
        activity_end = datetime(2022, 1, 20, 7, 30, 0, tzinfo=timezone.utc)
        dbm.update_last_event_time(RunStyle.ACTIVITIES, RunEvent.START, extra_info=None, event_time=activity_start)
        dbm.update_last_event_time(RunStyle.ACTIVITIES, RunEvent.END, extra_info=None, event_time=activity_end)
        metadata = dbm.get_last_metadata(dbm.get_key(RunStyle.FULL, RunEvent.START))
        assert "StartTime" in metadata



    @pytest.mark.integration
    @pytest.mark.current
    def test_sync_activities_scratch(self):
        """Scratchpad to prototype up the activities sync"""
        directory = WdDirectory(**self.wddir_kwargs)
        # First get a list of users -- we are going to need them along with their sync objects
        users = list(directory.generate_users())
        user_helpers = {u["Id"]: UserHelper(u) for u in users}
        user_keyhelpers = {uid: UserKeyHelper(uh, self.std_wdb_kwargs["bucket_url"]) for uid, uh in user_helpers.items()}
        awsclients = self.wddir_kwargs["clients"]
        user_syncers = {u["Id"]: WorkDocs2BucketSync(awsclients, user_helpers[u["Id"]], user_keyhelpers[u["Id"]]) for u in users}
        apex_syncer = WdItemApexOwner(awsclients, users, user_syncers)
        # Next figure out what the start-time is
        #TODO: Good heuristics for determining last runtime
        start_time = datetime.now(tz=timezone.utc) + timedelta(seconds=-3600*48)
        # Now to generate a list of actions
        activities = list(directory.generate_activities(start_time))
        # Consolidating events without a parent
        doc_finalevents = dict() # Keyed by documentid, value is final event
        doc_moves = defaultdict(list) # Keyed by documentid, value is list of move events
        folder_finalevents = dict() # Keyed by folderid, value is final event
        folder_moves = defaultdict(list) # Keyed by folderid, value is list of move events
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
        folder_updates = [{"folder_id": k, "user": a["ResourceMetadata"]["Owner"]["Id"]} for k, a in folder_finalevents.items()]

        # Patch old folders into document activities
        for doc_id, moves in doc_moves.items():
            old_folders = [a["OriginalParent"]["Id"] for a in moves]
            doc_finalevents[doc_id]["OldFolderIds"] = old_folders

        doc_updates = [{"document_id": k, "user": a["ResourceMetadata"]["Owner"]["Id"], "old_folder_ids": a.get("OldFolderIds", [])} for k, a in doc_finalevents.items()]

        # TODO: Handle situations where we can't read the document (possibly because it's deleted) better.
        actions = []
        def choose_syncer(syncer_mapper: WdItemApexOwner, owner_guess:str, syncer_args, folder_id = None, document_id = None):
            if document_id is None and folder_id is None:
                raise RuntimeError("folder_id and document_id can't both be absent")
            syncer = None
            try:
                if document_id is not None:
                    syncer = syncer_mapper.get_by_document_id(document_id)
                else:
                    syncer = syncer_mapper.get_by_folder_id(folder_id)
            except (awsclients.docs_client().exceptions.EntityNotExistsException, \
                awsclients.docs_client().exceptions.UnauthorizedResourceAccessException):
                if owner_guess in syncer_mapper.result_map:
                    # syncer = syncer_mapper.result_map[owner_guess]
                    print("Consider returning a delete lambda?")
            if syncer is None:
                return lambda args=syncer_args: {}
            if document_id is not None:
                return lambda args=syncer_args: syncer.sync_document_to_bucket(**args)
            return lambda args=syncer_args: syncer.update_folder_summary(**args)


        for upd in folder_updates:
            actions.append(choose_syncer(apex_syncer, upd["user"], \
                syncer_args={"folder_id": upd["folder_id"]}, folder_id=upd["folder_id"]))
        for upd in doc_updates:
            actions.append(choose_syncer(apex_syncer, upd["user"], \
                syncer_args={"document_id": upd["document_id"], "old_folder_ids": upd["old_folder_ids"]}, document_id=upd["document_id"]))
        for act in actions:
            act()





        pass