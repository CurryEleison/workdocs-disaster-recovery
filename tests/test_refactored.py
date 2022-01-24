import logging
import pytest
import boto3

from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.cli_arguments import bucket_url_from_input, clients_from_input, organization_id_from_input
from workdocs_dr.directory_backup import DirectoryBackupRunner
from workdocs_dr.directory_minder import RunStyle
from workdocs_dr.listings import Listings, WdDirectory, WdFilter
from workdocs_dr.user import UserHelper, UserKeyHelper
# from workdocs_dr.user_backup import UserBackupRunner
from workdocs_dr.workdocs_bucket_sync import WorkDocs2BucketSync
from tests.helpers import get_complex_user, get_simple_user, get_known_workdocs_path



class TestRefactored:



    wddir_kwargs = {
        "clients": clients_from_input(),
        "organization_id": organization_id_from_input(),
    }
    std_wdb_kwargs = {**wddir_kwargs, **{"bucket_url": bucket_url_from_input()}}


    @staticmethod
    def folderidfrompath(clients:AwsClients, rootfolderid, path):
        subfolderlist = path.strip('/').split('/')
        currentfolderid = rootfolderid
        client = clients.docs_client()
        for folder in subfolderlist:
            response = client.describe_folder_contents(
                FolderId=currentfolderid)
            matchingfolder = next(
                (d for d in response["Folders"] if d["Name"] == folder), None)
            if matchingfolder is None:
                raise RuntimeError("Bad path")
            currentfolderid = matchingfolder["Id"]
        return currentfolderid
    
    @pytest.mark.integration
    @pytest.mark.current
    def test_refactored_listusers(self):
        directory = WdDirectory(**self.wddir_kwargs)
        users = [u for u in directory.generate_users()]
        assert len(users) > 0
        complex_user = get_complex_user()
        simple_user = get_simple_user()
        assert len([u for u in users if u["Username"] == complex_user]) == 1
        assert len([u for u in users if u["Username"] == simple_user]) == 1
    
    # @pytest.mark.integration
    # def test_refactored_folderlist(self):
    #     directory = WdDirectory(**self.wddir_kwargs)
    #     users = [u for u in directory.generate_users()]
    #     complexuser = next(u for u in users if "Username" in u and u["Username"] == get_complex_user())
    #     userhelper = UserHelper(complexuser)
    #     wdft = WorkdocsFolderTree(self.wddir_kwargs["clients"])
    #     folders = []
    #     for f in wdft.generate_subfolders(userhelper.root_folder_id):
    #         folders.append(f)
    #         if len(folders) > 3 * 3 * 3:
    #             break
    #     assert len(folders) > 0



    @pytest.mark.integration
    def test_refactored_listextracts(self):
        directory = WdDirectory(**self.wddir_kwargs)
        users = [UserHelper(u) for u in directory.generate_users()]
        uh = next(u for u in users if u.username == get_complex_user())
        awsclients = self.wddir_kwargs["clients"]
        extracts_folder_id = TestRefactored.folderidfrompath(awsclients, uh.root_folder_id, get_known_workdocs_path())
        lists = Listings(awsclients)
        documents = lists.list_wd_documents(extracts_folder_id)
        assert len(documents) > 0
        assert next((d for d in documents if d["LatestVersionMetadata"]["Name"] == "vat.tsv"), None) is not None

    # @pytest.mark.integration
    # def test_refactored_syncextracts(self):
    #     directory = WdDirectory(**self.wddir_kwargs)
    #     users = [UserHelper(u) for u in directory.generate_users()]
    #     complex_user = get_complex_user()
    #     uh = next(u for u in users if u.username == complex_user)
    #     awsclients = self.wddir_kwargs["clients"]
    #     extracts_folder_id = TestRefactored.folderidfrompath(awsclients, uh.root_folder_id, get_known_workdocs_path())
    #     ubr = UserBackupRunner(uh, UserKeyHelper(uh, self.std_wdb_kwargs["bucket_url"]), awsclients)
    #     summary = ubr.backupfolder_tree(extracts_folder_id)
    #     assert len(summary) > 0

    @pytest.mark.integration
    def test_refactored_updateuserinfo(self):
        directory = WdDirectory(**self.wddir_kwargs)
        users = [UserHelper(u) for u in directory.generate_users()]
        uh = next(u for u in users if u.username == get_complex_user())
        awsclients = self.wddir_kwargs["clients"]
        wd2bs = WorkDocs2BucketSync(awsclients, uh, UserKeyHelper(uh, self.std_wdb_kwargs["bucket_url"]))
        wd2bs.update_user_info()

    # @pytest.mark.integration
    # def test_refactored_backupuser(self):
    #     directory = WdDirectory(**self.wddir_kwargs)
    #     users = [UserHelper(u) for u in directory.generate_users()]
    #     simple_user = get_simple_user()
    #     uh = next(u for u in users if u.username == simple_user)
    #     awsclients = self.wddir_kwargs["clients"]
    #     ubr = UserBackupRunner(uh, UserKeyHelper(uh, self.std_wdb_kwargs["bucket_url"]), awsclients)
    #     logging.debug("Starting log")
    #     boto3.set_stream_logger('', logging.INFO)
    #     results = ubr.backup_user()

    @pytest.mark.integration
    def test_refactored_filtered(self):
        filter = WdFilter(userquery=get_complex_user(), foldernames=[get_known_workdocs_path()])
        dbr = DirectoryBackupRunner(filter=filter, **self.std_wdb_kwargs)
        logging.debug("Starting log")
        boto3.set_stream_logger('', logging.INFO)
        results = dbr.runall()

    @pytest.mark.integration
    def test_refactored_simple(self):
        filter = WdFilter(userquery=get_simple_user())
        dbr = DirectoryBackupRunner(filter=filter, run_style=RunStyle.FULL, **self.std_wdb_kwargs)
        logging.debug("Starting log")
        boto3.set_stream_logger('', logging.INFO)
        results = dbr.runall()

    @pytest.mark.integration
    def _test_refactored_unlimited(self):
        dbr = DirectoryBackupRunner(**self.std_wdb_kwargs)
        logging.debug("Starting log")
        boto3.set_stream_logger('', logging.INFO)
        results = dbr.runall()
        pass

    @pytest.mark.integration
    def test_refactored_simple_s3_folders(self):
        filter = WdFilter(userquery=get_simple_user())
        boto3.set_stream_logger('', logging.INFO)
        directory = WdDirectory(**self.wddir_kwargs)
        users = [UserHelper(u) for u in directory.generate_users(filter)]
        uh = next(u for u in users)
        ukh = UserKeyHelper(uh, self.std_wdb_kwargs["bucket_url"])
        awsclients = self.wddir_kwargs["clients"]
        lister = Listings(awsclients)

        s3folders = lister.list_s3_folders(ukh.bucket, ukh.bucket_folderprefix(""))
        s3_folderids = [kv["Prefix"].strip("/").split("/")[-1] for kv in s3folders]
        pass
        


