import boto3
import pytest
import logging
from tempfile import TemporaryDirectory

from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.cli_arguments import bucket_url_from_input, clients_from_input, organization_id_from_input
from workdocs_dr.directory_restore import DirectoryRestoreRunner
from workdocs_dr.listings import WdFilter
from workdocs_dr.user import UserHelper, UserKeyHelper
# from workdocs_dr.user_restore import UserRestoreInfo, UserRestoreRunner
from tests.helpers import get_complex_user, get_simple_user


class TestRestoring:

    std_wdb_kwargs = {
        "clients": clients_from_input(),
        "organization_id": organization_id_from_input(),
        "bucket_url": bucket_url_from_input(),
    }

    @pytest.mark.integration
    def test_restore_userlist(self):
        drr = DirectoryRestoreRunner(**self.std_wdb_kwargs)
        users = drr._userlist()
        assert isinstance(users, list)

    def test_restore_userlist_filtered(self):
        filter = WdFilter(userquery=get_simple_user())
        drr = DirectoryRestoreRunner(filter=filter, **self.std_wdb_kwargs)
        users = drr._userlist()
        assert isinstance(users, list)
        assert len(users) <= 1

    # def test_userhelper_from_s3(self):
    #     username = get_complex_user()
    #     uri = UserRestoreInfo(**self.std_wdb_kwargs)
    #     uh, ukh = uri.userhelper_userkeyhelper_from_username(username)
    #     assert isinstance(uh, UserHelper)
    #     assert isinstance(ukh, UserKeyHelper)

    # def test_restore_user(self):
    #     awsclients = self.std_wdb_kwargs["clients"]
    #     username = get_simple_user()
    #     uri = UserRestoreInfo(**self.std_wdb_kwargs)
    #     uh, ukh = uri.userhelper_userkeyhelper_from_username(username)
    #     with TemporaryDirectory() as tempdir:
    #         ur = UserRestoreRunner(uh, ukh, awsclients, tempdir)
    #         ur.restore_user(filter=None)

    def test_restore_dir_single_user(self):
        filter = WdFilter(userquery=get_simple_user())
        with TemporaryDirectory() as tempdir:
            urr = DirectoryRestoreRunner(filter=filter, restore_path=tempdir, **self.std_wdb_kwargs)
            urr.runall()