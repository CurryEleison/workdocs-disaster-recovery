

import logging
from pathlib import Path
from sys import prefix
from typing import List
from urllib.parse import urlparse
from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.cli_arguments import bucket_url_from_input
from workdocs_dr.document import DocumentHelper
from workdocs_dr.listings import Listings, WdFilter
from workdocs_dr.user import UserHelper, UserKeyHelper
from workdocs_dr.user_restore import UserRestoreInfo, UserRestoreRunner


class RestoreInferencer:
    def __init__(self, clients: AwsClients, bucket: str, prefix: str = None, organization_id: str = None) -> None:
        self.clients = clients
        self.bucket = bucket
        self.prefix = prefix
        self.organization_id = organization_id
        self.inferred_prefix = None
        self.inferred_orgid = None

    def _infer_prefix_orgid(self):
        raise NotImplementedError()

    def get_bucketurl(self):
        if prefix is not None:
            return bucket_url_from_input(self.bucket, self.prefix)
        self._infer_prefix_orgid()
        return bucket_url_from_input(self.bucket, self.inferred_prefix)

    def get_organization_id(self):
        if self.organization_id is not None:
            return self.organization_id
        self._infer_prefix_orgid()
        return self.inferred_orgid


class DirectoryRestoreRunner:
    def __init__(self, clients: AwsClients, organization_id: str,
                 bucket_url: str, filter: WdFilter = None, restore_path: Path = Path(".")) -> None:
        self.clients = clients
        self.organization_id = organization_id
        self.bucket_url = bucket_url
        self.filter = filter
        self.restore_path = restore_path if isinstance(restore_path, Path) else Path(restore_path)
        self.s3_fragments = urlparse(bucket_url)
        self.bucket = self.s3_fragments.hostname
        self.prefix = self.s3_fragments.path.strip("/")

    def _userlist(self):
        userdirs_prefix = UserKeyHelper.org_prefix(self.prefix, self.organization_id).strip("/")
        lister = Listings(self.clients)
        user_listing = lister.list_s3_subfoldernames(self.bucket, userdirs_prefix)

        userfilter = self.filter.is_user_matching_query if self.filter is not None else lambda u: True
        return [u for u in user_listing if userfilter(u)]

    def runall(self):
        usernames = self._userlist()
        urr = UserRestoreInfo(self.clients, self.organization_id, self.bucket_url)
        for username in usernames:
            uh, ukh = urr.userhelper_userkeyhelper_from_username(username)
            userpath = self.restore_path / uh.username if len(usernames) > 1 else self.restore_path
            ur = UserRestoreRunner(uh, ukh, self.clients, userpath)
            ur.restore_user_queued(self.filter)
            logging.info(f"Restored user {uh.username}")
