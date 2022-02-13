from pathlib import Path
from urllib.parse import urlparse
from workdocs_dr.cli_arguments import (
    bucket_url_from_input,
    clients_from_input,
    organization_id_from_input,
)
from workdocs_dr.listings import Listings


class TestListings:
    std_wdb_kwargs = {
        "clients": clients_from_input(),
        "organization_id": organization_id_from_input(),
        "bucket_url": bucket_url_from_input(),
    }

    def test_delimited_listing(self):
        # s3://recovery-workdocs/workdocs/d-93670a730b/
        fragments = urlparse(self.std_wdb_kwargs["bucket_url"])
        dirtolist = Path(fragments.path) / "d-93670a730b"
        lst = Listings(self.std_wdb_kwargs["clients"])
        subfolders = lst.list_s3_subfoldernames(fragments.hostname, str(dirtolist))
        pass
