
## Setting up

Install development dependencies with `pipenv install --dev`. Run tests with pytest

The tests are all full integration tests. Copy `pytest.ini.template` to `pytest.ini`, fill in the environment
variables and see if you can make it work.

## Outstanding

Needs a lot of cleaning!

Tasks and ideas:

[ ] Consider downgrading to python 3.8 for better Amazon Linux 2 compatibility
[ ] Detect and warn if bucket and workdocs drive are in same account
[ ] Allow WorkDocs account to be in other region than default
[ ] Some tests that aren't just integration tests against a real env
[x] Test if we are handling folder renaming correctly
[x] Create new incremental run based on `describe-activities`
[ ] Make nice, simplified report of backup run
[ ] Nice simplified report of restore runs
[x] Think about optimal amount of parallel threads
[x] Add chunked directory walking to parallel workloads
[x] Figure out way to update .folderinfo when new folders are present
    - head .folderinfo for last mod date and compare against newest folder mod date
    - We are here accepting that _deleting_ a folder won't update the .folderinfo
[x] Start work on restore
[x] Implement --verbose flag and do logging to stdout/stderr
[x] Implement skipping of restore actions if file with same name/date/size already exists
[x] Implement pruning of directories
[ ] Re-implement filtering for backups (and restores). Use fnmatch. Maybe
[ ] Implement filtering on paths for restore. Maybe
[x] Refresh logging to be more on-point distinguish between debug and info
[x] Improved detection of possibility of skipping a write on restore (Use sizes to see if there are matches)
[x] Reorganize: Split listing_queue and remove unused functions
[x] Remove mpire from Pipfile
[ ] Implement context manager for QueueHelper
[x] Adjust boto3 client config to increase max_pool_connections
[ ] Prune instances where duplicate filenames are part of same folder (newest file wins)
[ ] Implement prioritized queues so larger files are more likely to be synced/restored first. Maybe
[ ] Consider sharing same queue of individual sync/restores across users
[ ] Implement handling of conflicting directory paths
[ ] Implement handling of unwriteable file names (use regex r"[\\/:"*?<>|]+" )
[x] Implement writing of relevant metadata (mainly creation/modify dates)
[ ] Maybe? implement restore to archive file?
[ ] Detect and error if disk is filling up on restore 
