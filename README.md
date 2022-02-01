# WorkDocs Disaster Recovery

AWS WorkDocs isn't covered by by AWS Backup, so I've cobbled together a quick implementation that will
back up your documents to an S3 bucket along with a restore script. If you want to use it, run the container
at https://hub.docker.com/repository/docker/curryeleison/workdocs-disaster-recovery .

You will need:
- A WorkDocs drive/directory
- An S3 bucket (preferably in a separate account)
- Some way to schedule periodic runs (I use ECS/Fargate, but a k8s cron job would be just as nice)
- IAM policies and roles (see below)


## Operational Setup

This just notes some highlights. If you'd like to see a more detailed description log an issue.

### Bucket

I recommend the bucket to be set up in an account separate from the WorkDocs Site. 
The bucket must be versioned and I recommend you set up a lifecycle rule to delete
non-current versions of objects after a period. The backup script will perform plenty
of HEAD and LIST operations, so I think access tiers like Infrequent Access or Glacier
will probably be more expensive on balance.

### IAM

#### Bucket access

For writing to the bucket you will need a policy like this:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "readwrite-permissions",
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:ListBucketVersions",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"
            ],
            "Resource": [
                "arn:aws:s3:::<YOUR-BUCKET-NAME>",
                "arn:aws:s3:::<YOUR-BUCKET-NAME>/*"
            ]
        }
    ]
}
```


#### WorkDocs access

For reading and listing the documents to be backed up you need a policy like

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Condition": {
                "StringEquals": {
                    "Resource.OrganizationId": "<YOUR-ORGANIZATION-ID>"
                }
            },
            "Action": [
                "workdocs:GetDocument*",
                "workdocs:GetFolder*",
                "workdocs:Describe*",
                "workdocs:DownloadDocumentVersion"
            ],
            "Resource": [
                "*"
            ],
            "Effect": "Allow"
        }
    ]
}
```

## Running

### As a scheduled container

The image is over at https://hub.docker.com/repository/docker/curryeleison/workdocs-disaster-recovery . 
The container needs a profile, so it can assume roles with the permissions mentioned above. The following 
environment variables can be set:

- `AWS_DEFAULT_REGION`: Region where the task runs. Assumed to be the region where the WorkDocs Site is
- `ORGANIZATION_ID`: Organization Id (i.e. something like `d-abc0124567`)
- `BUCKET_URL`: S3 Url with bucket and prefix (i.e. `s3://my-bucket-name/workdocs-backup`)
- `WORKDOCS_ROLE_ARN`: ARN of role to assume when reading from Workdocs. Optional if profile already allows this
- `BUCKET_ROLE_ARN`: ARN of role to assume when writing to S3 bucket. Optional if profile already allows this
- `AWS_PROFILE`: Optinal profile to use
- `RUN_STYLE`: "FULL" or "ACTIVITIES" to force a full or incremental backup. Optional.
- `VERBOSE`: Optional. Any value will set loglevel to INFO instead of WARNING

The image is built for `amd64` (Intel) and `arm64` (ARM) architectures.

### From command line

#### Setup

Check out, and install dependencies with `pipenv install`. You can get pipenv with `pip install pipenv` if you
don't already have it. I have been using python 3.9, but it should work on 3.8 as well.

#### Running a backup

Use `pipenv shell` to activate the virtual environment and run with `python main.py`
You can get the command line arguments by running `python main.py --help`. They are

- `--bucket-name`: Name of bucket to back up to
- `--prefix`: Prefix of S3 object keys to back up to (e.g. `workdocs-backup`)
- `--user-query`: Username of single user to back up
- `--organization-id`: Organization Id (i.e. something like `d-abc0124567`)
- `--bucket-role-arn`: ARN of role to write to bucket (optional if profile has permissions)
- `--workdocs-role-arn`: ARN of role to read from WorkDocs (optional if profile has permissions)
- `--region`: Optional Region (region of WorkDocs Site by assumption)
- `--profile`: Optional AWS profile to use for run
- `--run-style`: Optional. Run a FULL or ACTIVITIES (incremental) backup. Default is autodetect
- `--verbose`: Optional. Detailed output

#### Running a restore

Activate the virtual environment with `pipenv shell` and run with `python restore.py`. Get
command line arguments with `python restore.py --help`. Command line arguments are:

- `--bucket-name`: Name of bucket with backup
- `--prefix`: S3 key prefix
- `--organization-id`: Organization Id
- `--user-query`: Optional 
- `--folder`: Optional folder to be restored
- `--path`: Path to restore to
- `--bucket-role-arn`: Optional IAM role to assume to read from bucket
- `--profile`: Optinal AWS Profile
- `--region`: Optional AWS Region
- `--verbose`: Optional. Chatty output


## Setting up development environment

Install development dependencies with `pipenv install --dev`. Run tests with pytest

The tests are all full integration tests. Copy `pytest.ini.template` to `pytest.ini`, fill in the environment
variables and see if you can make it work.

## Outstanding

Needs a lot of cleaning!

Tasks and ideas:

- [ ] Consider downgrading to python 3.8 for better Amazon Linux 2 compatibility
- [ ] Detect and warn if bucket and workdocs drive are in same account
- [ ] Allow WorkDocs account to be in other region than default
- [ ] Some tests that aren't just integration tests against a real env
- [x] Environment variable to control verbosity of output
- [x] Test if we are handling folder renaming correctly
- [x] Create new incremental run based on `describe-activities`
- [ ] Improve restores with "point-in-time" capability to use versions
- [ ] Make nice, simplified report of backup run
- [ ] Nice simplified report of restore runs
- [x] Think about optimal amount of parallel threads
- [x] Add chunked directory walking to parallel workloads
- [x] Figure out way to update .folderinfo when new folders are present  
      - HEAD .folderinfo for last mod date and compare against newest folder mod date  
      - We are here accepting that _deleting_ a folder won't update the .folderinfo
- [x] Start work on restore
- [x] Implement --verbose flag and do logging to stdout/stderr
- [x] Implement skipping of restore actions if file with same name/date/size already exists
- [x] Implement pruning of directories
- [ ] Re-implement filtering for backups (and restores). Use fnmatch. Maybe
- [ ] Implement filtering on paths for restore. Maybe
- [x] Refresh logging to be more on-point distinguish between debug and info
- [x] Improved detection of possibility of skipping a write on restore (Use sizes to see if there are matches)
- [x] Reorganize: Split listing_queue and remove unused functions
- [x] Remove mpire from Pipfile
- [ ] Implement context manager for QueueHelper
- [x] Adjust boto3 client config to increase max_pool_connections
- [ ] Prune instances where duplicate filenames are part of same folder (newest file wins)
- [ ] Implement prioritized queues so larger files are more likely to be synced/restored first. Maybe
- [ ] Consider sharing same queue of individual sync/restores across users
- [ ] Implement handling of conflicting directory paths
- [ ] Implement handling of unwriteable file names (use regex r"[\\/:"*?<>|]+" )
- [x] Implement writing of relevant metadata (mainly creation/modify dates)
- [ ] Maybe? implement restore to archive file?
- [ ] Detect and error if disk is filling up on restore
