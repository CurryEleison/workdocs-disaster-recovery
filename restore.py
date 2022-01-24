from argparse import ArgumentParser, ArgumentTypeError
from os.path import isdir
from pathlib import Path
import logging

from workdocs_dr.cli_arguments import clients_from_input, bucket_url_from_input, logging_setup, organization_id_from_input, wdfilter_from_input
from workdocs_dr.directory_restore import DirectoryRestoreRunner
rootlogger = logging.getLogger()
rootlogger.setLevel(logging.INFO)


def main():
    parser = ArgumentParser()
    parser.add_argument("--profile", help="AWS profile", default=None)
    parser.add_argument("--region", help="AWS region", default=None)
    parser.add_argument("--user-query", help="Query of user", default=None)
    parser.add_argument("--folder", help="Folder(s) to restore", default=None)
    parser.add_argument("--organization-id",
                        help="Workdocs organization id (directory id)", default=None)
    parser.add_argument(
        "--prefix", help="Prefix for bucket access", default=None)
    parser.add_argument("--bucket-name", help="Name of bucket", default=None)
    parser.add_argument("--path", type=dir_path, default=Path("."))

    parser.add_argument(
        "--bucket-role-arn",
        help="ARN of role that puts/gets disaster recovery documents", default=None)
    parser.add_argument("--verbose", help="Verbose output",
                        dest="verbose", action="store_true")
    args = parser.parse_args()
    clients = clients_from_input(profile_name=args.profile, region_name=args.region,
                                 workdocs_role_arn=None, bucket_role_arn=args.bucket_role_arn)
    bucket = bucket_url_from_input(args.bucket_name, args.prefix)
    filter = wdfilter_from_input(args.user_query, args.folder)
    organization_id = organization_id_from_input(args.organization_id)
    # Restorer goes here
    drr = DirectoryRestoreRunner(
        clients,
        organization_id,
        bucket,
        filter,
        args.path
    )
    drr.runall()
    logging_setup(rootlogger=rootlogger, verbose=args.verbose)


def dir_path(path):
    if isdir(path):
        return path
    else:
        raise ArgumentTypeError(f"readable_dir:{path} is not a valid path")


if __name__ == '__main__':
    main()
