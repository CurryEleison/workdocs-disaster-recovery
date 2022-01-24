from argparse import ArgumentParser
import logging

from workdocs_dr.cli_arguments import clients_from_input, bucket_url_from_input, logging_setup, organization_id_from_input, run_style_from_input, wdfilter_from_input
from workdocs_dr.directory_backup import DirectoryBackupRunner

rootlogger = logging.getLogger()
rootlogger.setLevel(logging.DEBUG)


def main():
    parser = ArgumentParser()
    # parser.add_argument("--action", default="backup", help="Action to take", choices=["backup"])
    parser.add_argument("--profile", help="AWS profile", default=None)
    parser.add_argument("--region", help="AWS region", default=None)
    parser.add_argument("--organization-id",
                        help="Workdocs organization id (directory id)", default=None)
    parser.add_argument("--bucket-name", help="Name of bucket", default=None)
    parser.add_argument(
        "--prefix", help="Prefix for bucket access", default=None)
    parser.add_argument("--user-query", help="Query of user", default=None)
    parser.add_argument(
        "--folder", help="Folder(s) to backup up", default=None)
    parser.add_argument("--workdocs-role-arn",
                        help="ARN of role that can access workdocs", default=None)
    parser.add_argument("--run-style",
                        help="Run a FULL or ACTIVITIES (incremental) backup. Default is autodetect",
                        default=None)
    parser.add_argument(
        "--bucket-role-arn",
        help="ARN of role that puts/gets disaster recovery documents", default=None)
    parser.add_argument("--verbose", help="Verbose output",
                        dest="verbose", action="store_true")
    args = parser.parse_args()
    clients = clients_from_input(profile_name=args.profile, region_name=args.region,
                                 workdocs_role_arn=args.workdocs_role_arn, bucket_role_arn=args.bucket_role_arn)
    organization_id = organization_id_from_input(args.organization_id)
    run_style = run_style_from_input(args.run_style)
    db = DirectoryBackupRunner(
        clients=clients,
        organization_id=organization_id,
        bucket_url=bucket_url_from_input(args.bucket_name, args.prefix),
        filter=wdfilter_from_input(args.user_query, args.folder),
        run_style=run_style,
    )
    logging_setup(rootlogger=rootlogger, verbose=args.verbose)
    logging.info(f"orgid {db.organization_id} url {db.bucket_url}")
    db.runall()
    return


if __name__ == '__main__':
    main()
