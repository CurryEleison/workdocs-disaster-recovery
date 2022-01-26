import logging
import sys
from os import environ

import boto3

from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.directory_minder import RunStyle
from workdocs_dr.listings import WdFilter


def wdfilter_from_input(userquery, foldersexpr) -> WdFilter:
    folders = [f.strip() for f in foldersexpr.split(" ")] if foldersexpr is not None else []
    return WdFilter(userquery=userquery, foldernames=folders)


def organization_id_from_input(organization_id=None):
    return organization_id or environ.get("ORGANIZATION_ID")


def run_style_from_input(run_style=None) -> RunStyle:
    def normalize_str(s: str) -> str:
        return s.strip().lower() if s is not None else None
    intended_style = run_style or environ.get("RUN_STYLE", None)
    all_styles = {normalize_str(rs.name): rs for rs in list(RunStyle)}
    return all_styles.get(normalize_str(intended_style), None)


def bucket_url_from_input(bucket_name=None, prefix=None) -> str:
    if bucket_name is None and environ.get("BUCKET_URL") is not None:
        return environ.get("BUCKET_URL")
    if prefix is not None:
        return f"s3://{bucket_name}/{prefix}"
    return f"s3://{bucket_name}"


def clients_from_input(profile_name=None, region_name=None, workdocs_role_arn=None, bucket_role_arn=None) -> AwsClients:
    session = basesession_from_input(profile_name, region_name)
    wd_role = workdocs_role_arn or environ.get("WORKDOCS_ROLE_ARN")
    s3_role = bucket_role_arn or environ.get("BUCKET_ROLE_ARN")
    return AwsClients(session, wd_role, s3_role)


def basesession_from_input(profile_name=None, region_name=None):
    profile = profile_name or environ.get("AWS_PROFILE")
    if profile is not None:
        return boto3.Session(profile_name=profile)
    region = region_name or environ.get("AWS_DEFAULT_REGION")
    if region is not None:
        return boto3.Session(region_name=region)
    return boto3.session.Session()


def logging_setup(rootlogger, verbose: bool):
    isverbose = verbose or "VERBOSE" in environ
    loglevel = logging.INFO if isverbose else logging.WARN
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(loglevel)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    rootlogger.addHandler(handler)
