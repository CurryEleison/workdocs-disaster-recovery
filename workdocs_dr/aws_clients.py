import boto3
import botocore

from workdocs_dr.boto_session import RefreshableBotoSession


class AwsClients:
    def __init__(
        self,
        basesession,
        workdocs_role_arn=None,
        bucket_role_arn=None,
    ) -> None:
        self.basesession = (basesession
                            if basesession is not None else boto3.Session())
        # self.s3_url = bucket_url
        # self.workdocs_client = None
        self.role_arns = {
            "bucket": bucket_role_arn,
            "workdocs": workdocs_role_arn
        }
        self.sessions = {}
        self.init_sessions()
        # self.sessions = getsessions(self.basesession, self.role_arns)
        self.clients = {
            "bucket": self.sessions["bucket"].client("s3", config=botocore.client.Config(max_pool_connections=50)),
            "workdocs": self.sessions["workdocs"].client("workdocs"),
        }

    def init_sessions(self):
        for k, v in self.role_arns.items():
            if v is None:
                self.sessions[k] = self.basesession
            self.sessions[k] = RefreshableBotoSession(base_session=self.basesession, sts_arn=v).refreshable_session()

    def bucket_client(self):
        return self.clients["bucket"]

    def docs_client(self):
        return self.clients["workdocs"]
