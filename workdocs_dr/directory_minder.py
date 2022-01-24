
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
import logging
from urllib.parse import urlparse

from yaml import dump
from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.document import DocumentHelper
from workdocs_dr.user import UserKeyHelper


class RunStyle(Enum):
    ABORT = auto()
    FULL = auto()
    ACTIVITIES = auto()

    def __str__(self) -> str:
        return self.name.lower()


class RunEvent(Enum):
    START = auto()
    END = auto()

    def __str__(self) -> str:
        return self.name.lower()


class DirectoryBackupMinder():
    start_time_key = "StartTime"
    end_time_key = "EndTime"

    def __init__(self, clients: AwsClients, organization_id: str, bucket_url: str) -> None:
        self.clients = clients
        self.s3_fragments = urlparse(bucket_url)
        self.bucket = self.s3_fragments.hostname
        self.prefix = self.s3_fragments.path.strip("/")
        self.organization_id = organization_id
        self.org_prefix = UserKeyHelper.org_prefix(self.prefix, self.organization_id)
        self.last_times = None
        self.current_run = None

    def init_last_times(self):
        styles = [RunStyle.FULL, RunStyle.ACTIVITIES]
        events = list(RunEvent)
        if self.last_times is None:
            self.last_times = dict()
            for s in styles:
                for e in events:
                    key = self.get_key(s, e)
                    self.last_times[(s, e)] = self.get_last_metadata(key)

    def get_key(self, run_style: RunStyle, run_event: RunEvent):
        return f"{self.org_prefix}/.last_backup_{run_event}_{run_style}"

    def get_best_run_style(self, max_days_since_last_full=30, max_hours_to_complete_last_full_run=12) -> RunStyle:
        cut_last_start_abort = self.get_now() + timedelta(hours=-1 * max_hours_to_complete_last_full_run)
        cut_last_full = self.get_now() + timedelta(days=-1 * max_days_since_last_full)
        self.init_last_times()
        if self.last_times[(RunStyle.FULL, RunEvent.END)].get(self.start_time_key) < cut_last_full:
            # Appears it's been a long time since the last complete full sync
            if self.last_times[(RunStyle.FULL, RunEvent.START)].get(self.start_time_key) > cut_last_start_abort:
                # There's a good chance we just started a full run, so let's not start another
                return RunStyle.ABORT
            return RunStyle.FULL
        return RunStyle.ACTIVITIES

    def get_activities_cutoff(self) -> datetime:
        self.init_last_times()
        # Should return time of start of most recent completed run
        last_start_time = max(
            self.last_times[(RunStyle.ACTIVITIES, RunEvent.END)].get(self.start_time_key),
            self.last_times[(RunStyle.FULL, RunEvent.END)].get(self.start_time_key)
        )
        # Add a 30 mins of padding, because Workdocs can take ~5 mins to register updates

        effective_start_time = last_start_time - \
            timedelta(minutes=30) if last_start_time > self.get_min_time() else last_start_time
        logging.debug(f"Effective start time for activities is {effective_start_time.isoformat()}")
        return effective_start_time

    def get_now(self):
        return datetime.now(tz=timezone.utc)

    def get_min_time(self):
        return datetime.min.replace(tzinfo=timezone.utc)

    def update_last_event_time(self, run_style: RunStyle, run_event: RunEvent, extra_info: str = None, event_time: datetime = None):
        current_time = event_time or self.get_now()
        if self.current_run is None:
            self.current_run = {"RunStyle": run_style, self.start_time_key: current_time}
        if run_event is RunEvent.END:
            self.current_run[self.end_time_key] = current_time
        key = self.get_key(run_style, run_event)
        body = (extra_info or dump(self.current_run)).encode("utf-8")
        s3_request = {
            "Bucket": self.bucket,
            "Key": key,
            "Body": body,
            "Metadata": DocumentHelper.metadata_dict2s3(self.current_run),
        }
        self.clients.bucket_client().put_object(**s3_request)

    def get_last_metadata(self, key):
        s3_request = {"Bucket": self.bucket, "Key": key}
        try:
            response = self.clients.bucket_client().head_object(**s3_request)
            return DocumentHelper.metadata_s32dict(response["Metadata"])
        except:
            return {self.start_time_key: self.get_min_time(), self.end_time_key: self.get_min_time()}
