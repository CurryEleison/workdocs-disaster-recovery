from datetime import datetime, timedelta, timezone
from enum import Enum, auto
import logging
from urllib.parse import urlparse
from yaml import dump
from workdocs_dr.activity_backup import ActivityBackupRunner
from workdocs_dr.aws_clients import AwsClients
from workdocs_dr.directory_minder import DirectoryBackupMinder, RunEvent, RunStyle
from workdocs_dr.document import DocumentHelper
from workdocs_dr.listings import WdDirectory, WdFilter
from workdocs_dr.user import UserHelper, UserKeyHelper
from workdocs_dr.user_backup import UserBackupRunner


class DirectoryBackupRunner:
    def __init__(
        self,
        clients: AwsClients,
        organization_id: str,
        bucket_url: str,
        filter: WdFilter = None,
        run_style: RunStyle = None,
    ) -> None:
        self.clients = clients
        self.organization_id = organization_id
        self.bucket_url = bucket_url
        self.forced_runstyle = run_style
        self.filter = filter
        self.minder = None

    def get_minder(self):
        if self.minder is None:
            self.minder = DirectoryBackupMinder(
                self.clients, self.organization_id, self.bucket_url
            )
        return self.minder

    def runall(self):
        directory = WdDirectory(self.organization_id, self.clients)

        run_style = self.forced_runstyle or self.get_minder().get_best_run_style()
        logging.info(f"Starting Backup. Runstyle is {run_style}")
        if run_style is RunStyle.ABORT:
            return
        if run_style is RunStyle.ACTIVITIES:
            self._update_event_time(RunStyle.ACTIVITIES, RunEvent.START)
            # TODO: Implement user filters -- not at all trivial due to sharing, but we'll do it some day
            abr = ActivityBackupRunner(
                self.clients,
                self.organization_id,
                self.bucket_url,
                directory,
                self.minder,
            )
            results = abr.backup_activity_queue()
            self._update_event_time(RunStyle.ACTIVITIES, RunEvent.END)
            return results
        # Seems we are looking at a full backup
        users = [UserHelper(u) for u in directory.generate_users(self.filter)]
        self._update_event_time(RunStyle.FULL, RunEvent.START)
        results = []
        for u in users:
            ukh = UserKeyHelper(u, self.bucket_url)
            ubr = UserBackupRunner(u, ukh, self.clients)
            results.extend(ubr.backup_user_queue(self.filter))
        self._update_event_time(RunStyle.FULL, RunEvent.END)
        return results

    def _update_event_time(self, run_style: RunStyle, run_event: RunEvent) -> None:
        if self.filter is None or (
            (self.filter.foldernames is None or len(self.filter.foldernames) == 0)
            and self.filter.userquery is None
            and self.filter.folderpattern is None
        ):
            self.get_minder().update_last_event_time(
                run_style=run_style, run_event=run_event
            )
