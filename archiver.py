# -*- coding: utf-8 -*-
# Copyright 2017 Vector Creations Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from paramiko.client import AutoAddPolicy, SSHClient
from datetime import date
from collections import namedtuple
import argparse
import progressbar
import gzip
import re
import os
import os.path
import yaml


FIND_COMMAND_TEMPLATE = 'find %(dir)s -name "%(glob)s"'
DATE_REGEX = re.compile("(20[0-9][0-9])-([0-9][0-9])-([0-9][0-9])")


Service = namedtuple("Service", (
    "name", "host", "account", "directory", "pattern",
    "days_to_keep_on_remote",
))


def filter_by_age(files, comparator):
    """Filter files based on the date in their name relative to today.

    Args:
        files (iterable(str)): filenames with a date in them
        comparator (func): A function that takes a date.timedelta and returns
            a bool that indicates whether to include file in the output list

    Returns:
        list(str): The filtered list of files
    """
    today = date.today()
    results = []
    for f in files:
        m = DATE_REGEX.search(f)
        if not m:
            continue
        f_date = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

        if comparator(today - f_date):
            results.append((f_date, f))

    results.sort()
    return [f for _, f in results]


class Archiver(object):
    def __init__(self, base_dir, verbose, dry_run, remove, use_ssh_agent):
        """
        Args:
            base_dir(str): Local base path to log files to
            verbose(bool): Print what its doing
            dry_run(bool): Don't actually copy files, just print
            remove(bool): Actually remove remote files
            use_ssh_agent(bool): Allow SSH client to try keys in SSH agent
        """
        self.base_dir = base_dir
        self.verbose = verbose
        self.dry_run = dry_run
        self.remove = remove
        self.use_ssh_agent = use_ssh_agent

    def archive_service(self, service):
        """Actually do the archiving step for the given Service
        """

        # Create the base directory for this service, i.e. where we put logs.
        base_dir = os.path.join(self.base_dir, service.name, service.host)
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)

        if "<DATE->" not in service.pattern:
            # We ignore services that don't have a <DATE-> in their pattern
            print "Warning:", service.name, "does not include date. Ignoring."

        # Connect to remote
        client = SSHClient()
        # TODO: Use something other than auto add policy?
        client.set_missing_host_key_policy(AutoAddPolicy())
        client.connect(
            service.host,
            username=service.account,
            compress=True,
            allow_agent=self.use_ssh_agent,
        )

        # Fetch list of files from the remote
        glob = service.pattern.replace("<DATE->", "????-??-??")
        cmd = FIND_COMMAND_TEMPLATE % {
            "dir": service.directory,
            "glob": glob,
        }
        _, stdout, _ = client.exec_command(cmd)
        files = stdout.readlines()
        files[:] = list(f.strip() for f in files)
        files.sort()

        # Filter the files to ones we want to archive
        files = filter_by_age(
            files,
            lambda d: d.days > service.days_to_keep_on_remote
        )

        # For each file download to a pending file name (optionally gzipping)
        # and only after it has succesfully been downloaded do we optionally
        # delete from the remote.
        sftp = client.open_sftp()
        for file_name in files:
            local_name = os.path.join(base_dir, os.path.basename(file_name))
            if not file_name.endswith(".gz"):
                local_name += ".gz"
            pending_name = local_name + ".download"

            if os.path.exists(pending_name):
                os.remove(pending_name)

            if os.path.exists(local_name):
                print "Warning: ", local_name, "already exists"
                continue

            # Set up progress bar for downloads
            if self.verbose:
                widgets = [
                    os.path.basename(file_name), " ",
                    progressbar.Percentage(),
                    ' ', progressbar.Bar(),
                    ' ', progressbar.ETA(),
                    ' ', progressbar.FileTransferSpeed(),
                ]
                pb = progressbar.ProgressBar(widgets=widgets)

                def progress_cb(bytes_downloaded, total_size):
                    pb.max_value = total_size
                    pb.update(bytes_downloaded)
            else:
                def progress_cb(bytes_downloaded, total_size):
                    pass

            if self.verbose or self.dry_run:
                print "Archiving: %s:%s to %s" % (
                    service.host, file_name, local_name,
                )

            if not self.dry_run:
                # If filename does not end with '.gz' then we compress while
                # we download
                # TODO: Should we be preserving last modified times?
                if not file_name.endswith(".gz"):
                    with gzip.open(pending_name, 'wb', compresslevel=9) as f:
                        sftp.getfo(file_name, f, callback=progress_cb)
                else:
                    sftp.get(file_name, pending_name, callback=progress_cb)

                if self.verbose:
                    pb.finish()

                os.rename(pending_name, local_name)

                if self.remove:
                    if self.verbose:
                        print "Removing remote"
                    sftp.remove(file_name)

        sftp.close()
        client.close()

        # We now go and delete any files that are older than the retention
        # period, if specified
        if service.retention_period_days:
            local_files = list(
                os.path.join(dirpath, filename)
                for dirpath, _, filenames in os.walk(base_dir)
                for filename in filenames
            )

            files_to_delete = filter_by_age(
                local_files,
                lambda d: d.days > service.retention_period_days
            )

            for file_name in files_to_delete:
                if self.verbose or self.dry_run:
                    print "Deleting file due to retention policy: %s" % (
                        file_name,
                    )

                if not self.dry_run:
                    os.remove(file_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument("-v", "--verbose", help="increase output verbosity",
                        action="store_true")
    parser.add_argument("-n", "--dry-run",
                        help="print files that would be archived",
                        action="store_true")
    parser.add_argument("--remove", help="remove files from remote",
                        action="store_true")
    parser.add_argument("--use-ssh-agent",
                        help="allow using keys from ssh agent",
                        action="store_true")
    args = parser.parse_args()

    config_file = args.config
    config = yaml.load(open(config_file))

    base_dir = config["archive_dir"]

    services = [
        Service(
            name=name,
            host=host,
            account=serv_config["account"],
            directory=serv_config["directory"],
            pattern=serv_config["pattern"],
            days_to_keep_on_remote=serv_config["days_to_keep_on_remote"],
            retention_period_days=serv_config.get("retention_period_days"),
        )
        for name, serv_config in config["services"].iteritems()
        for host in serv_config["hosts"]
    ]

    archiver = Archiver(
        base_dir, args.verbose, args.dry_run, args.remove, args.use_ssh_agent
    )

    for service in services:
        if args.verbose:
            print "Handling", service.name, service.host

        try:
            archiver.archive_service(service)
        except Exception as e:
            print "Error while processing", service.name, service.host, e
