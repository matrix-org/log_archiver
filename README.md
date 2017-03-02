# log_archiver

A python script to move old log files from various services on various machines
to a locale archive directory using SSH.

## Usage

```
$ python archiver.py -h
usage: python archiver.py [-h] [-v] [-n] [--remove] [--use-ssh-agent] config

positional arguments:
  config

optional arguments:
  -h, --help       show this help message and exit
  -v, --verbose    increase output verbosity
  -n, --dry-run    print files that would be archived
  --remove         remove files from remote
  --use-ssh-agent  allow using keys from ssh agent

```

The script will look for SSH keys in `~/.ssh`.

Note that the script doesn't remove files on the remote by default for safety
reasons. You'll probably want to add something like the following to cron:

```
@daily python log_archiver/archiver.py service.yaml --remove
```

## Example config

```yaml
archive_dir: /mnt/logs/archived

services:
  synapse:
    account: matrix
    hosts:
      - hera.matrix.org
    directory: /home/matrix/synapse/var/
    pattern: "*.log.<DATE->*"
    days_to_keep_on_remote: 2
```
