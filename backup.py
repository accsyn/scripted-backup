#!/usr/local/bin/python3
# accsyn example Python 2/3 script for automising backups.
# See README.md for more information.

import sys
import os
import argparse
import datetime
import re
import logging
import json

import accsyn_api

logger = logging.getLogger(__name__)


def info(s):
    logger.info("(accsyn backup) {}".format(s or ""))


def warning(s):
    logger.warning("(accsyn backup) {}".format(s or ""))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description='Calculates and submits a backup transfer job to accsyn, based on the contents of the supplied folder, assuming each folder is a project or large other large resource.'
    )

    parser.add_argument(
        '--source',
        help='The source site, default is no site which means main hq site. Can also be an user (user=<email>) or and explicit client (client=<id>), given they have root share mapped and readable.',
    )

    parser.add_argument(
        '--share',
        help='The (root)share that path is corresponding to, if not provided the default root share is assumed.',
    )

    parser.add_argument(
        '--destination',
        help='The destination backup site, default is "backup" site. Can also be an user (user=<email>) or and explicit client (client=<id>), given they have root share mapped and writeable.',
    )

    parser.add_argument(
        '--mode',
        help='The file copy mode to apply; copy(default): default rsync protocol, copy missing or modified files. onewaysync: Also delete files at destination that are not present at source.',
        choices=['copy', 'onewaysync'],
    )

    parser.add_argument(
        '--subfolder', help='The root share subfolder to scan, for example "Projects" or "Media/_SOURCES_"'
    )

    DEFAULT_EXCLUDE_FOLDERS = ['^[.]']
    parser.add_argument(
        '--exclude_folders',
        help='Comma(",") separated list of backup files/folders/projects to exclude, regexp can be used. For example "tmp,mnt". Default is to exclude all files and folders starting with "."({})'.format(
            DEFAULT_EXCLUDE_FOLDERS[0]
        ),
    )

    DEFAULT_EXCLUDE = "*.tmp,.*,re('Icon\\r'),Thumbs.db"
    parser.add_argument(
        '--exclude',
        help='Comma(",") separated list of files to exclude during transfer, regexp can be used. Default: {}'.format(
            DEFAULT_EXCLUDE
        ),
    )

    parser.add_argument(
        '--name',
        help='The transfer job title to apply and look for amongst existing jobs, default is "Backup YY.MM" which means that a new job will be created every month. Having the title "Backup" will mean on single large job will be reused each backup cycle. If you have multiple backup jobs between different sites, be careful to name then accordingly and have on backup job for each source-destination pair.',
    )

    parser.add_argument('--dry_run', help='Only print accsyn calls to be made/data to be sent.', action='store_true')

    parser.add_argument(
        'path',
        help='The path to backup, each file or folder beneath will be synchronized one at a time to the destination site.',
    )

    info("Initialising")

    args = parser.parse_args()

    if args.path is None or not os.path.exists(args.path):
        raise Exception('Path "{}" does not exist!'.format(args.path))
    path = args.path

    subfolder = args.subfolder

    # Disk scan or read from JSON?
    path_scan = None
    backup_data = None
    if os.path.isdir(args.path):
        path_scan = path
        if len(subfolder or '') > 0:
            path_scan = '{}{}{}'.format(path_scan, os.sep, subfolder)
            if not os.path.exists(path_scan) or not os.path.isdir(path_scan):
                raise Exception('Subfolder "{}" does not exist or is not a directory!'.format(path_scan))
    elif os.path.isfile(args.path):
        backup_data = json.load(open(args.path, 'r'))
        info('Read backup data {} from: {}'.format(backup_data, args.path))
    else:
        # TODO: This could be a REST API endpoint or other way to fetch project data to backup
        raise Exception('Could not interpret path: {}!'.format(args.path))

    exclude_folders = args.exclude_folders
    if len(exclude_folders or '') > 0:
        exclude_folders = exclude_folders.split(',')
    else:
        exclude_folders = DEFAULT_EXCLUDE_FOLDERS

    source = args.source or ''
    if len(source) > 0:
        if source.find('=') == -1:
            source = 'site={}'.format(source)

    destination = args.destination or ''
    if len(destination) == 0:
        destination = 'site=backup'
    else:
        if destination.find('=') == -1:
            destination = 'site={}'.format(destination)

    name = args.name or ''
    if len(name) == 0:
        name = 'Backup {}{}'.format(
            '{} '.format(subfolder) if len(subfolder or '') > 0 else '', datetime.datetime.now().strftime('%y.%m')
        )

    # Collect directories that needs to be backed up

    session = accsyn_api.Session()

    share = args.share
    if len(share or '') == 0:
        info('Quering default share..')
        s = session.find_one('share where default=true')
        assert s, 'No default share is configured!'
        share = s['code']

    info("Locating accsyn job '{}' (API version: {})".format(name, accsyn_api.__version__))
    j = session.find_one('Job WHERE code="{}"'.format(name))
    new_job_data = None
    if j is None:
        exclude = args.exclude or ''
        if len(exclude) == 0:
            exclude = DEFAULT_EXCLUDE
        new_job_data = {
            'code': name,
            'tasks': {},
            'settings': {'task_bucketsize': '1', 'transfer_mode': args.mode or 'copy', 'transfer_exclude': exclude},
            'mirror': True,
        }
        warning((' ' * 3) + 'Not found, creating new!')
    else:
        info("Got existing backup job: {}".format(j['code']))

    # Find existing backup tasks
    existing_tasks = session.find("task WHERE job.id={}".format(j['id'])) if j else []

    # Flag all as missing
    for t in existing_tasks:
        t['_missing'] = True

    task_edit_data = []
    new_tasks = []

    def process_entry(filename):
        do_exclude = False
        for x in exclude_folders:
            if re.match(x, filename):
                info((" " * 3) + "({}) Excluded!".format(filename))
                do_exclude = True
                break
        if do_exclude:
            return
        # Locate it
        found = False
        for t in existing_tasks:
            dirname = os.path.basename(t['source']['path'])
            if dirname.lower() == filename.lower():
                found = True
                t['_missing'] = False
                if t['status'] == "excluded":
                    # Include again
                    task_edit_data.append({"id": t['id'], "status": "queued"})
                    info((" " * 3) + "({}) Including in backup again!".format(filename))
                else:
                    if t['status'] in ["done", "failed"]:
                        task_edit_data.append({"id": t['id'], "status": "queued"})
                    info((" " * 3) + "({}) Verified - will be backed up!".format(filename))
                break
        if not found:
            info((" " * 3) + "({}) Adding new file/directory to backup!".format(filename))
            accsyn_source_path = '{}:'.format(source) if len(source) > 0 else ''
            accsyn_source_path = '{}share={}'.format(accsyn_source_path, share)
            if subfolder:
                accsyn_source_path = '{}/{}'.format(accsyn_source_path, subfolder)
            accsyn_source_path = '{}/{}'.format(accsyn_source_path, filename)
            accsyn_destination_path = '{}'.format(destination)
            new_tasks.append({'source': accsyn_source_path, 'destination': accsyn_destination_path})

    if path_scan:
        info('Scanning {}...'.format(path))
        for filename in os.listdir(path_scan):
            process_entry(filename)
    else:
        '''
        Parse JSON data, assume being on the form:

        backup_data = [
            {'name':"PR03",'folder':'pr03','status':'active'},
            {'name':"PR02",'folder':'pr02','status':'active'},
            {'name':"PR01",'folder':'pr01','status':'inactive'},
        ]

        '''
        for d in backup_data:
            if d['status'] == 'active':
                process_entry(d['folder'])

    # Any folders disappeared?
    for t in existing_tasks:
        if t['_missing'] is True and not t['status'] in ['excluded']:
            task_edit_data.append({"id": t['id'], "status": "excluded"})
            info(
                (" " * 3)
                + "({}) Excluded from backup - not here anymore(task: {})!".format(t['source']['path'], t['id'])
            )

    if 0 < len(task_edit_data):
        info(
            "Result of task update: {}".format(
                session.update_many("task", task_edit_data, entityid=j["id"]) if not args.dry_run else task_edit_data
            )
        )

    if 0 < len(new_tasks):
        if new_job_data:
            new_job_data['tasks'] = new_tasks
            info(
                "Result of job create: {}".format(
                    session.create("job", new_job_data) if not args.dry_run else new_job_data
                )
            )
        else:
            info(
                "Result of task create: {}".format(
                    session.create("task", {"tasks": new_tasks}, j["id"]) if not args.dry_run else new_tasks
                )
            )
    else:
        info('No new backup tasks to add')

    if j and len(task_edit_data) == 0 and len(new_tasks) == 0 and j['status'] in ["aborted", "done", "failed"]:
        job_edit_data = {"status": "waiting"}
        info(
            "Result of job resume: {}".format(
                session.update_one("job", j["id"], job_edit_data) if not args.dry_run else job_edit_data
            )
        )

    sys.exit(0)
