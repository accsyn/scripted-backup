#!/usr/local/bin/python3
# Accsyn example Python 2/3 script for automising backups. See README.md for more information.

import sys, os, json, copy, datetime

import accsyn_api

def info(s):
    s = "(Accsyn backup) %s"%(s or "")
    try:
        if ((3, 0) < sys.version_info):
            # Python 3 code in this block
            expr = """print(s)"""
        else:
            # Python 2 code in this block
            expr = """print s"""
        eval(expr)
    except:
        pass


def warning(s):
    info("[WARNING] %s"%(s or ""))

if __name__ == '__main__':

    info("Initialising")

    PROJECTS_PATH = "C:\\Accsyn_storage\\projects" # Assume Accsyn has been configured with a root share at this path

    # Information about our projects, it could come from metadata stored on disk or an project management system reachable through an REST API.
    PROJECT_DATA = [
        {'name':"PR03","status":"active"},
        {'name':"PR02","status":"active"},
        {'name':"PR01","status":"inactive"},
    ]

        

    BACKUP_SITE = "backup" # Name of the remote backup site, were a server is configured to serve the root share.

    JOB_IDENT = "Daily Backup"

    # Collect directories that needs to be backed up
    files = []
    for d in PROJECT_DATA:
        if d['status'] == "active":
            p = os.path.join(PROJECTS_PATH, d['name']) # Assume project folder on disk has the same name as project
            if os.path.exists(p) and 0<len(os.listdir(p)): # Only include if exists and have contents, do not want to sync over empty folders that might erase remote backed up contents.
                info("Project folder to be backed up: %s"%p)
                files.append(p)
            else:
                warning("Skipping non existing or empty project folder: %s"%p)

    # Create Accsyn session, make sure ACCSYN_API_DOMAIN, ACCSYN_API_USER and ACCSYN_API_KEY environment variables are properly set upon script launch.
    session = accsyn_api.Session()

    j = None

    if 0<len(files):

        # Find existing job
        j = session.find_one('job WHERE code="{0}"'.format(JOB_IDENT))

        if j is None:
            # Create a new backup job
            accsyn_job_data = {
                'code':JOB_IDENT,
                'tasks':{},
                'mirror_paths':True, # Make sure paths will end up the same at remote end relative root share
                'settings':{
                    'transfer_mode':"onewaysync", # Make remote files are deleted that do not exists on-prem
                    'task_bucketsize':"1", # Transfer each project at a time, prevents Accsyn transfer to run out of memory if many large projects.
                    'transfer_exclude':"*.tmp,.*", # Exclude temp files and files starting with "."
                    'job_done_actions':"delete_excluded" # When backup job is done, make sure Accsyn deletes excluded tasks (inactive projects) at backup site
                }
            }

            for p in files:
                uri = os.path.basename(p)
                accsyn_job_data['tasks'][uri] = {'source':p, 'destination':"site={0}".format(BACKUP_SITE)} # Each task is identified (uri) by their name

            j = session.create("job", accsyn_job_data)
            info("Successfully submitted new backup job to Accsyn!")
        else:
            # Update/retry existing backup job
            task_data = {}
            for p in files:
                uri = os.path.basename(p)
                task_data[uri] = {'source':p} # Do not need to define destination, can only be same as rest of job
            session.create("Task", task_data, j['id']) # Tasks that already exists will be retried
            info("Successfully updated existing backup job with %d task(s)!"%(len(task_data)))
    else:
        warning("No projects to back up!")

    if not j is None:
        # Exclude inactive projects - have them deleted upon job finish
        task_data = []
        for task in session.find("Task WHERE job.id={0} AND status!=excluded".format(j['id'])):
            is_active = False
            for d in PROJECT_DATA:
                if d['name'] == task['uri'] and d['status'] == "active":
                    is_active = True
                    break
            if not is_active:
                # Need to exclude this
                task_data.append({'id':task['id'],'status':"excluded"})
                info("Excluding inactive project: %s"%(task['uri']))
        if 0<len(task_data):
            session.update_many("Task", task_data, j['id'])
            info("Successfully excluded %d inactive project task(s) from Accsyn job!"%(len(task_data)))

    sys.exit(0)