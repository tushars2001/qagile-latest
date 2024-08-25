import hashlib
import inspect
import json
import math
import multiprocessing
import os
import pdb
import random
import signal
import string
import time
from datetime import datetime
import environ
import jwt
import psutil
import requests
from django.http import JsonResponse
from . import models


# Create your views here.

def jira_test_cases(request):
    out_data = {}
    if request.method == 'GET':
        if 'method' in request.GET and is_valid_method(request.GET['method']):
            runid = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
            data = {'runid': runid}

            if request.GET['method'] == 'jira_get_tcs_by_project' and 'project_id' in request.GET \
                    and len(request.GET['project_id']):
                # NOT NEEDED AS OF NOW ***************
                return JsonResponse(data)

            elif request.GET['method'] == 'jira_load_tcs_for_project' and 'project_id' in request.GET and \
                    len(request.GET['project_id']) and 'load' in request.GET and len(request.GET['load']) \
                    and ['F', 'D'].count(request.GET['load']):
                if request.GET['load'] == 'F':
                    print("starting daemon: " + str(time.time()))
                    daemonProcess = multiprocessing.Process(target=jira_load_tcs_for_project,
                                                            args=(request.GET['project_id'], runid))
                    daemonProcess.daemon = True
                    daemonProcess.start()
                    data['pid'] = daemonProcess.pid
                    # jira_load_tcs_for_project(request.GET['project_id'], runid)
                    print("outside daemon" + str(time.time()))

                    return JsonResponse(data)
                elif request.GET['load'] == 'D':
                    last_updated_date = models.jira_get_last_updated_tc_for_project(request.GET['project_id'])
                    print("starting daemon: " + str(time.time()))
                    daemonProcess = multiprocessing.Process(target=jira_load_tcs_for_project,
                                                            args=(request.GET['project_id'], runid, last_updated_date))
                    daemonProcess.daemon = True
                    daemonProcess.start()
                    data['pid'] = daemonProcess.pid
                    print("outside daemon" + str(time.time()))
                    return JsonResponse(data)
                else:
                    out_data['message'] = "Invalid value of param load"

            elif request.GET['method'] == 'jira_load_defects_for_project' and 'project_id' in request.GET and \
                    len(request.GET['project_id']) and 'load' in request.GET and len(request.GET['load']) \
                    and ['F', 'D'].count(request.GET['load']):
                if request.GET['load'] == 'F':
                    print("starting daemon: " + str(time.time()))
                    daemonProcess = multiprocessing.Process(target=jira_load_defects_for_project,
                                                            args=(request.GET['project_id'], runid))
                    daemonProcess.daemon = True
                    daemonProcess.start()
                    data['pid'] = daemonProcess.pid
                    print("outside daemon" + str(time.time()))

                    return JsonResponse(data)
                elif request.GET['load'] == 'D':
                    last_updated_date = models.jira_get_last_updated_defect_for_project(request.GET['project_id'])
                    print("starting daemon: " + str(time.time()))
                    daemonProcess = multiprocessing.Process(target=jira_load_defects_for_project,
                                                            args=(request.GET['project_id'], runid, last_updated_date))
                    daemonProcess.daemon = True
                    daemonProcess.start()
                    data['pid'] = daemonProcess.pid
                    print("outside daemon" + str(time.time()))

                    return JsonResponse(data)
                else:
                    out_data['message'] = "Invalid value of param load"

            elif request.GET['method'] == 'jira_load_executions_by_project' and 'project_id' in request.GET and \
                    len(request.GET['project_id']) and 'load' in request.GET and len(request.GET['load']) \
                    and ['F', 'D'].count(request.GET['load']):
                project_name = models.get_project_name_by_id(request.GET['project_id'])
                if project_name:
                    if request.GET['load'] == 'F':
                        print("starting daemon: " + str(time.time()))
                        print("parent : " + str(multiprocessing.current_process().pid))
                        daemonProcess = multiprocessing.Process(target=jira_load_executions_by_project,
                                                                args=(request.GET['project_id'], runid))
                        daemonProcess.daemon = True
                        daemonProcess.start()
                        data['pid'] = daemonProcess.pid
                        print("outside daemon" + str(time.time()))
                    elif request.GET['load'] == 'D':
                        last_updated_date = models.jira_get_last_updated_execution_for_project(
                            request.GET['project_id'])
                        print("starting daemon: " + str(time.time()))
                        # jira_load_executions_by_project(request.GET['project_id'], runid, last_updated_date)
                        print("parent : " + str(multiprocessing.current_process().pid))
                        daemonProcess = multiprocessing.Process(target=jira_load_executions_by_project,
                                                                args=(
                                                                    request.GET['project_id'], runid,
                                                                    last_updated_date))
                        daemonProcess.daemon = True
                        daemonProcess.start()
                        data['pid'] = daemonProcess.pid
                        print("outside daemon" + str(time.time()))
                    else:
                        out_data['message'] = "Invalid value of param load"

                else:
                    data = {'error': 'Project not found'}
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_load_executions_by_project_no_data':
                projects_no_data = models.get_all_projects_with_no_exec_data('jira')
                last = jira_load_executions_by_project_no_data(projects_no_data, runid)

            elif request.GET['method'] == 'job_status' and 'runid' in request.GET and \
                    len(request.GET['runid']):
                start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                load_log(runid, 'NA', 'processing', start_time, None, 'Status', 0, 0, 'NA',
                         None, None, None)
                job_status = models.get_job_status_by_runid(request.GET['runid'])
                load_log(runid, 'NA', 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'Status', 0,
                         0, 'NA',
                         None, None, None)
                return JsonResponse({'job_status': job_status})

            elif request.GET['method'] == 'kill_process' and 'pid' in request.GET:

                message = {'message': kill_process(request.GET['pid'], runid)}

                return JsonResponse(message, safe=False)

            elif request.GET['method'] == 'jira_load_all_projects':
                print("starting daemon: " + str(time.time()))
                daemonProcess = multiprocessing.Process(target=jira_load_all_projects, args=(runid,))
                daemonProcess.daemon = True
                daemonProcess.start()
                data['pid'] = daemonProcess.pid
                print("outside daemon" + str(time.time()))
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_user_refresh':
                print("starting daemon: " + str(time.time()))
                # jira_user_refresh(runid)
                daemonProcess = multiprocessing.Process(target=jira_user_refresh, args=(runid,))
                daemonProcess.daemon = True
                daemonProcess.start()
                data['pid'] = daemonProcess.pid
                print("outside daemon" + str(time.time()))
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_load_all_cycles':
                print("starting daemon: " + str(time.time()))
                # daemonProcess = multiprocessing.Process(target=jira_load_all_projects, args=(runid,))
                # daemonProcess.daemon = True
                # daemonProcess.start()
                # data['pid'] = daemonProcess.pid
                result = jira_load_all_cycles(runid)
                print("outside daemon" + str(time.time()))
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_load_all_users':
                print("starting daemon: " + str(time.time()))
                # daemonProcess = multiprocessing.Process(target=jira_load_all_projects, args=(runid,))
                # daemonProcess.daemon = True
                # daemonProcess.start()
                # data['pid'] = daemonProcess.pid
                result = jira_load_all_users(runid)
                print("outside daemon" + str(time.time()))
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_load_tcs_for_all_projects' \
                    and 'load' in request.GET and len(request.GET['load']) and ['F', 'D'].count(request.GET['load']):
                print("starting daemon: " + str(time.time()))
                daemonProcess = multiprocessing.Process(target=jira_load_tcs_for_all_projects,
                                                        args=(request.GET['load'], runid))
                daemonProcess.daemon = True
                daemonProcess.start()
                data['pid'] = daemonProcess.pid
                # jira_load_tcs_for_all_projects(request.GET['load'], runid)
                print("outside daemon" + str(time.time()))
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_load_defects_for_all_projects' \
                    and 'load' in request.GET and len(request.GET['load']) and ['F', 'D'].count(request.GET['load']):
                print("starting daemon: " + str(time.time()))
                daemonProcess = multiprocessing.Process(target=jira_load_defects_for_all_projects,
                                                        args=(request.GET['load'], runid))
                daemonProcess.daemon = True
                daemonProcess.start()
                data['pid'] = daemonProcess.pid
                # jira_load_defects_for_all_projects(request.GET['load'], runid)
                print("outside daemon" + str(time.time()))
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_load_executions_for_all_projects' \
                    and 'load' in request.GET and len(request.GET['load']) and ['F', 'D'].count(request.GET['load']):
                print("starting daemon: " + str(time.time()))
                daemonProcess = multiprocessing.Process(target=jira_load_executions_for_all_projects,
                                                        args=(request.GET['load'], runid))
                daemonProcess.daemon = True
                daemonProcess.start()
                data['pid'] = daemonProcess.pid
                # jira_load_executions_for_all_project(request.GET['load'], runid)
                print("outside daemon" + str(time.time()))
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_load_executions_for_all_projects_full':
                print("starting daemon: " + str(time.time()))
                daemonProcess = multiprocessing.Process(target=jira_load_executions_for_all_projects_full,
                                                        args=(runid,))
                daemonProcess.daemon = True
                daemonProcess.start()
                data['pid'] = daemonProcess.pid
                # jira_load_executions_for_all_project(request.GET['load'], runid)
                print("outside daemon" + str(time.time()))
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_get_last_updated_tc_for_project' and 'project_id' in request.GET and \
                    len(request.GET['project_id']):
                last_updated_date = models.jira_get_last_updated_tc_for_project(request.GET['project_id'])
                return JsonResponse({'last_updated_date': last_updated_date})

            elif request.GET['method'] == 'running_jobs':
                pids = psutil.pids()
                pids_running = []
                for pid in range(len(pids)):
                    if psutil.pid_exists(pids[pid]) and psutil.Process(pids[pid]).status() == 'running':
                        pids_running.append(pids[pid])
                data = models.get_runids_from_pids(pids_running)
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'show_daemons':
                print("PID of Child Process is: {}".format(multiprocessing.current_process().pid))

            elif request.GET['method'] == 'long_process':
                print("starting daemon: " + str(time.time()))
                print("parent : " + str(multiprocessing.current_process().pid))
                daemonProcess = multiprocessing.Process(target=long_process,
                                                        args=(request.GET['t'], runid), name="qagile")
                daemonProcess.daemon = True

                daemonProcess.start()
                data['pid'] = daemonProcess.pid
                print("outside daemon" + str(time.time()))
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'kill_long_running_jobs':
                message = kill_long_running_jobs()
                return JsonResponse(message, safe=False)

            else:
                out_data['message'] = "Missing/invalid params"
        else:
            out_data['message'] = "Invalid Method"
    else:
        out_data['message'] = "Only GET is supported"
    return JsonResponse(out_data)


def jira_load_tcs_for_project(project_id, runid, last_updated=None, load_type=None):
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_status = None
    try:
        response = {}
        api = "https://jira-us-aholddelhaize.atlassian.net/rest/api/3/search"
        jql = 'project in ("' + project_id + '") and type in (Test)'
        fields = "id, key, assignee, reporter, updated, summary, priority, creator, created"
        if last_updated:
            jql = jql + " and updated >= '" + last_updated + "'"
        if not load_type:
            if last_updated:
                load_type = 'D'
            else:
                load_type = 'F'

        url = api + '?jql=' + jql + '&fields=id'
        payload = {}
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
        }
        print(url)
        load_log(runid, url, 'WaitingJira-1', start_time, None,
                 inspect.stack()[0][3], None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, project_id)
        response = requests.request("GET", url, headers=headers, data=payload)
        load_log(runid, url, 'GotJira-1', start_time, None,
                 inspect.stack()[0][3], None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, project_id)
        if is_json(response.text):
            response_json = json.loads(response.text)
            if 'errorMessages' in response_json and len(response_json['errorMessages']):
                pages = 0
                response_json['total'] = 0
            else:
                pages = math.ceil(response_json['total'] / 50)
            startAt = 0
            for page in range(pages):
                records = []
                startAt = page * 50
                url = api + '?jql=' + jql + '&fields=' + fields + '&startAt=' + str(startAt)
                print(url)
                load_log(runid, url, 'WaitingJira-2', start_time, None,
                         inspect.stack()[0][3], response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, project_id)
                response = requests.request("GET", url, headers=headers, data=payload)
                load_log(runid, url, 'GotJira-2', start_time, None,
                         inspect.stack()[0][3], response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, project_id)
                if is_json(response.text):
                    response_json = json.loads(response.text)
                    record = {
                        'tcid': '',
                        'source_id': '',
                        'title': '',
                        'created': '',
                        'updated': '',
                        'created_by': '',
                        'source': 'Jira',
                        'project_id': ''
                    }
                    for issue in response_json['issues']:
                        created_by = None
                        if issue['fields']['reporter']:
                            created_by = issue['fields']['reporter']['displayName']
                        record = {
                            'tcid': issue['key'],
                            'source_id': issue['id'],
                            'title': issue['fields']['summary'].encode('ascii', 'ignore').decode('ascii'),
                            'created': issue['fields']['created'].split('.')[0],
                            'updated': issue['fields']['updated'].split('.')[0],
                            'created_by': created_by,
                            'source': 'Jira',
                            'project_id': project_id
                        }
                        records.append(record.copy())

                    action = models.load_tc_records(records, runid)
                else:
                    load_log(runid, url, 'processing', start_time, None,
                             inspect.stack()[0][3], None, None, jql,
                             multiprocessing.current_process().pid, load_type, project_id)
            load_log(runid, url, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     inspect.stack()[0][3], response_json['total'], response_json['total'], jql,
                     multiprocessing.current_process().pid, load_type, project_id)
            models.compress_load_log()
            last_status = models.get_final_log(runid)
            return last_status
        else:
            load_log(runid, None, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     "jira_load_tcs_for_project", None, None, str(response)[:200],
                     multiprocessing.current_process().pid,
                     load_type,
                     project_id)
    except Exception as e:
        load_log(runid, None, 'Abort-Exception', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "jira_load_tcs_for_project", None, None, str(e)[:200], multiprocessing.current_process().pid,
                 load_type, project_id)
        raise e


def jira_load_tcs_for_project_no_data(project_ids, runid, last_updated=None, load_type=None):
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_status = None
    try:
        response = {}
        api = "https://jira-us-aholddelhaize.atlassian.net/rest/api/3/search"
        project_id_zql = ''
        for rec in project_ids:
            project_id_zql = project_id_zql + "'" + rec['project_id'] + "',"
        project_id_zql = project_id_zql + "'ABC'"

        jql = "project in (" + project_id_zql + ") and type in (Test)"
        fields = "id, key, assignee, reporter, updated, summary, priority, creator, created"
        if last_updated:
            jql = jql + " and updated >= '" + last_updated + "'"
        if not load_type:
            if last_updated:
                load_type = 'D'
            else:
                load_type = 'F'

        url = api + '?jql=' + jql + '&fields=id'
        payload = {}
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
        }
        print(url)
        load_log(runid, url, 'WaitingJira-1', start_time, None,
                 "jira_load_tcs_for_project", None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, "Multi")
        response = requests.request("GET", url, headers=headers, data=payload)
        load_log(runid, url, 'GotJira-1', start_time, None,
                 "jira_load_tcs_for_project", None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, "Multi")
        if is_json(response.text):
            response_json = json.loads(response.text)
            if 'errorMessages' in response_json and len(response_json['errorMessages']):
                pages = 0
                response_json['total'] = 0
            else:
                pages = math.ceil(response_json['total'] / 50)
            startAt = 0
            for page in range(pages):
                records = []
                startAt = page * 50
                url = api + '?jql=' + jql + '&fields=' + fields + '&startAt=' + str(startAt)
                print(url)
                load_log(runid, url, 'WaitingJira-2', start_time, None,
                         "jira_load_tcs_for_project", response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, "Multi")
                response = requests.request("GET", url, headers=headers, data=payload)
                load_log(runid, url, 'GotJira-2', start_time, None,
                         "jira_load_tcs_for_project", response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, "Multi")
                if is_json(response.text):
                    response_json = json.loads(response.text)
                    record = {
                        'tcid': '',
                        'source_id': '',
                        'title': '',
                        'created': '',
                        'updated': '',
                        'created_by': '',
                        'source': 'Jira',
                        'project_id': ''
                    }
                    for issue in response_json['issues']:
                        created_by = None
                        if issue['fields']['reporter']:
                            created_by = issue['fields']['reporter']['displayName']
                        record = {
                            'tcid': issue['key'],
                            'source_id': issue['id'],
                            'title': issue['fields']['summary'].encode('ascii', 'ignore').decode('ascii'),
                            'created': issue['fields']['created'].split('.')[0],
                            'updated': issue['fields']['updated'].split('.')[0],
                            'created_by': created_by,
                            'source': 'Jira',
                            'project_id': issue['key'].split("-")[0]
                        }
                        records.append(record.copy())

                    action = models.load_tc_records(records, runid)
                else:
                    load_log(runid, url, 'processing', start_time, None,
                             "jira_load_tcs_for_project", None, None, jql,
                             multiprocessing.current_process().pid, load_type, "Multi")
            load_log(runid, url, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     "jira_load_tcs_for_project", response_json['total'], response_json['total'], jql,
                     multiprocessing.current_process().pid, load_type, "Multi")
            models.compress_load_log()
            last_status = models.get_final_log(runid)
            return last_status
        else:
            load_log(runid, None, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     "jira_load_tcs_for_project", None, None, str(response)[:200],
                     multiprocessing.current_process().pid,
                     load_type,
                     "Multi")
    except Exception as e:
        load_log(runid, None, 'Abort-Exception', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "jira_load_tcs_for_project", None, None, str(e)[:200], multiprocessing.current_process().pid,
                 load_type, "Multi")
        raise e


def jira_load_tcs_for_all_projects(load, runid):
    if already_running('jira_load_tcs_for_project'):
        print("RUN-ABORT")
        load_log(runid, None, 'RUN-ABORT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 'jira_load_tcs_for_project', 0, 0, None, multiprocessing.current_process().pid, load, 'ALL')
        return {'data': [], 'error': 'This process is already running'}

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_status = None
    load_type = 'D'
    try:
        response = {}
        api = "https://jira-us-aholddelhaize.atlassian.net/rest/api/3/search"
        jql = "type = 'test' and updated >= '-15d'"
        fields = "id, key, assignee, reporter, updated, summary, priority, creator, created"

        url = api + '?jql=' + jql + '&fields=id'
        payload = {}
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
        }
        print(url)
        load_log(runid, url, 'WaitingJira-1', start_time, None,
                 "jira_load_tcs_for_project", None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, "Multi")
        response = requests.request("GET", url, headers=headers, data=payload)
        load_log(runid, url, 'GotJira-1', start_time, None,
                 "jira_load_tcs_for_project", None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, "Multi")
        if is_json(response.text):
            response_json = json.loads(response.text)
            if 'errorMessages' in response_json and len(response_json['errorMessages']):
                pages = 0
                response_json['total'] = 0
            else:
                pages = math.ceil(response_json['total'] / 50)
            startAt = 0
            for page in range(pages):
                records = []
                startAt = page * 50
                url = api + '?jql=' + jql + '&fields=' + fields + '&startAt=' + str(startAt)
                print(url)
                load_log(runid, url, 'WaitingJira-2', start_time, None,
                         "jira_load_tcs_for_project", response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, "Multi")
                response = requests.request("GET", url, headers=headers, data=payload)
                load_log(runid, url, 'GotJira-2', start_time, None,
                         "jira_load_tcs_for_project", response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, "Multi")
                if is_json(response.text):
                    response_json = json.loads(response.text)
                    record = {
                        'tcid': '',
                        'source_id': '',
                        'title': '',
                        'created': '',
                        'updated': '',
                        'created_by': '',
                        'source': 'Jira',
                        'project_id': ''
                    }
                    for issue in response_json['issues']:
                        created_by = None
                        if issue['fields']['reporter']:
                            created_by = issue['fields']['reporter']['displayName']
                        record = {
                            'tcid': issue['key'],
                            'source_id': issue['id'],
                            'title': issue['fields']['summary'].encode('ascii', 'ignore').decode('ascii'),
                            'created': issue['fields']['created'].split('.')[0],
                            'updated': issue['fields']['updated'].split('.')[0],
                            'created_by': created_by,
                            'source': 'Jira',
                            'project_id': issue['key'].split("-")[0]
                        }
                        records.append(record.copy())

                    action = models.load_tc_records(records, runid)
                else:
                    load_log(runid, url, 'processing', start_time, None,
                             "jira_load_tcs_for_project", None, None, jql,
                             multiprocessing.current_process().pid, load_type, "Multi")
            load_log(runid, url, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     "jira_load_tcs_for_project", response_json['total'], response_json['total'], jql,
                     multiprocessing.current_process().pid, load_type, "Multi")
            models.compress_load_log()
            last_status = models.get_final_log(runid)
            return last_status
        else:
            load_log(runid, None, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     "jira_load_tcs_for_project", None, None, str(response)[:200],
                     multiprocessing.current_process().pid, load_type, "Multi")
    except Exception as e:
        load_log(runid, None, 'Abort-Exception', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "jira_load_tcs_for_project", None, None, str(e)[:200], multiprocessing.current_process().pid,
                 load_type, "Multi")
        raise e

    return last_status


def jira_load_tcs_for_all_projects_legacy(load, runid):
    if already_running('jira_load_tcs_for_project'):
        print("RUN-ABORT")
        load_log(runid, None, 'RUN-ABORT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 'jira_load_tcs_for_project', 0, 0, None, multiprocessing.current_process().pid, load, 'ALL')
        return {'data': [], 'error': 'This process is already running'}
    projects = models.get_all_projects_with_tcs_data('jira')
    projects_no_data = models.get_all_projects_with_no_tcs_data('jira')
    ret = {'data': []}
    load_log(runid, None, 'Starting-data', None, None,
             "jira_load_tcs_for_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')
    for record in projects:
        project_id = record['project_id']
        last_updated_date = None
        if load == 'D':
            last_updated_date = models.jira_get_last_updated_tc_for_project(project_id)
            if last_updated_date:
                print("Project: " + project_id + ", Last Updated: " + last_updated_date)
            else:
                print("Project: " + project_id + ", Doesn't have last Updated.")
        last_status = jira_load_tcs_for_project(project_id, runid, last_updated_date, load)
        ret['data'].append(last_status.copy())

    load_log(runid, None, 'Completed-data', None, None,
             "jira_load_tcs_for_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')
    load_log(runid, None, 'Starting-No-data', None, None,
             "jira_load_tcs_for_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')
    last_updated_date = None
    last_status = jira_load_tcs_for_project_no_data(projects_no_data, runid, last_updated_date, load)
    ret['data'].append(last_status.copy())
    load_log(runid, None, 'Completed-No-data', None, None,
             "jira_load_tcs_for_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')

    ret['start_time'] = ret['data'][0]['start_time']
    ret['end_time'] = ret['data'][-1]['end_time']
    ret['total_processed'] = sum(item['records_processed'] for item in ret['data'])
    return ret


def jira_load_defects_for_project(project_id, runid, last_updated=None, load_type=None):
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        response = {}
        api = "https://jira-us-aholddelhaize.atlassian.net/rest/api/3/search"
        jql = 'project in (' + project_id + ') and type in (Bug)'
        fields = "id, key, assignee, reporter, updated, summary, priority, creator, created, status, environment, assignee"
        if last_updated:
            jql = jql + " and updated >= '" + last_updated + "'"

        if not load_type:
            if last_updated:
                load_type = 'D'
            else:
                load_type = 'F'

        url = api + '?jql=' + jql + '&fields=id'
        payload = {}
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
        }
        print(url)
        load_log(runid, url, 'WaitingJira-1', start_time, None,
                 inspect.stack()[0][3], None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, project_id)
        response = requests.request("GET", url, headers=headers, data=payload)
        load_log(runid, url, 'GotJira-1', start_time, None,
                 inspect.stack()[0][3], None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, project_id)
        if is_json(response.text):

            response_json = json.loads(response.text)
            if 'errorMessages' in response_json and len(response_json['errorMessages']):
                response_json['total'] = 0
            pages = math.ceil(response_json['total'] / 50)

            startAt = 0
            for page in range(pages):
                records = []
                startAt = page * 50
                url = api + '?jql=' + jql + '&fields=' + fields + '&startAt=' + str(startAt)
                print(url)
                load_log(runid, url, 'WaitingJira-2', start_time, None,
                         inspect.stack()[0][3], response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, project_id)
                response = requests.request("GET", url, headers=headers, data=payload)
                load_log(runid, url, 'GotJira-2', start_time, None,
                         inspect.stack()[0][3], response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, project_id)
                if is_json(response.text):
                    response_json = json.loads(response.text)
                    record = {
                    }
                    for issue in response_json['issues']:
                        assignee = None
                        created_by = None
                        if issue['fields']['assignee']:
                            assignee = issue['fields']['assignee']['displayName']
                        if issue['fields']['reporter']:
                            created_by = issue['fields']['reporter']['displayName']
                        elif issue['fields']['creator']:
                            created_by = issue['fields']['creator']['displayName']
                        record = {
                            'defectid': issue['key'],
                            'source_id': issue['id'],
                            'title': issue['fields']['summary'].encode('ascii', 'ignore').decode('ascii'),
                            'created': issue['fields']['created'].split('.')[0],
                            'updated': issue['fields']['updated'].split('.')[0],
                            'created_by': created_by,
                            'source': 'Jira',
                            'assignee': assignee,
                            'sev': issue['fields']['priority']['name'],
                            'status': issue['fields']['status']['name'],
                            'project_id': project_id
                        }
                        records.append(record.copy())
                    action = models.load_defect_records(records, runid)
                else:
                    load_log(runid, url, 'processing', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                             inspect.stack()[0][3], None, None, str(response)[:200],
                             multiprocessing.current_process().pid, load_type,
                             project_id)
            load_log(runid, url, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     inspect.stack()[0][3], response_json['total'], response_json['total'], jql,
                     multiprocessing.current_process().pid, load_type, project_id)
            models.compress_load_log()
            last_status = models.get_final_log(runid)
            return last_status
        else:
            load_log(runid, None, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     inspect.stack()[0][3], None, None, str(response)[:200], multiprocessing.current_process().pid,
                     load_type, project_id)
    except Exception as e:
        load_log(runid, None, 'Abort-Exception', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "jira_load_defects_for_project", None, None, str(e)[:200], multiprocessing.current_process().pid,
                 load_type,
                 project_id)


def jira_load_defects_for_project_no_data(project_ids, runid, last_updated=None, load_type=None):
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        response = {}
        api = "https://jira-us-aholddelhaize.atlassian.net/rest/api/3/search"
        project_id_zql = ''
        for rec in project_ids:
            project_id_zql = project_id_zql + "'" + rec['project_id'] + "',"
        project_id_zql = project_id_zql + "'ABC'"

        jql = "project in (" + project_id_zql + ") and type in (Bug)"
        fields = "id, key, assignee, reporter, updated, summary, priority, creator, created, status, environment, " \
                 "assignee "
        if last_updated:
            jql = jql + " and updated >= '" + last_updated + "'"

        if not load_type:
            if last_updated:
                load_type = 'D'
            else:
                load_type = 'F'

        url = api + '?jql=' + jql + '&fields=id'
        payload = {}
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
        }
        print(url)
        load_log(runid, url, 'WaitingJira-1', start_time, None,
                 "jira_load_defects_for_project", None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, "Multi")
        response = requests.request("GET", url, headers=headers, data=payload)
        load_log(runid, url, 'GotJira-1', start_time, None,
                 "jira_load_defects_for_project", None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, "Multi")
        if is_json(response.text):

            response_json = json.loads(response.text)
            if 'errorMessages' in response_json and len(response_json['errorMessages']):
                response_json['total'] = 0
            pages = math.ceil(response_json['total'] / 50)

            startAt = 0
            for page in range(pages):
                records = []
                startAt = page * 50
                url = api + '?jql=' + jql + '&fields=' + fields + '&startAt=' + str(startAt)
                print(url)
                load_log(runid, url, 'WaitingJira-2', start_time, None,
                         "jira_load_defects_for_project", response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, "Multi")
                response = requests.request("GET", url, headers=headers, data=payload)
                load_log(runid, url, 'GotJira-2', start_time, None,
                         "jira_load_defects_for_project", response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, "Multi")
                if is_json(response.text):
                    response_json = json.loads(response.text)
                    record = {
                    }
                    for issue in response_json['issues']:
                        assignee = None
                        created_by = None
                        if issue['fields']['assignee']:
                            assignee = issue['fields']['assignee']['displayName']
                        if issue['fields']['reporter']:
                            created_by = issue['fields']['reporter']['displayName']
                        elif issue['fields']['creator']:
                            created_by = issue['fields']['creator']['displayName']
                        record = {
                            'defectid': issue['key'],
                            'source_id': issue['id'],
                            'title': issue['fields']['summary'].encode('ascii', 'ignore').decode('ascii'),
                            'created': issue['fields']['created'].split('.')[0],
                            'updated': issue['fields']['updated'].split('.')[0],
                            'created_by': created_by,
                            'source': 'Jira',
                            'assignee': assignee,
                            'sev': issue['fields']['priority']['name'],
                            'status': issue['fields']['status']['name'],
                            'project_id': issue['key'].split('-')[0]
                        }
                        records.append(record.copy())
                    action = models.load_defect_records(records, runid)
                else:
                    load_log(runid, url, 'processing', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                             "jira_load_defects_for_project", None, None, str(response)[:200],
                             multiprocessing.current_process().pid, load_type, "Multi")
            load_log(runid, url, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     "jira_load_defects_for_project", response_json['total'], response_json['total'], jql,
                     multiprocessing.current_process().pid, load_type, "Multi")
            models.compress_load_log()
            last_status = models.get_final_log(runid)
            return last_status
        else:
            load_log(runid, None, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     "jira_load_defects_for_project", None, None, str(response)[:200],
                     multiprocessing.current_process().pid,
                     load_type, "Multi")
    except Exception as e:
        load_log(runid, None, 'Abort-Exception', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "jira_load_defects_for_project", None, None, str(e)[:200], multiprocessing.current_process().pid,
                 load_type, "Multi")


def jira_load_defects_for_all_projects(load, runid):
    if already_running('jira_load_defects_for_project'):
        print("RUN-ABORT")
        load_log(runid, None, 'RUN-ABORT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 'jira_load_defects_for_project', 0, 0, None, multiprocessing.current_process().pid, load, 'ALL')
        return {'data': [], 'error': 'This process is already running'}
    ret = {'data': []}
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    last_status = None
    load_type = 'D'

    try:
        response = {}
        api = "https://jira-us-aholddelhaize.atlassian.net/rest/api/3/search"
        jql = "type = 'bug' and updated >= '-15d'"
        fields = "id, key, assignee, reporter, updated, summary, priority, creator, created, status, environment, assignee"

        url = api + '?jql=' + jql + '&fields=id'
        payload = {}
        headers = {
            'Accept': 'application/json',
            'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
        }
        print(url)
        load_log(runid, url, 'WaitingJira-1', start_time, None,
                 "jira_load_defects_for_project", None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, "Multi")
        response = requests.request("GET", url, headers=headers, data=payload)
        load_log(runid, url, 'GotJira-1', start_time, None,
                 "jira_load_defects_for_project", None, None, str(payload),
                 multiprocessing.current_process().pid, load_type, "Multi")
        if is_json(response.text):

            response_json = json.loads(response.text)
            if 'errorMessages' in response_json and len(response_json['errorMessages']):
                response_json['total'] = 0
            pages = math.ceil(response_json['total'] / 50)

            startAt = 0
            for page in range(pages):
                records = []
                startAt = page * 50
                url = api + '?jql=' + jql + '&fields=' + fields + '&startAt=' + str(startAt)
                print(url)
                load_log(runid, url, 'WaitingJira-2', start_time, None,
                         "jira_load_defects_for_project", response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, "Multi")
                response = requests.request("GET", url, headers=headers, data=payload)
                load_log(runid, url, 'GotJira-2', start_time, None,
                         "jira_load_defects_for_project", response_json['total'], startAt, jql,
                         multiprocessing.current_process().pid, load_type, "Multi")
                if is_json(response.text):
                    response_json = json.loads(response.text)
                    record = {
                    }
                    for issue in response_json['issues']:
                        assignee = None
                        created_by = None
                        if issue['fields']['assignee']:
                            assignee = issue['fields']['assignee']['displayName']
                        if issue['fields']['reporter']:
                            created_by = issue['fields']['reporter']['displayName']
                        elif issue['fields']['creator']:
                            created_by = issue['fields']['creator']['displayName']
                        record = {
                            'defectid': issue['key'],
                            'source_id': issue['id'],
                            'title': issue['fields']['summary'].encode('ascii', 'ignore').decode('ascii'),
                            'created': issue['fields']['created'].split('.')[0],
                            'updated': issue['fields']['updated'].split('.')[0],
                            'created_by': created_by,
                            'source': 'Jira',
                            'assignee': assignee,
                            'sev': issue['fields']['priority']['name'],
                            'status': issue['fields']['status']['name'],
                            'project_id': issue['key'].split("-")[0]
                        }
                        records.append(record.copy())
                    action = models.load_defect_records(records, runid)
                else:
                    load_log(runid, url, 'processing', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                             "jira_load_defects_for_project", None, None, str(response)[:200],
                             multiprocessing.current_process().pid, load_type, "Multi")
            load_log(runid, url, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     "jira_load_defects_for_project", response_json['total'], response_json['total'], jql,
                     multiprocessing.current_process().pid, load_type, "Multi")
            models.compress_load_log()
            last_status = models.get_final_log(runid)
            return last_status
        else:
            load_log(runid, None, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     "jira_load_defects_for_project", None, None, str(response)[:200],
                     multiprocessing.current_process().pid,
                     load_type, "Multi")
    except Exception as e:
        load_log(runid, None, 'Abort-Exception', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "jira_load_defects_for_project", None, None, str(e)[:200], multiprocessing.current_process().pid,
                 load_type, "Multi")
    return last_status


def jira_load_defects_for_all_projects_legacy(load, runid):
    if already_running('jira_load_defects_for_project'):
        print("RUN-ABORT")
        load_log(runid, None, 'RUN-ABORT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 'jira_load_defects_for_project', 0, 0, None, multiprocessing.current_process().pid, load, 'ALL')
        return {'data': [], 'error': 'This process is already running'}
    projects = models.get_all_projects_with_defects_data('jira')
    projects_no_data = models.get_all_projects_with_no_defects_data('jira')
    ret = {'data': []}
    load_log(runid, None, 'Starting-data', None, None,
             "jira_load_defects_for_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')
    for record in projects:
        project_id = record['project_id']
        last_updated_date = None
        if load == 'D':
            last_updated_date = models.jira_get_last_updated_defect_for_project(project_id)
            if last_updated_date:
                print("Project: " + project_id + ", Last Updated: " + last_updated_date)
            else:
                print("Project: " + project_id + ", Doesn't have last Updated.")
        last_status = jira_load_defects_for_project(project_id, runid, last_updated_date, load)
        ret['data'].append(last_status.copy())
    load_log(runid, None, 'Completed-data', None, None,
             "jira_load_defects_for_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')
    load_log(runid, None, 'Starting-No-data', None, None,
             "jira_load_defects_for_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')
    last_updated_date = None
    last_status = jira_load_defects_for_project_no_data(projects_no_data, runid, last_updated_date, load)
    ret['data'].append(last_status.copy())
    load_log(runid, None, 'Completed-No-data', None, None,
             "jira_load_defects_for_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')

    ret['start_time'] = ret['data'][0]['start_time']
    ret['end_time'] = ret['data'][-1]['end_time']
    ret['total_processed'] = sum(item['records_processed'] for item in ret['data'])
    return ret


def jira_load_executions_by_project(project_id, runid, last_updated=None, load_type=None):
    # Algo:
    # Execution Delta load:
    # 1. Get last_executed_on date
    # 2. Query zephyr to get executions greater then last_executed_on and status not in ‘unexecuted’
    # 3. Iterate results and insert into staging
    # 4. Update main table from staging
    # 5. Query zephyr for all executions for status = ‘unexecuted’ with max_record 1
    # 6. Get total_counts
    # 7. Delete all records from main tables for unexecuted
    # 8. Create fake total_counts records
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        last_status = None
        print_l("Child: " + str(multiprocessing.current_process().pid))
        relative_path = '/public/rest/api/1.0/zql/search'
        method = 'POST'
        project_name = models.get_project_name_by_id(project_id)
        if last_updated:  # Delta load case
            total_expected = 0
            total_processed = 0
            if not load_type:
                load_type = 'D'
            # Executed cases
            print("Delta Load")
            json_param = {
                "zqlQuery": 'project = "' + project_name + '" AND executionDate >= "' + last_updated + '"', "offset": 0,
                "maxRecords": 1}

            # MAKE REQUEST:
            BASE_URL = 'https://prod-api.zephyr4jiracloud.com/connect'
            load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-1', None, None,
                     inspect.stack()[0][3], None, None, str(json_param)[:200],
                     multiprocessing.current_process().pid, load_type, project_id)

            headers = jira_zql_header(relative_path, 'POST')
            print("Executed case with last date: " + last_updated, BASE_URL + relative_path, headers, json_param)
            response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
            load_log(runid, BASE_URL + relative_path, 'GotZephyr-1', None, None,
                     inspect.stack()[0][3], None, None, str(json_param)[:200],
                     multiprocessing.current_process().pid, load_type, project_id)
            json_result = {
                'type': None,
                'data': None
            }
            if is_json(response.text):
                # iterate through all executed pages
                response_json = json.loads(response.text)

                pages = math.ceil(response_json['totalCount'] / 50)
                print("Total: " + str(response_json['totalCount']))
                total_expected = response_json['totalCount']
                startAt = 0
                for page in range(pages):
                    records = []
                    startAt = page * 50
                    # json_param = {"zqlQuery": 'project = "' + project_id + '"', "offset": startAt, "maxRecords": 50}
                    json_param = {
                        "zqlQuery": 'project = "' + project_name + '" AND executionDate >= "' + last_updated + '"',
                        "offset": startAt, "maxRecords": 50}
                    print("Page " + str(page) + " . request : ")
                    load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-2', start_time, None,
                             inspect.stack()[0][3], response_json['totalCount'], startAt, str(json_param)[:200],
                             multiprocessing.current_process().pid, load_type, project_id)
                    print("Page " + str(page) + " .Sending request : " + BASE_URL + relative_path, headers, json_param)
                    response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
                    load_log(runid, BASE_URL + relative_path, 'GotZephyr-2', start_time, None,
                             inspect.stack()[0][3], response_json['totalCount'], startAt, str(json_param)[:200],
                             multiprocessing.current_process().pid, load_type, project_id)
                    print("Got response")
                    if is_json(response.text):
                        response_json = json.loads(response.text)
                        record = {
                            'tcid': '',
                            'source_id': '',
                            'executedBy': '',
                            'executedOn': '',
                            'status': '',
                            'cycleName': '',
                            'source': 'Jira',
                            'project_id': project_id,
                            'assignedTo': ''
                        }
                        for issue in response_json['searchObjectList']:
                            total_processed = total_processed + 1
                            # print(issue)
                            if 'executedBy' in issue['execution']:
                                executedBy = issue['execution']['executedBy']
                            else:
                                executedBy = None

                            if 'executedOn' in issue['execution']:
                                executedOn = issue['execution']['executedOn']
                            else:
                                executedOn = None

                            if 'assignedTo' in issue['execution']:
                                assignedTo = issue['execution']['assignedTo']
                            else:
                                assignedTo = None

                            record = {
                                'tcid': issue['issueKey'],
                                'source_id': issue['execution']['id'],
                                'executedBy': executedBy,
                                'executedOn': executedOn,
                                'status': issue['execution']['status']['name'],
                                'cycleName': issue['execution']['cycleName'],
                                'source': 'Jira',
                                'project_id': project_id,
                                'assignedTo': assignedTo
                            }
                            records.append(record.copy())
                        action = models.load_tc_executions_records(records, runid)

                    else:
                        load_log(runid, BASE_URL + relative_path, 'processing', start_time, None,
                                 inspect.stack()[0][3], None, None, str(response.text),
                                 multiprocessing.current_process().pid, load_type, project_id)
                print("Executed case completed with last execution date was " + last_updated)
                models.compress_load_log()

                load_log(runid, BASE_URL + relative_path, 'processing', start_time,
                         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                         inspect.stack()[0][3], str(total_expected), str(total_processed),
                         str(json_param)[:200],
                         multiprocessing.current_process().pid, load_type, project_id)
            else:
                load_log(runid, BASE_URL + relative_path, 'processing', None, None,
                         inspect.stack()[0][3], None, None, str(response.text),
                         multiprocessing.current_process().pid, load_type, project_id, "No executed case results")
            # Unexecuted cases
            # Steps to manage unexecuted cases
            json_param = {
                "zqlQuery": 'project = "' + project_name + '" AND executionStatus = "UNEXECUTED" '
                                                           'and creationDate >= "-1d"', "offset": 0, "maxRecords": 1}
            startAt = 0

            print("sending request for unexecuted date. Details: ", BASE_URL + relative_path, headers, json_param)
            load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-3', None, None,
                     inspect.stack()[0][3], None, None, str(json_param)[:200],
                     multiprocessing.current_process().pid, load_type, project_id)
            response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
            load_log(runid, BASE_URL + relative_path, 'GotZephyr-3', None, None,
                     inspect.stack()[0][3], None, None, str(json_param)[:200],
                     multiprocessing.current_process().pid, load_type, project_id)
            print("Got response back")
            if is_json(response.text):
                response_json = json.loads(response.text)

                pages = math.ceil(response_json['totalCount'] / 50)
                print("Total: " + str(response_json['totalCount']))
                total_expected = total_expected + response_json['totalCount']
                startAt = 0
                for page in range(pages):
                    records = []
                    startAt = page * 50
                    # json_param = {"zqlQuery": 'project = "' + project_id + '"', "offset": startAt, "maxRecords": 50}
                    json_param = {
                        "zqlQuery": 'project = "' + project_name + '" AND executionStatus = "UNEXECUTED" '
                                                                   'and creationDate >= "-1d"', "offset": startAt,
                        "maxRecords": 50}
                    print("Page " + str(page) + " . request : ")
                    load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-4', start_time, None,
                             inspect.stack()[0][3], response_json['totalCount'], startAt, str(json_param)[:200],
                             multiprocessing.current_process().pid, load_type, project_id)
                    print("Page " + str(page) + " .Sending request : " + BASE_URL + relative_path, headers,
                          json_param)
                    response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
                    load_log(runid, BASE_URL + relative_path, 'GotZephyr-4', start_time, None,
                             inspect.stack()[0][3], response_json['totalCount'], startAt, str(json_param)[:200],
                             multiprocessing.current_process().pid, load_type, project_id)
                    print("Got response")
                    if is_json(response.text):
                        response_json = json.loads(response.text)
                        record = {
                            'tcid': '',
                            'source_id': '',
                            'executedBy': '',
                            'executedOn': '',
                            'status': '',
                            'cycleName': '',
                            'source': 'Jira',
                            'project_id': project_id,
                            'assignedTo': ''
                        }
                        for issue in response_json['searchObjectList']:
                            total_processed = total_processed + 1
                            # print(issue)
                            if 'executedBy' in issue['execution']:
                                executedBy = issue['execution']['executedBy']
                            else:
                                executedBy = None

                            if 'executedOn' in issue['execution']:
                                executedOn = issue['execution']['executedOn']
                            else:
                                executedOn = None

                            if 'assignedTo' in issue['execution']:
                                assignedTo = issue['execution']['assignedTo']
                            else:
                                assignedTo = None

                            record = {
                                'tcid': issue['issueKey'],
                                'source_id': issue['execution']['id'],
                                'executedBy': executedBy,
                                'executedOn': executedOn,
                                'status': issue['execution']['status']['name'],
                                'cycleName': issue['execution']['cycleName'],
                                'source': 'Jira',
                                'project_id': project_id,
                                'assignedTo': assignedTo
                            }
                            records.append(record.copy())
                        action = models.load_tc_executions_records(records, runid)
                        print("Un-Executed case completed")
                    else:
                        response_json = None

                load_log(runid, BASE_URL + relative_path, 'finished', start_time,
                         datetime.now().strftime("%Y-%m-%d %H:%M:%S"), inspect.stack()[0][3], total_expected,
                         total_processed, str(json_param)[:200], multiprocessing.current_process().pid, load_type,
                         project_id)
                models.compress_load_log()
                ##################
                # response_json = json.loads(response.text)
                # total_records = response_json['totalCount']
                # # by creating fake records
                # records = []
                # for i in range(total_records):
                #     record = {
                #         'tcid': 'fake-' + str(i),
                #         'source_id': None,
                #         'executedBy': None,
                #         'executedOn': None,
                #         'status': 'UNEXECUTED',
                #         'cycleName': None,
                #         'source': 'Jira',
                #         'project_id': project_id,
                #         'assignedTo': None
                #     }
                #     records.append(record.copy())
                # print("Faking records...: ", records)
                # action = models.load_tc_executions_records(records, runid)
                # # models.manage_unexecuted(total_records, project_id, runid)
                # print("Faking records...done")
            else:
                print("No unexecuted found")
                load_log(runid, BASE_URL + relative_path, 'finished', start_time,
                         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                         inspect.stack()[0][3], str(total_expected), str(total_processed), str(json_param)[:200],
                         multiprocessing.current_process().pid, load_type, project_id, "No unexecuted found")
        else:
            # Full Load
            if not load_type:
                load_type = 'F'
            json_param = {
                "zqlQuery": 'project = "' + project_name + '" and creationDate >= "-52w" AND '
                                                           '(executionDate >= "-52w" or executionStatus = "UNEXECUTED")'
                , "offset": 0, "maxRecords": 1}

            # MAKE REQUEST:
            BASE_URL = 'https://prod-api.zephyr4jiracloud.com/connect'
            load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-5', None, None,
                     inspect.stack()[0][3], None, None, str(json_param)[:200],
                     multiprocessing.current_process().pid, load_type, project_id)
            print_l(runid)
            print_l("FULL: " + project_name)
            headers = jira_zql_header(relative_path, 'POST')
            response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
            load_log(runid, BASE_URL + relative_path, 'GotZephyr-5', None, None,
                     inspect.stack()[0][3], None, None, str(json_param)[:200],
                     multiprocessing.current_process().pid, load_type, project_id)
            json_result = {
                'type': None,
                'data': None
            }
            if is_json(response.text):
                response_json = json.loads(response.text)
                pages = math.ceil(response_json['totalCount'] / 50)
                print_l("FULL: " + project_name + " Total: " + str(response_json['totalCount']))
                startAt = 0
                for page in range(pages):
                    records = []
                    startAt = page * 50
                    # json_param = {"zqlQuery": 'project = "' + project_id + '"', "offset": startAt, "maxRecords": 50}
                    json_param = {
                        "zqlQuery": 'project = "' + project_name + '" and creationDate >= "-52w" '
                                                                   'AND (executionDate >= "-52w" '
                                                                   'or executionStatus= "UNEXECUTED")',
                        "offset": startAt, "maxRecords": 50}
                    load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-6', start_time, None,
                             inspect.stack()[0][3], response_json['totalCount'], startAt, str(json_param)[:200],
                             multiprocessing.current_process().pid, load_type, project_id)
                    print_l("FULL: " + project_name + " Total: " + str(
                        response_json['totalCount']) + "sending page: " + str(page))
                    response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
                    load_log(runid, BASE_URL + relative_path, 'GotZephyr-6', start_time, None,
                             inspect.stack()[0][3], response_json['totalCount'], startAt, str(json_param)[:200],
                             multiprocessing.current_process().pid, load_type, project_id)
                    if is_json(response.text):
                        response_json = json.loads(response.text)
                        print_l("FULL: " + project_name + " Total: " + str(
                            response_json['totalCount']) + "got page: " + str(page))
                        record = {
                            'tcid': '',
                            'source_id': '',
                            'executedBy': '',
                            'executedOn': '',
                            'status': '',
                            'cycleName': '',
                            'source': 'Jira',
                            'project_id': project_id,
                            'assignedTo': ''
                        }
                        for issue in response_json['searchObjectList']:
                            # print(issue)
                            if 'executedBy' in issue['execution']:
                                executedBy = issue['execution']['executedBy']
                            else:
                                executedBy = None

                            if 'executedOn' in issue['execution']:
                                executedOn = issue['execution']['executedOn']
                            else:
                                executedOn = None

                            if 'assignedTo' in issue['execution']:
                                assignedTo = issue['execution']['assignedTo']
                            else:
                                assignedTo = None

                            record = {
                                'tcid': issue['issueKey'],
                                'source_id': issue['execution']['id'],
                                'executedBy': executedBy,
                                'executedOn': executedOn,
                                'status': issue['execution']['status']['name'],
                                'cycleName': issue['execution']['cycleName'],
                                'source': 'Jira',
                                'project_id': project_id,
                                'assignedTo': assignedTo
                            }
                            records.append(record.copy())
                        action = models.load_tc_executions_records(records, runid)
                    else:
                        load_log(runid, BASE_URL + relative_path, 'processing', None, None,
                                 inspect.stack()[0][3], None, None, str(response.text),
                                 multiprocessing.current_process().pid, load_type, project_id, "No execution found")
                load_log(runid, BASE_URL + relative_path, 'finished', start_time,
                         datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                         inspect.stack()[0][3], response_json['totalCount'], response_json['totalCount'],
                         str(json_param)[:200],
                         multiprocessing.current_process().pid, load_type, project_id)
                models.compress_load_log()
                last_status = models.get_final_log(runid)
                return last_status
            else:
                load_log(runid, BASE_URL + relative_path, 'finished', None, None,
                         inspect.stack()[0][3], None, None, str(response.text),
                         multiprocessing.current_process().pid, load_type, project_id, "No execution found")
    except Exception as e:
        load_log(runid, None, 'Abort-Exception', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "jira_load_executions_by_project", None, None, str(e)[:200], multiprocessing.current_process().pid,
                 load_type,
                 project_id)
    last_status = models.get_final_log(runid)
    return last_status


def jira_load_executions_by_project_no_data(project_ids, runid, last_updated=None, load_type=None):
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        last_status = None
        print_l("Child: " + str(multiprocessing.current_process().pid))
        relative_path = '/public/rest/api/1.0/zql/search'
        method = 'POST'
        projects_data = models.get_project_names_by_ids(project_ids)
        # Full Load
        if not load_type:
            load_type = 'F'
        project_names = ''
        for rec in projects_data:
            project_names = project_names + "'" + rec['title'].replace("'", "\\'") + "',"
        project_names = project_names.replace("\t", "") + "'qagile'"
        json_param = {
            "zqlQuery": "project in (" + project_names + ") and creationDate >= '-52w' AND "
                                                         "(executionDate >= '-52w' or executionStatus = 'UNEXECUTED')"
            , "offset": 0, "maxRecords": 1}

        # MAKE REQUEST:
        BASE_URL = 'https://prod-api.zephyr4jiracloud.com/connect'
        load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-7', None, None,
                 'jira_load_executions_by_project', None, None, str(json_param)[:200],
                 multiprocessing.current_process().pid, load_type, 'MULTI', "jira_load_executions_by_project_no_data")
        headers = jira_zql_header(relative_path, 'POST')
        response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
        load_log(runid, BASE_URL + relative_path, 'GotZephyr-7', None, None,
                 'jira_load_executions_by_project', None, None, str(json_param)[:200],
                 multiprocessing.current_process().pid, load_type, "MULTI", "jira_load_executions_by_project_no_data")
        json_result = {
            'type': None,
            'data': None
        }
        if is_json(response.text):
            response_json = json.loads(response.text)
            pages = math.ceil(response_json['totalCount'] / 50)
            startAt = 0
            for page in range(pages):
                records = []
                startAt = page * 50
                # json_param = {"zqlQuery": 'project = "' + project_id + '"', "offset": startAt, "maxRecords": 50}
                json_param = {
                    "zqlQuery": 'project in (' + project_names + ') and creationDate >= "-52w" '
                                                                 'AND (executionDate >= "-52w" or executionStatus= '
                                                                 '"UNEXECUTED")',
                    "offset": startAt, "maxRecords": 50}
                load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-8', start_time, None,
                         'jira_load_executions_by_project', response_json['totalCount'], startAt, str(json_param)[:200],
                         multiprocessing.current_process().pid, load_type, "MULTI",
                         "jira_load_executions_by_project_no_data")
                response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
                load_log(runid, BASE_URL + relative_path, 'GotZephyr-8', start_time, None,
                         'jira_load_executions_by_project', response_json['totalCount'], startAt, str(json_param)[:200],
                         multiprocessing.current_process().pid, load_type, "MULTI",
                         "jira_load_executions_by_project_no_data")
                if is_json(response.text):
                    response_json = json.loads(response.text)
                    record = {
                        'tcid': '',
                        'source_id': '',
                        'executedBy': '',
                        'executedOn': '',
                        'status': '',
                        'cycleName': '',
                        'source': 'Jira',
                        'project_id': '',
                        'assignedTo': ''
                    }
                    for issue in response_json['searchObjectList']:
                        # print(issue)
                        if 'executedBy' in issue['execution']:
                            executedBy = issue['execution']['executedBy']
                        else:
                            executedBy = None

                        if 'executedOn' in issue['execution']:
                            executedOn = issue['execution']['executedOn']
                        else:
                            executedOn = None

                        if 'assignedTo' in issue['execution']:
                            assignedTo = issue['execution']['assignedTo']
                        else:
                            assignedTo = None
                        project_id = issue['issueKey'].split("-")[0]

                        record = {
                            'tcid': issue['issueKey'],
                            'source_id': issue['execution']['id'],
                            'executedBy': executedBy,
                            'executedOn': executedOn,
                            'status': issue['execution']['status']['name'],
                            'cycleName': issue['execution']['cycleName'],
                            'source': 'Jira',
                            'project_id': project_id,
                            'assignedTo': assignedTo
                        }
                        records.append(record.copy())
                    action = models.load_tc_executions_records(records, runid)
                else:
                    load_log(runid, BASE_URL + relative_path, 'processing', None, None,
                             'jira_load_executions_by_project', None, None, str(response.text)[:200],
                             multiprocessing.current_process().pid, load_type, "MULTI",
                             "jira_load_executions_by_project_no_data")
            load_log(runid, BASE_URL + relative_path, 'finished', start_time,
                     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     'jira_load_executions_by_project', response_json['totalCount'], response_json['totalCount'],
                     str(json_param)[:200],
                     multiprocessing.current_process().pid, load_type, "MULTI",
                     "jira_load_executions_by_project_no_data")
            models.compress_load_log()
            last_status = models.get_final_log(runid)
            return last_status
        else:
            load_log(runid, BASE_URL + relative_path, 'finished', None, None,
                     'jira_load_executions_by_project', None, None, str(response.text)[:200],
                     multiprocessing.current_process().pid, load_type,
                     "MULTI", "jira_load_executions_by_project_no_data -No execution found")
    except Exception as e:
        load_log(runid, None, 'Abort-Exception', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "jira_load_executions_by_project", None, None, str(e)[:200],
                 multiprocessing.current_process().pid, load_type,
                 "Multi", "jira_load_executions_by_project_no_data")
        raise e
    last_status = models.get_final_log(runid)
    return last_status


def jira_load_executions_for_all_projects(load, runid):
    load = 'D'
    load_type = 'D'
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if already_running('jira_load_executions_by_project'):
        print("RUN-ABORT")
        load_log(runid, None, 'RUN-ABORT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 'jira_load_executions_by_project', 0, 0, None, multiprocessing.current_process().pid, load, 'ALL')
        return {'data': [], 'error': 'This process is already running'}
    ret = {'data': []}
    load_log(runid, None, 'Starting-new', start_time, None,
             "jira_load_executions_by_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')
    try:
        last_status = None
        print_l("Child: " + str(multiprocessing.current_process().pid))
        relative_path = '/public/rest/api/1.0/zql/search'
        method = 'POST'
        json_param = {
            "zqlQuery": "executionDate >= '-15d' or (executionStatus = UNEXECUTED and creationDate >= '-15d')"
            , "offset": 0, "maxRecords": 1}

        # MAKE REQUEST:
        BASE_URL = 'https://prod-api.zephyr4jiracloud.com/connect'
        load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-7', None, None,
                 'jira_load_executions_by_project', None, None, str(json_param)[:200],
                 multiprocessing.current_process().pid, load_type, 'MULTI', "jira_load_executions_by_for_all")
        headers = jira_zql_header(relative_path, 'POST')
        response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
        load_log(runid, BASE_URL + relative_path, 'GotZephyr-7', None, None,
                 'jira_load_executions_by_project', None, None, str(json_param)[:200],
                 multiprocessing.current_process().pid, load_type, "MULTI", "jira_load_executions_by_for_all")
        json_result = {
            'type': None,
            'data': None
        }
        if is_json(response.text):
            response_json = json.loads(response.text)
            pages = math.ceil(response_json['totalCount'] / 50)
            startAt = 0
            for page in range(pages):
                records = []
                startAt = page * 50
                # json_param = {"zqlQuery": 'project = "' + project_id + '"', "offset": startAt, "maxRecords": 50}
                json_param = {
                    "zqlQuery": "executionDate >= '-15d' or (executionStatus = UNEXECUTED and creationDate >= '-15d')"
                    , "offset": startAt, "maxRecords": 50}
                load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-8', start_time, None,
                         'jira_load_executions_by_project', response_json['totalCount'], startAt, str(json_param)[:200],
                         multiprocessing.current_process().pid, load_type, "MULTI",
                         "")
                response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
                load_log(runid, BASE_URL + relative_path, 'GotZephyr-8', start_time, None,
                         'jira_load_executions_by_project', response_json['totalCount'], startAt, str(json_param)[:200],
                         multiprocessing.current_process().pid, load_type, "MULTI",
                         "")
                if is_json(response.text):
                    response_json = json.loads(response.text)
                    print(response_json)
                    record = {
                        'tcid': '',
                        'source_id': '',
                        'executedBy': '',
                        'executedOn': '',
                        'status': '',
                        'cycleName': '',
                        'source': 'Jira',
                        'project_id': '',
                        'assignedTo': ''
                    }
                    for issue in response_json['searchObjectList']:
                        # print(issue)
                        if 'executedBy' in issue['execution']:
                            executedBy = issue['execution']['executedBy']
                        else:
                            executedBy = None

                        if 'executedOn' in issue['execution']:
                            executedOn = issue['execution']['executedOn']
                        else:
                            executedOn = None

                        if 'assignedTo' in issue['execution']:
                            assignedTo = issue['execution']['assignedTo']
                        else:
                            assignedTo = None
                        project_id = issue['issueKey'].split("-")[0]

                        record = {
                            'tcid': issue['issueKey'],
                            'source_id': issue['execution']['id'],
                            'executedBy': executedBy,
                            'executedOn': executedOn,
                            'status': issue['execution']['status']['name'],
                            'cycleName': issue['execution']['cycleName'],
                            'source': 'Jira',
                            'project_id': project_id,
                            'assignedTo': assignedTo
                        }
                        records.append(record.copy())
                    action = models.load_tc_executions_records(records, runid)
                else:
                    load_log(runid, BASE_URL + relative_path, 'processing', None, None,
                             'jira_load_executions_by_project', None, None, str(response.text)[:200],
                             multiprocessing.current_process().pid, load_type, "MULTI",
                             "")
            load_log(runid, BASE_URL + relative_path, 'finished', start_time,
                     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     'jira_load_executions_by_project', response_json['totalCount'], response_json['totalCount'],
                     str(json_param)[:200],
                     multiprocessing.current_process().pid, load_type, "MULTI",
                     "")
            models.compress_load_log()
            last_status = models.get_final_log(runid)
            return last_status
        else:
            load_log(runid, BASE_URL + relative_path, 'finished', None, None,
                     'jira_load_executions_by_project', None, None, str(response.text)[:200],
                     multiprocessing.current_process().pid, load_type,
                     "MULTI", " -No execution found")
    except Exception as e:
        load_log(runid, None, 'Abort-Exception', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "jira_load_executions_by_project", None, None, str(e)[:200],
                 multiprocessing.current_process().pid, load_type,
                 "Multi", "")
    last_status = models.get_final_log(runid)

    load_log(runid, None, 'Completed-new', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
             "jira_load_executions_by_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')

    return last_status


def jira_load_executions_for_all_projects_full(runid, startAt=0):
    load = 'F'
    load_type = 'F'
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if already_running('jira_load_executions_by_project'):
        print("RUN-ABORT")
        load_log(runid, None, 'RUN-ABORT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 'jira_load_executions_by_project', 0, 0, None, multiprocessing.current_process().pid, load, 'ALL')
        return {'data': [], 'error': 'This process is already running'}
    ret = {'data': []}
    load_log(runid, None, 'Starting-new', start_time, None,
             "jira_load_executions_by_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')
    try:
        last_status = None
        print_l("Child: " + str(multiprocessing.current_process().pid))
        relative_path = '/public/rest/api/1.0/zql/search'
        method = 'POST'
        json_param = {
            "zqlQuery": "creationDate >= '-52w'"
            , "offset": 0, "maxRecords": 1}

        # MAKE REQUEST:
        BASE_URL = 'https://prod-api.zephyr4jiracloud.com/connect'
        load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-1F', None, None,
                 'jira_load_executions_by_project', None, None, str(json_param)[:200],
                 multiprocessing.current_process().pid, load_type, 'MULTI', "jira_load_executions_by_for_all")
        headers = jira_zql_header(relative_path, 'POST')
        response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
        load_log(runid, BASE_URL + relative_path, 'GotZephyr-1F', None, None,
                 'jira_load_executions_by_project', None, None, str(json_param)[:200],
                 multiprocessing.current_process().pid, load_type, "MULTI", "jira_load_executions_by_for_all")
        json_result = {
            'type': None,
            'data': None
        }
        if is_json(response.text):
            response_json = json.loads(response.text)
            pages = math.ceil(response_json['totalCount'] / 50)
            print("Total pages " + str(pages))
            startAt = int(startAt)
            pageStart = startAt / 50
            for page in range(int(pageStart), pages):
                records = []
                startAt = page * 50
                print("Loading page " + str(page))
                # json_param = {"zqlQuery": 'project = "' + project_id + '"', "offset": startAt, "maxRecords": 50}
                json_param = {
                    "zqlQuery": "creationDate >= '-52w' order by creationDate ASC"
                    , "offset": startAt, "maxRecords": 50}
                load_log(runid, BASE_URL + relative_path, 'WaitingZephyr-2F', start_time, None,
                         'jira_load_executions_by_project', response_json['totalCount'], startAt, str(json_param)[:200],
                         multiprocessing.current_process().pid, load_type, "MULTI",
                         "")
                response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
                load_log(runid, BASE_URL + relative_path, 'GotZephyr-2F', start_time, None,
                         'jira_load_executions_by_project', response_json['totalCount'], startAt, str(json_param)[:200],
                         multiprocessing.current_process().pid, load_type, "MULTI",
                         "")
                if is_json(response.text):
                    response_json = json.loads(response.text)
                    record = {
                        'tcid': '',
                        'source_id': '',
                        'executedBy': '',
                        'executedOn': '',
                        'status': '',
                        'cycleName': '',
                        'source': 'Jira',
                        'project_id': '',
                        'assignedTo': '',
                        'cycleId': '',
                        'versionId': '',
                    }
                    for issue in response_json['searchObjectList']:
                        # print(issue)
                        if 'executedBy' in issue['execution']:
                            executedBy = issue['execution']['executedBy']
                        else:
                            executedBy = None

                        if 'executedOn' in issue['execution']:
                            executedOn = issue['execution']['executedOn']
                        else:
                            executedOn = None

                        if 'assignedTo' in issue['execution']:
                            assignedTo = issue['execution']['assignedTo']
                        else:
                            assignedTo = None
                        project_id = issue['issueKey'].split("-")[0]

                        record = {
                            'tcid': issue['issueKey'],
                            'source_id': issue['execution']['id'],
                            'executedBy': executedBy,
                            'executedOn': executedOn,
                            'status': issue['execution']['status']['name'],
                            'cycleName': issue['execution']['cycleName'],
                            'source': 'Jira',
                            'project_id': project_id,
                            'assignedTo': assignedTo,
                            'cycleId': issue['execution']['cycleId'],
                            'versionId': issue['execution']['versionId'],
                        }
                        records.append(record.copy())
                    action = models.load_tc_executions_records(records, runid)
                else:
                    load_log(runid, BASE_URL + relative_path, 'processing', None, None,
                             'jira_load_executions_by_project', None, None, str(response.text)[:200],
                             multiprocessing.current_process().pid, load_type, "MULTI",
                             "")
            load_log(runid, BASE_URL + relative_path, 'finished', start_time,
                     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     'jira_load_executions_by_project', response_json['totalCount'], response_json['totalCount'],
                     str(json_param)[:200],
                     multiprocessing.current_process().pid, load_type, "MULTI",
                     "")
            models.compress_load_log()
            last_status = models.get_final_log(runid)
            return last_status
        else:
            load_log(runid, BASE_URL + relative_path, 'finished', None, None,
                     'jira_load_executions_by_project', None, None, str(response.text)[:200],
                     multiprocessing.current_process().pid, load_type,
                     "MULTI", " -No execution found")
    except Exception as e:
        load_log(runid, None, 'Abort-Exception', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 "jira_load_executions_by_project", None, None, str(e)[:200],
                 multiprocessing.current_process().pid, load_type,
                 "Multi", "")
        raise e
    last_status = models.get_final_log(runid)

    load_log(runid, None, 'Completed-new', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
             "jira_load_executions_by_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')

    return last_status


def jira_load_executions_for_all_projects_legacy(load, runid):
    if already_running('jira_load_executions_by_project'):
        print("RUN-ABORT")
        load_log(runid, None, 'RUN-ABORT', datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 'jira_load_executions_by_project', 0, 0, None, multiprocessing.current_process().pid, load, 'ALL')
        return {'data': [], 'error': 'This process is already running'}
    projects_no_data = models.get_all_projects_with_no_exec_data('jira')
    projects = models.get_all_projects_with_exec_data('jira')
    ret = {'data': []}
    load_log(runid, None, 'Starting-data', None, None,
             "jira_load_executions_by_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')
    for record in projects:
        project_id = record['project_id']
        last_updated_date = None
        if load == 'D':
            last_updated_date = models.jira_get_last_updated_execution_for_project(project_id)
            if last_updated_date:
                print_l("Project: " + project_id + ", Last Updated: " + last_updated_date)
            else:
                print_l("Project: " + project_id + ", Doesn't have last Updated.")
        last_status = jira_load_executions_by_project(project_id, runid, last_updated_date, load)
        ret['data'].append(last_status.copy())
    load_log(runid, None, 'Completed-data', None, None,
             "jira_load_executions_by_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')
    load_log(runid, None, 'Starting-No-data', None, None,
             "jira_load_executions_by_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')
    last_updated_date = None
    last_status = jira_load_executions_by_project_no_data(projects_no_data, runid, last_updated_date, load)
    ret['data'].append(last_status.copy())
    load_log(runid, None, 'Completed-No-data', None, None,
             "jira_load_executions_by_project", None, None, None,
             multiprocessing.current_process().pid, load, 'MULTI')

    ret['start_time'] = ret['data'][0]['start_time']
    ret['end_time'] = ret['data'][-1]['end_time']
    ret['total_processed'] = sum(item['records_processed'] if item['records_processed'] else 0 for item in ret['data'])
    return ret


def jira_load_all_projects(runid):
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
    }
    url = "https://jira-us-aholddelhaize.atlassian.net/rest/api/2/project"

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_log(runid, url, 'waiting-jira', start_time, None,
             inspect.stack()[0][3], None, None, None, multiprocessing.current_process().pid)
    print(url, headers)
    response = requests.request("GET", url, headers=headers, data={})
    records = []
    if is_json(response.text):
        response_json = json.loads(response.text)
        load_log(runid, url, 'processing', start_time, None,
                 inspect.stack()[0][3], len(response_json), 0, None, multiprocessing.current_process().pid)
        for project in response_json:
            record = {
                'project_id': project['key'],
                'source_id': project['id'],
                'title': project['name'].encode('ascii', 'ignore').decode('ascii'),
                'source': 'Jira'
            }
            records.append(record.copy())
        load_log(runid, url, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 inspect.stack()[0][3], len(response_json), len(response_json), None,
                 multiprocessing.current_process().pid)
        action = models.load_projects(records, runid)

    return records


def jira_user_refresh(runid):
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
    }
    url = "https://jira-us-aholddelhaize.atlassian.net/rest/api/3/user"

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    to_be_updated = models.get_users_to_be_updated()
    records = []
    for tbu_user in to_be_updated:
        if len(tbu_user['accountId']) == 0:
            continue
        load_log(runid, url, 'waiting-jira', start_time, None,
                 inspect.stack()[0][3], None, None, None, multiprocessing.current_process().pid)
        print("trying.. " + tbu_user['accountId'])
        response = requests.request("GET", url, headers=headers, params={'accountId': tbu_user['accountId']})
        if is_json(response.text):
            response_json = json.loads(response.text)
            if 'errorMessages' in response_json and len(response_json['errorMessages']):
                pass
            else:
                load_log(runid, url, 'processing', start_time, None,
                         inspect.stack()[0][3], len(response_json), 0, None, multiprocessing.current_process().pid)
                emailAddress = None
                if 'emailAddress' in response_json:
                    emailAddress = response_json['emailAddress']
                record = {
                    'emailAddress': emailAddress,
                    'displayName': response_json['displayName'],
                    'accountId': response_json['accountId'],
                }
                records.append(record.copy())
                action = models.load_users(records)
    load_log(runid, url, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
             inspect.stack()[0][3], len(response_json), len(response_json), None,
             multiprocessing.current_process().pid)

    return records


def jira_load_all_users(runid):
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
    }
    url = "https://jira-us-aholddelhaize.atlassian.net/rest/api/3/users/search"

    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    response = requests.request("GET", url, headers=headers, data={})
    records = []
    if is_json(response.text):
        response_json = json.loads(response.text)
        for user in response_json:
            record = {
                'accountID': user['accountId'],
                'displayName': user['displayName'].encode('ascii', 'ignore').decode('ascii'),
            }
            records.append(record.copy())
        action = models.load_users(records)

    return records


def jira_load_all_cycles(runid):
    relative_path = '/public/rest/api/1.0/cycles/search'
    method = 'GET'
    BASE_URL = 'https://prod-api.zephyr4jiracloud.com/connect'
    to_be_updated = models.get_cycles_from_execution()
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("To be updated : " + str(len(to_be_updated)))
    idx = 0
    for rec in to_be_updated:
        idx += 1
        print("Working on # " + str(idx) + " rec: " + str(rec))
        versionId = rec['versionId']
        projectId = rec['projectId']
        query = 'expand=&projectId=' + str(projectId) + '&versionId=' + str(versionId)
        json_param = {"expand": "", "versionId": str(versionId), "projectId": str(projectId)}
        headers = jira_zql_header(relative_path, method, query)
        response = requests.get(BASE_URL + relative_path + "?" + query, headers=headers, json=json_param)
        records = []
        if is_json(response.text):
            response_json = json.loads(response.text)
            load_log(runid, relative_path, 'processing', start_time, None,
                     inspect.stack()[0][3], len(response_json), 0, None, multiprocessing.current_process().pid)
            for cycle in response_json:
                startDate = None
                endDate = None
                if 'endDate' in cycle:
                    endDate = cycle['endDate']

                if 'startDate' in cycle:
                    startDate = cycle['startDate']

                record = {
                    'cycleId': cycle['cycleIndex'],
                    'startDate': startDate,
                    'endDate': endDate,
                    'cycleName': cycle['name'].encode('ascii', 'ignore').decode('ascii'),
                    'versionId': versionId,
                    'project_id': projectId
                }
                records.append(record.copy())
            load_log(runid, relative_path, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                     inspect.stack()[0][3], len(response_json), len(response_json), None,
                     multiprocessing.current_process().pid)
            action = models.load_cycles(records, runid)

    return records


def load_log(runid, load_url, load_status, start_time, end_time, method, records_expected, records_processed,
             params=None, pid=None, load_type=None, project_id=None, comments=None):
    try:
        if params:
            params = params[:200]
        models.load_log(runid, load_url, load_status, start_time, end_time, method, records_expected, records_processed,
                        params, pid, load_type, project_id, comments)
    except Exception as e:
        raise e
    return True


def jira_zql_header(RELATIVE_PATH, method, query=''):
    # USER
    ACCOUNT_ID = '60c4d4eec90cb200688734e'

    # ACCESS KEY from navigation >> Tests >> API Keys
    ACCESS_KEY = 'OTk4YWM2MzUtOWJkNC0zOTY1LTliYjYtZDIyMjVlNDg0ZjdiIDYwYzRkNGVlYzkwY2IyMDA2ODg3MzRlNSBVU0VSX0RFRkFVTFRfTkFNRQ'
    env = environ.Env()
    environ.Env.read_env()
    # ACCESS KEY from navigation >> Tests >> API Keys
    # SECRET_KEY = env('SECRET_KEY_API')
    SECRET_KEY = env.str('SECRET_KEY_API', 'sample_unsafe_secret')

    # JWT EXPIRE how long token been to be active? 3600 == 1 hour
    JWT_EXPIRE = 3600

    # BASE URL for Zephyr for Jira Cloud
    BASE_URL = 'https://prod-api.zephyr4jiracloud.com/connect'

    # RELATIVE PATH for token generation and make request to api
    RELATIVE_PATH = RELATIVE_PATH
    # '/public/rest/api/1.0/cycle'

    # CANONICAL PATH (Http Method & Relative Path & Query String)
    CANONICAL_PATH = method + '&' + RELATIVE_PATH + '&' + query
    # TOKEN HEADER: to generate jwt token
    payload_token = {
        'sub': ACCOUNT_ID,
        'qsh': hashlib.sha256(CANONICAL_PATH.encode('utf-8')).hexdigest(),
        'iss': ACCESS_KEY,
        'exp': int(time.time()) + JWT_EXPIRE,
        'iat': int(time.time())
    }

    # GENERATE TOKEN
    token = jwt.encode(payload_token, SECRET_KEY, algorithm='HS256')

    # REQUEST HEADER: to create cycle
    headers = {
        'Authorization': 'JWT ' + token,
        'Content-Type': 'application/json',
        'zapiAccessKey': ACCESS_KEY
    }

    return headers


def long_process(t=5, runid=121):
    print("child : " + str(multiprocessing.current_process().pid))
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_log(runid, None, 'processing', start_time, None, 'long_process', 0, 0, None,
             multiprocessing.current_process().pid)
    for i in range(int(t)):
        time.sleep(1)
    load_log(runid, None, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'long_process',
             0, 0, None, multiprocessing.current_process().pid)
    print("Exiting")


def kill_process(pid, runid):
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = ''
    try:
        load_log(runid, 'NA', 'processing', start_time, None, 'Kill', 0, 0, 'NA',
                 int(pid), None, None)
        if os.name == 'posix':
            os.kill(int(pid), signal.SIGKILL)
        else:
            os.kill(int(pid), -9)
    except OSError as error:
        print(error)
        message = str(error)
        load_log(runid, 'NA', 'OSError', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'Kill', 0, 0, 'NA',
                 int(pid), None, None)
    except psutil.NoSuchProcess as error:
        print(error)
        message = str(error)
        load_log(runid, 'NA', 'NoProcess', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'Kill',
                 0, 0, 'NA',
                 int(pid), None, None)
    except Exception as e:
        message = str(e)[:200]
        load_log(runid, 'NA', 'NoProcess', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'Kill',
                 0, 0, 'NA',
                 int(pid), None, None)

    load_log(runid, 'NA', 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'Kill',
             0, 0, 'NA', int(pid), None, None)
    return message


def kill_long_running_jobs():
    long_running_jobs = models.running_jobs()
    for job in long_running_jobs:
        print(job['runid'], job['project_id'], job['start_time'])
        print("Trying to set abort")
        models.set_force_abort(job['runid'], job['project_id'], job['start_time'])
        print("Now Trying to kill process")
        kill_process(job['pid'], job['runid'])
    return long_running_jobs


def print_l(message):
    try:
        print(message)
        models.log_message(message)
    except Exception as e:
        pass


def already_running(method):
    pid = models.get_last_pid(method)
    print(pid)
    if psutil.pid_exists(int(pid)):
        if psutil.Process(int(pid)).status() == "running":
            print('RUNNING')
            return True
        else:
            print('GO-AHEAD')
            return False
    else:
        print('NO-PROCESS')
        return False


def is_valid_method(method):
    methods = [
        'jira_get_tcs_by_project',
        'jira_get_tcs_by_project_delta',
        'jira_get_tcs_by_projects',
        'jira_get_tcs_by_projects_delta',
        'jira_get_last_updated_tc_for_project',
        'jira_zql_get_executions_by_project',
        'jira_load_tcs_for_project',
        'jira_load_tcs_for_project_no_data',
        'jira_load_tcs_for_all_projects',
        'jira_load_executions_by_project',
        'jira_load_executions_by_project_no_data',
        'jira_load_executions_for_all_projects',
        'jira_load_executions_for_all_projects_full',
        'jira_load_all_projects',
        'jira_load_defects_for_project',
        'jira_load_defects_for_project_no_data',
        'jira_load_defects_for_all_projects',
        'jira_load_all_cycles',
        'jira_load_all_users',
        'job_status',
        'show_daemons',
        'long_process',
        'kill_process',
        'kill_long_running_jobs',
        'running_jobs',
        'jira_user_refresh'
    ]
    return methods.count(method)


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def is_json(myjson):
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True
