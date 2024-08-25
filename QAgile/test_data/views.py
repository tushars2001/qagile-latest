import hashlib
import pdb, json
import time
import inspect
import threading


import jwt
from django.shortcuts import render
import requests
from django.http import JsonResponse
from . import models
from datetime import datetime


# Create your views here.
def is_valid_method(method):
    methods = [
        'jira_get_tcs_by_project',
        'jira_get_tcs_by_project_delta',
        'jira_get_tcs_by_projects',
        'jira_get_tcs_by_projects_delta',
        'jira_load_tcs_for_project',
        'jira_get_last_updated_tc_for_project',
        'jira_load_tcs_for_all_projects',
        'jira_zql_get_executions_by_project',
        'jira_load_executions_by_project',
        'jira_load_all_projects',
        'jira_load_defects_for_project'
    ]
    return methods.count(method)


def jira_test_cases(request):
    out_data = {}
    if request.method == 'GET':
        if 'method' in request.GET and is_valid_method(request.GET['method']):

            if request.GET['method'] == 'jira_get_tcs_by_project' and 'project_id' in request.GET \
                    and len(request.GET['project_id']):
                data = jira_get_tcs_by_project(request.GET['project_id'])
                if is_json(data):
                    data_json = json.loads(data)
                    return JsonResponse(data_json)

            elif request.GET['method'] == 'jira_load_tcs_for_project' and 'project_id' in request.GET and \
                    len(request.GET['project_id']) and 'load' in request.GET and len(request.GET['load']) \
                    and ['F', 'D'].count(request.GET['load']):
                if request.GET['load'] == 'F':
                    data = jira_load_tcs_for_project(request.GET['project_id'])
                    return JsonResponse(data)
                elif request.GET['load'] == 'D':
                    last_updated_date = models.jira_get_last_updated_tc_for_project(request.GET['project_id'])
                    data = jira_load_tcs_for_project(request.GET['project_id'], last_updated_date)
                    return JsonResponse(data)
                else:
                    out_data['message'] = "Invalid value of param load"

            elif request.GET['method'] == 'jira_load_defects_for_project' and 'project_id' in request.GET and \
                    len(request.GET['project_id']) and 'load' in request.GET and len(request.GET['load']) \
                    and ['F', 'D'].count(request.GET['load']):
                if request.GET['load'] == 'F':
                    data = jira_load_defects_for_project(request.GET['project_id'])
                    return JsonResponse(data)
                elif request.GET['load'] == 'D':
                    last_updated_date = models.jira_get_last_updated_defect_for_project(request.GET['project_id'])
                    data = jira_load_defects_for_project(request.GET['project_id'], last_updated_date)
                    return JsonResponse(data)
                else:
                    out_data['message'] = "Invalid value of param load"

            elif request.GET['method'] == 'jira_load_tcs_for_all_projects' \
                    and 'load' in request.GET and len(request.GET['load']) and ['F', 'D'].count(request.GET['load']):
                data = jira_load_tcs_for_all_projects(request.GET['load'])
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_load_executions_by_project' and 'project_id' in request.GET and \
                    len(request.GET['project_id']):
                project_name = models.get_project_name_by_id(request.GET['project_id'])
                if project_name:
                    data = jira_load_executions_by_project(project_name)
                else:
                    data = {'error': 'Project not found'}
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_load_all_projects':
                data = jira_load_all_projects()
                return JsonResponse(data, safe=False)

            elif request.GET['method'] == 'jira_get_last_updated_tc_for_project' and 'project_id' in request.GET and \
                    len(request.GET['project_id']):
                last_updated_date = models.jira_get_last_updated_tc_for_project(request.GET['project_id'])
                return JsonResponse({'last_updated_date': last_updated_date})
            else:
                out_data['message'] = "Missing/invalid params"
        else:
            out_data['message'] = "Invalid Method"
    else:
        out_data['message'] = "Only GET is supported"
    return JsonResponse(out_data)


def jira_load_tcs_for_project(project_id, last_updated=None):
    response = {}
    api = "https://jira-us-aholddelhaize.atlassian.net/rest/api/3/search"
    jql = 'project in (' + project_id + ') and type in (Test)'
    fields = "id, key, assignee, reporter, updated, summary, priority, creator, created"

    if last_updated:
        jql = jql + " and updated >= '" + last_updated + "'"

    url = api + '?jql=' + jql + '&fields=id'
    payload = {}
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
    }
    print(url)
    response = requests.request("GET", url, headers=headers, data=payload)
    if is_json(response.text):
        response_json = json.loads(response.text)
        import math
        pages = math.ceil(response_json['total'] / 50)
        runid = datetime.now().strftime("%Y%m%d%H%M%S")
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        startAt = 0
        for page in range(pages):
            records = []
            startAt = page * 50
            url = api + '?jql=' + jql + '&fields=' + fields + '&startAt=' + str(startAt)
            print(url)
            load_log(runid, url, 'processing', start_time, None,
                     inspect.stack()[0][3], response_json['total'], startAt)
            response = requests.request("GET", url, headers=headers, data=payload)
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
                    record = {
                        'tcid': issue['key'],
                        'source_id': issue['id'],
                        'title': issue['fields']['summary'],
                        'created': issue['fields']['created'].split('.')[0],
                        'updated': issue['fields']['updated'].split('.')[0],
                        'created_by': issue['fields']['reporter']['displayName'],
                        'source': 'Jira',
                        'project_id': project_id
                    }
                    records.append(record.copy())
                action = models.load_tc_records(records, runid)
            else:
                response_json = None
        load_log(runid, url, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 inspect.stack()[0][3], response_json['total'], response_json['total'])
        models.compress_load_log()
        last_status = models.get_final_log(runid)
        return last_status
    else:
        raise Exception("Response is not JSON. Something not right :-(")


def jira_load_tcs_for_all_projects(load):
    projects = models.get_all_projects('jira')
    ret = {'data': []}
    for record in projects:
        project_id = record['project_id']
        last_updated_date = None
        if load == 'D':
            last_updated_date = models.jira_get_last_updated_tc_for_project(project_id)
        print("loading data for: " + project_id)
        last_status = jira_load_tcs_for_project(project_id, last_updated_date)
        ret['data'].append(last_status.copy())

    ret['start_time'] = ret['data'][0]['start_time']
    ret['end_time'] = ret['data'][-1]['end_time']
    ret['total_processed'] = sum(item['records_processed'] for item in ret['data'])
    return ret


def is_json(myjson):
    try:
        json.loads(myjson)
    except ValueError as e:
        return False
    return True


def load_log(runid, load_url, load_status, start_time, end_time, method, records_expected, records_processed):
    models.load_log(runid, load_url, load_status, start_time, end_time, method, records_expected, records_processed)
    return True


def jira_load_executions_by_project(project_id):
    relative_path = '/public/rest/api/1.0/zql/search'
    method = 'POST'
    json_param = {"zqlQuery": 'project = "' + project_id + '"', "offset": 0, "maxRecords": 1}

    # MAKE REQUEST:
    BASE_URL = 'https://prod-api.zephyr4jiracloud.com/connect'
    headers = jira_zql_header(relative_path, 'POST')
    response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
    print(BASE_URL + relative_path, headers, json_param)
    json_result = {
        'type': None,
        'data': None
    }
    if is_json(response.text):
        response_json = json.loads(response.text)
        import math
        pages = math.ceil(response_json['totalCount'] / 50)
        runid = datetime.now().strftime("%Y%m%d%H%M%S")
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        startAt = 0
        for page in range(pages):
            records = []
            startAt = page * 50
            json_param = {"zqlQuery": 'project = "' + project_id + '"', "offset": startAt, "maxRecords": 50}
            load_log(runid, BASE_URL + relative_path, 'processing', start_time, None,
                     inspect.stack()[0][3], response_json['totalCount'], startAt)
            response = requests.post(BASE_URL + relative_path, headers=headers, json=json_param)
            if is_json(response.text):
                response_json = json.loads(response.text)
                record = {
                    'tcid': '',
                    'source_id': '',
                    'executedBy': '',
                    'executedOn': '',
                    'status': '',
                    'cycleName': '',
                    'source': 'Jira'
                }
                for issue in response_json['searchObjectList']:
                    print(issue)
                    if 'executedBy' in issue['execution']:
                        executedBy = issue['execution']['executedBy']
                    else:
                        executedBy = None

                    if 'executedOn' in issue['execution']:
                        executedOn = issue['execution']['executedOn']
                    else:
                        executedOn = None

                    record = {
                        'tcid': issue['issueKey'],
                        'source_id': issue['execution']['id'],
                        'executedBy': executedBy,
                        'executedOn': executedOn,
                        'status': issue['execution']['status']['name'],
                        'cycleName':  issue['execution']['cycleName'],
                        'source': 'Jira'
                    }
                    records.append(record.copy())
                action = models.load_tc_executions_records(records, runid)
            else:
                response_json = None
        load_log(runid, BASE_URL + relative_path, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 inspect.stack()[0][3], response_json['totalCount'], response_json['totalCount'])
        models.compress_load_log()
        last_status = models.get_final_log(runid)
        return last_status
    else:
        return {'error': 'response is not json'}


def jira_zql_header(RELATIVE_PATH, method, query=''):
    # USER
    ACCOUNT_ID = '60c4d4eec90cb200688734e'

    # ACCESS KEY from navigation >> Tests >> API Keys
    ACCESS_KEY = 'OTk4YWM2MzUtOWJkNC0zOTY1LTliYjYtZDIyMjVlNDg0ZjdiIDYwYzRkNGVlYzkwY2IyMDA2ODg3MzRlNSBVU0VSX0RFRkFVTFRfTkFNRQ'

    # ACCESS KEY from navigation >> Tests >> API Keys
    SECRET_KEY = 'aYa2u8aS-NmKja0H5mhHJguRMHl_tuCbdzJu0NPOcR0'

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


def jira_load_all_projects():
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
    }
    url = "https://jira-us-aholddelhaize.atlassian.net/rest/api/2/project"
    print(url)
    runid = datetime.now().strftime("%Y%m%d%H%M%S")
    start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    load_log(runid, url, 'starting', start_time, None,
             inspect.stack()[0][3], None, None)
    response = requests.request("GET", url, headers=headers, data={})
    records = []
    if is_json(response.text):
        response_json = json.loads(response.text)
        for project in response_json:
            record = {
                'project_id': project['key'],
                'source_id': project['id'],
                'title': project['name'],
                'source': 'Jira'
            }
            records.append(record.copy())
        load_log(runid, url, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 inspect.stack()[0][3], len(response_json), len(response_json))
        action = models.load_projects(records, runid)

    return records


def jira_load_defects_for_project(project_id, last_updated=None):
    response = {}
    api = "https://jira-us-aholddelhaize.atlassian.net/rest/api/3/search"
    jql = 'project in (' + project_id + ') and type in (Bug)'
    fields = "id, key, assignee, reporter, updated, summary, priority, creator, created, status, environment, assignee"

    if last_updated:
        jql = jql + " and updated >= '" + last_updated + "'"

    url = api + '?jql=' + jql + '&fields=id'
    payload = {}
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Basic dHVzaGFyLnNheGVuYUBkZWxoYWl6ZS5jb206WU5FYzBnQzRCUzRHN2Jyam9tNE42Njc4'
    }
    print(url)
    response = requests.request("GET", url, headers=headers, data=payload)
    if is_json(response.text):
        response_json = json.loads(response.text)
        import math
        pages = math.ceil(response_json['total'] / 50)
        runid = datetime.now().strftime("%Y%m%d%H%M%S")
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        startAt = 0
        for page in range(pages):
            records = []
            startAt = page * 50
            url = api + '?jql=' + jql + '&fields=' + fields + '&startAt=' + str(startAt)
            print(url)
            load_log(runid, url, 'processing', start_time, None,
                     inspect.stack()[0][3], response_json['total'], startAt)
            response = requests.request("GET", url, headers=headers, data=payload)
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
                        'title': issue['fields']['summary'],
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
                response_json = None
        load_log(runid, url, 'finished', start_time, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                 inspect.stack()[0][3], response_json['total'], response_json['total'])
        models.compress_load_log()
        last_status = models.get_final_log(runid)
        return last_status
    else:
        raise Exception("Response is not JSON. Something not right :-(")
