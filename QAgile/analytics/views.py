import os
import pdb

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.core.files.storage import FileSystemStorage
from django.shortcuts import render
from . import models
import xmltodict
import xml.dom.minidom
from ..admin.models import get_all_person
from django.http import JsonResponse
from ..test_data_v2.models import get_runids_from_pids
import psutil
import xml.etree.ElementTree as ET
# Create your views here.


@login_required(login_url='/identity/')
def analytics_home(request):

    if request.method == 'POST' and 'myfile' in request.FILES and request.FILES['myfile']:
        print("In FILE Upload")
        myfile = request.FILES['myfile']
        fs = FileSystemStorage()
        try:
            os.remove(os.path.join(settings.MEDIA_ROOT, '50197.xml'))
        except FileNotFoundError:
            print("No file")

        filename = fs.save('50197.xml', myfile)
        uploaded_file_url = fs.url(filename)
        return render(request, "analytics_home.html", {
            'uploaded_file_url': uploaded_file_url
        })

    if request.method == "POST" and 'process' in request.POST:
        print("In FILE Process")
        processed = load_tenrox()
        return render(request, "analytics_home.html", {
            'processed': processed
        })

    return render(request, "analytics_home.html")


def load_tenrox():
    xmlObject = \
        xml.dom.minidom.parse(os.path.join(settings.MEDIA_ROOT, '50197.xml'))
    tenrox_xml = xmlObject.toprettyxml()
    tenrox_dict = xmltodict.parse(tenrox_xml)
    print(os.path.join(settings.MEDIA_ROOT, '50197.xml'))
    print(xmlObject)
    processed = models.load_tenrox_data(tenrox_dict)

    return processed


def load_jira(request):
    for i in range(1,11):
        xmlObject = \
            xml.dom.minidom.parse(os.path.join(settings.MEDIA_ROOT, 'executions/' + str(i) + '.xml'))
        jira_xml = xmlObject.toprettyxml()
        jira_dict = xmltodict.parse(jira_xml)

        processed = models.load_jira_data(jira_dict)
        pdb.set_trace()
    return processed


def planned_actuals(request):
    chartdata = {}
    from ..admin.models import get_all_person
    filters = {'org': None, 'domains': '', 'ppmids': '', 'vnids': ''}

    if 'org_select' in request.GET:
        filters['org'] = request.GET['org_select']

    if 'domain_ids' in request.GET:
        filters['domains'] = request.GET.getlist('domain_ids')

    if 'ppm_ids' in request.GET:
        filters['ppmids'] = request.GET.getlist('ppm_ids')

    if 'vnids' in request.GET:
        filters['vnids'] = request.GET.getlist('vnids')

    if 'dt_range' in request.GET:
        filters['dt_range'] = request.GET.getlist('dt_range')

    if 'dt_range' in request.GET:
        print("**Getting Chart Data****")
        chartdata['org_chart'] = models.get_org_chart_data(filters)
        chartdata['domains_chart'] = models.get_domains_chart_data(filters)
        chartdata['projects_chart'] = models.get_projects_chart_data(filters)
        chartdata['resources_chart'] = models.get_resources_chart_data(filters)

    persons_all = get_all_person()

    persons = [
        {k: v for k, v in d.items() if
         k in ('vnid', 'first_name', 'last_name', 'domain_name', 'role', 'loc', 'domain_id')}
        for d in persons_all]

    if 'dt_range' in filters:
        filters['dt_range'] = filters['dt_range'][0].split(";")

    domains_info = models.get_domains_info()
    ppm_info = models.get_project_info()
    domain_person = models.get_domain_person()
    project_person = models.get_project_person()

    context_var = {
        'domains_info': domains_info,
        'ppm_info': ppm_info,
        'persons': persons,
        'chartdata': chartdata,
        'domain_person': domain_person,
        'project_person': project_person,
        'filters': filters
    }

    return render(request, "planned_actuals.html", context_var)


def resource_plan(request):
    chartdata = {}

    filters = {'plan': 'on', 'domains': None, 'vnids': None}

    if 'domain_ids' in request.GET:
        filters['domains'] = request.GET.getlist('domain_ids')

    if 'vnids' in request.GET:
        filters['vnids'] = request.GET.getlist('vnids')

    if 'dt_range' in request.GET:
        if 'plan_select' not in request.GET:
            filters['plan'] = None
        filters['dt_range'] = request.GET.getlist('dt_range')

    if 'dt_range' in request.GET:
        chartdata = models.get_resource_data(filters)

    static_tbl = models.get_statictbl(filters)
    person_domain = models.get_domain_person()
    datahead = models.get_datahead(filters)[0]

    if 'dt_range' in filters:
        filters['dt_range'] = filters['dt_range'][0].split(";")

    persons_all = get_all_person()
    persons = [
        {k: v for k, v in d.items() if k in ('vnid', 'first_name', 'last_name', 'rate', 'domain_name', 'role', 'loc', 'domain_id')}
        for d in persons_all]

    context_var = {
        'domains_info': models.get_domains_info(),
        'persons': persons,
        'chartdata': chartdata,
        'static_tbl': static_tbl,
        'person_domain': person_domain,
        'datahead': datahead,
        'filters': filters
    }

    return render(request, "resource_plan.html", context_var)


@login_required(login_url='/identity/')
def capacity(request):
    return render(request, "capacity.html")


@login_required(login_url='/identity/')
def jira_home(request):
    context = {}
    duration = None
    if request.method == 'GET' and 'duration' in request.GET and len(request.GET['duration']):
        duration = request.GET['duration']
    context['jira_jobs'] = models.get_jobs_summary(duration)
    context['filter'] = None
    if request.method == 'GET' and 'filter' in request.GET and len(request.GET['filter']):
        context['filter'] = 'running_jobs'
        pids = psutil.pids()
        pids_running = []
        for pid in range(len(pids)):
            if psutil.pid_exists(pids[pid]) and psutil.Process(pids[pid]).status() == 'running':
                pids_running.append(pids[pid])
        runids = get_runids_from_pids(pids_running)
        data = []
        for rec in runids:
            data.append(rec['runid'])
        context['jira_jobs'] = [d for d in context['jira_jobs'] if d['runid'] in data]

    return render(request, "jira_home.html", context)


@login_required(login_url='/identity/')
def get_job_details(request):
    data = None
    if request.method == 'POST' and 'runid' in request.POST and len(request.POST['runid']):
        data = models.get_job_details(request.POST['runid'])

    return JsonResponse(data, safe=False)


@login_required(login_url='/identity/')
def projects(request):
    context = {}
    context['projects'] = models.get_all_projects()
    return render(request, "projects.html", context)


@login_required(login_url='/identity/')
def tracking(request):
    if request.method == 'POST' and 'project_id' in request.POST and 'tracking' in request.POST:
        if request.POST['tracking'] == 'true':
            track = 1
        else:
            track = 0
        models.update_tracking(request.POST['project_id'], track)
        return JsonResponse({'success': True, 'error': None})
    else:
        return JsonResponse({'success': False, 'error': 'invalid request'})


def filterTheDict(dictObj, callback):
    pdb.set_trace()
    newDict = dict()
    # Iterate over all the items in dictionary
    for (key, value) in dictObj.items():
        # Check if item satisfies the given condition then add to new dict
        if callback((key, value)):
            newDict[key] = value
    return newDict
