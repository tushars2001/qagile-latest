import pdb

from django.shortcuts import render
import datetime
import time
from django.contrib.auth.decorators import login_required
from . import models
from django.shortcuts import redirect
from django.http import JsonResponse
from django.core.exceptions import PermissionDenied
from ..identity.views import change_password as cp
from django.contrib.auth import authenticate, login
from ..rfs.models import get_rfsdata_pom_approval, get_rfsdata_for_domains
from ..admin.models import get_person_by_id, get_all_portfolio_managers
from ..analytics.models import get_resource_data, get_chart_data


@login_required(login_url='/identity/')
def profile(request):
    context = {}
    numofweeks = 16
    vnid = request.user.username

    if request.method == 'GET':
        if 'numofweeks' in request.GET and len(request.GET['numofweeks']):
            numofweeks = request.GET['numofweeks']
        if 'message' in request.GET:
            context['message'] = request.GET['message']

    # to generate capacity row
    datetime_object = datetime.datetime.now()
    if datetime_object.weekday() < 6:
        start = datetime_object - datetime.timedelta(days=datetime_object.weekday() + 1)
    else:
        start = datetime_object
    start_str = start.strftime("%Y-%m-%d")
    context['weeks'] = [start_str]
    for i in range(1, int(numofweeks)):
        start = start + datetime.timedelta(days=7)
        start_str = start.strftime("%Y-%m-%d")
        context['weeks'].append(start_str)

    context['my_capacity'] = \
        models.get_capacity(request.user.username, week_start=context['weeks'][0], week_end=context['weeks'][-1])

    # to show user specific RFSs
    context['userdata'] = get_person_by_id(request.user.username)
    print(context['userdata'])
    if context['userdata'][0]['role'] == 'Portfolio Manager':
        context['rfsdata'] = get_rfsdata_pom_approval(request.user.username)
    else:
        context['rfsdata'] = get_rfsdata_for_domains(context['userdata'][0]['domain_id'])

    # to generate last 6 weeks planned vs actuals
    today = datetime.date.today()
    end_date = today + datetime.timedelta(weeks=-6)
    to_date = int(time.mktime(datetime.datetime(int(today.strftime("%Y")), int(today.strftime("%m")),
                                                int(today.strftime("%d")), 0, 0).timetuple())*1000)
    from_dt = int(time.mktime(datetime.datetime(int(end_date.strftime("%Y")), int(end_date.strftime("%m")),
                                                int(end_date.strftime("%d")), 0, 0).timetuple())*1000)
    dt_range = str(from_dt) + ';' + str(to_date)
    filters = {'vnids': [request.user.username], 'dt_range': [dt_range], 'domains': None, 'ppmids': None, 'plan': None}
    context['chartdata'] = get_chart_data(filters)

    # to generate this week's hours
    today = datetime.date.today()
    end_date = today + datetime.timedelta(weeks=1)
    from_dt = int(time.mktime(datetime.datetime(int(today.strftime("%Y")), int(today.strftime("%m")),
                                                int(today.strftime("%d")), 0, 0).timetuple()) * 1000)
    to_date = int(time.mktime(datetime.datetime(int(end_date.strftime("%Y")), int(end_date.strftime("%m")),
                                                int(end_date.strftime("%d")), 0, 0).timetuple()) * 1000)
    dt_range = str(from_dt) + ';' + str(to_date)
    filters = {'vnids': [request.user.username], 'dt_range': [dt_range], 'plan': 'True'}
    context['this_week_data'] = get_resource_data(filters)

    # get all poms
    context['poms'] = get_all_portfolio_managers()
    context['delegated'] = models.get_delegated(request.user.username)

    return render(request, "profile.html", context)


def check_access(username, access):
    if models.has_access(username, access):
        pass
    else:
        raise PermissionDenied("You are not authorized!")


def get_accesses(username):
    return models.get_accesses(username)


@login_required(login_url='/identity/')
def change_password(request):
    if request.method == 'POST' and 'password' in request.POST and len(request.POST['password']) >= 5:
        cp(request.user.username, request.POST['password'])
        user = authenticate(username=request.user.username, password=request.POST['password'])
        login(request, user)
        return redirect("/profile/?message=Password Changed!")
    else:
        return redirect("/profile/?message=Invalid Request")


@login_required(login_url='/identity/')
def change_email(request):
    if request.method == 'POST' and 'email' in request.POST and len(request.POST['email']) >= 5:
        models.update_user_field(request.user.username, 'email', request.POST['email'])
        return redirect("/profile/?message=Email Changed!")
    else:
        return redirect("/profile/?message=Invalid Request")


@login_required(login_url='/identity/')
def change_phone(request):
    if request.method == 'POST' and 'phone' in request.POST and len(request.POST['phone']) >= 10:
        models.update_user_field(request.user.username, 'phone', request.POST['phone'])
        return redirect("/profile/?message=Phone Changed!")
    else:
        return redirect("/profile/?message=Invalid Request")


@login_required(login_url='/identity/')
def capacity(request):
    message = "Capacity Updated"
    if request.method == 'POST':
        models.update_capacity(request.user.username, request.POST)
    else:
        message = "Invalid Request!"
    return redirect("/profile/?message=" + message)


@login_required(login_url='/identity/')
def delegation(request):
    message = "Delegation Successful"
    if request.method == 'POST' and 'vnid_delegate' in request.POST and 'enddate' in request.POST:
        models.set_delegate(request.user.username, request.POST['vnid_delegate'], request.POST['enddate'])

    return redirect("/profile/?message=" + message)


@login_required(login_url='/identity/')
def update_field(request):
    ret = {'status': 'failed', 'message': 'Invalid Request'}

    if request.method == 'POST' and 'field' in request.POST:
        vnid = models.update_user_field(request.POST['vnid'], request.POST['field'], request.POST['value'])
        ret = {'status': 'success', 'message': 'Updated', 'vnid': vnid}
    return JsonResponse(ret, safe=False)
