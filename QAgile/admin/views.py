import pdb

from django.shortcuts import render
from django.http import JsonResponse, HttpResponse
from . import models
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect
from django.contrib.auth import get_user
from django.core.files.storage import FileSystemStorage
from openpyxl import load_workbook, drawing
from django.contrib.auth import authenticate
from django.contrib.auth.models import User
from django.db import DatabaseError, IntegrityError
from ..user.views import check_access
from operator import itemgetter


@login_required(login_url='/identity/')
def team(request):
    # need_login(request)
    check_access(request.user.username, 'resource')
    context_var = {
        'teamdata': models.get_all_person(),
        'domains': models.get_all_domain(),
        'rates': models.get_all_rates(),
        'roles': models.get_all_roles(),
        'locations': models.get_all_locations()
    }

    return render(request, 'team.html', context_var)


@login_required(login_url='/identity/')
def org_chart(request):
    # need_login(request)
    # check_access(request.user.username, 'resource')
    data = models.get_all_person()
    newlist = sorted(data, key=itemgetter('loc'), reverse=True)
    context_var = {
        'teamdata': newlist,
        'domains': models.get_all_domain(),
        'rates': models.get_all_rates(),
        'roles': models.get_all_roles(),
        'locations': models.get_all_locations()
    }

    return render(request, 'org_chart.html', context_var)


@login_required(login_url='/identity/')
def rfs(request):
    return render(request, 'rfs.html')


@login_required(login_url='/identity/')
def user_create(request):
    check_access(request.user.username, 'resource')
    ret = {'status': 'true', 'message': 'init', 'vnid': None}
    if request.method == 'POST' and 'vnid' in request.POST and len(request.POST['vnid']):
        try:
            first_name = None
            last_name = None
            email = None
            person = models.get_person_by_id(request.POST['vnid'])
            if len(person):
                first_name = person[0]['first_name']
                last_name = person[0]['last_name']
                email = person[0]['email']
            user = User.objects.create_user(request.POST['vnid'].lower(), email, request.POST['vnid'].lower(), first_name=first_name, last_name=last_name)
            user = authenticate(request, username=request.POST['vnid'], password=request.POST['vnid'])
            if user is not None:
                ret = {'status': 'true', 'message': 'Account Created!', 'vnid': request.POST['vnid']}
            else:
                ret = {'status': 'false', 'message': 'Error Creating Account', 'vnid': request.POST['vnid']}
        except IntegrityError:
            ret = {'status': 'false', 'message': 'Account Already Exists!', 'vnid': request.POST['vnid']}
        except DatabaseError:
            ret = {'status': 'false', 'message': 'Something Wrong!', 'vnid': request.POST['vnid']}
    else:
        ret = {'status': 'false', 'message': 'Invalid Request!', 'vnid': request.POST['vnid']}

    return JsonResponse(ret, safe=False)


@login_required(login_url='/identity/')
def user_reset(request):
    check_access(request.user.username, 'resource')
    ret = {'status': 'true', 'message': 'init', 'vnid': None}
    if request.method == 'POST' and 'vnid' in request.POST and len(request.POST['vnid']):
        try:
            u = User.objects.get(username=request.POST['vnid'])
            u.set_password(request.POST['vnid'].lower())
            u.save()
            ret = {'status': 'true', 'message': 'success', 'vnid': request.POST['vnid']}
        except IntegrityError:
            ret = {'status': 'false', 'message': 'Account Already Exists!', 'vnid': request.POST['vnid']}
        except DatabaseError:
            ret = {'status': 'false', 'message': 'Something Wrong!', 'vnid': request.POST['vnid']}
    else:
        ret = {'status': 'false', 'message': 'Invalid Request!', 'vnid': request.POST['vnid']}

    return JsonResponse(ret, safe=False)


@login_required(login_url='/identity/')
def editperson(request):
    check_access(request.user.username, 'resource')
    vnid=''

    if request.method == "GET":
        if 'vnid' in request.GET:
            vnid = request.GET['vnid']

    if request.method == "POST":
        if 'vnid' in request.POST:
            if 'del_button' in request.POST and 'enddate' in request.POST:
                vnid = models.delete_person(request.POST['vnid'], request.POST['enddate'])
                return redirect('/manage/team/')
            else:
                vnid = models.update_person(request.POST)

    persondata = models.get_person_by_id(vnid)

    if not persondata:
        persondata = [{'vnid': '', 'first_name': '', '': '', 'email': '', 'domain_id': '', 'category': '',
                       'rate_id': '', 'phone': '', 'created': None, 'role_id': '', 'location_id': '', 'city': '',
                       'rate': '', 'role': '', 'domain_name': ''}]
    print(persondata)
    context_var = {
        'persondata': persondata,
        'domains': models.get_all_domain(),
        'rates': models.get_all_rates(),
        'roles': models.get_all_roles(),
        'locations': models.get_all_locations()
    }

    return render(request, 'editperson.html', context_var)


@login_required(login_url='/identity/')
def editdomains(request):
    check_access(request.user.username, 'resource')
    domain_id = 'Null'

    if request.method == "GET":
        if 'domain_id' in request.GET:
            domain_id = request.GET['domain_id']

    if request.method == "POST":
        if 'domain_id' in request.POST:
            if 'del_button' in request.POST:
                domain_id = models.delete_object(
                    'domain_id', request.POST['domain_id'], get_user(request).username, 'qagile_domains')
                return redirect('/manage/team/')
            else:
                domain_id = models.update_domain(request.POST)


    domaindata = models.get_domain_by_id(domain_id)

    if not domaindata:
        domaindata = [{'domain_id': None, 'domain_name': '', 'domain_lead': '', 'domain_lead_two': '',
                       'pom': '', 'dl_name': '', 'pom_name': '', 'dl_name_2': ''}]

    context_var = {
        'domaindata': domaindata,
        'dldata': models.get_all_domain_leads(),
        'pomdata': models.get_all_portfolio_managers()
    }

    print(context_var)

    return render(request, 'editdomain.html', context_var)


@login_required(login_url='/identity/')
def editrole(request):
    check_access(request.user.username, 'resource')
    role_id = 'Null'

    if request.method == "GET":
        if 'role_id' in request.GET:
            role_id = request.GET['role_id']

    if request.method == "POST":
        if 'role_id' in request.POST:
            if 'del_button' in request.POST:
                role_id = models.delete_object(
                    'role_id', request.POST['role_id'], get_user(request).username, 'qagile_person_role')
                return redirect('/manage/team/')
            else:
                role_id = models.update_role(request.POST)

    roledata = models.get_role_by_id(role_id)
    print("*****ROOOLLLLEEEEE******")
    print(roledata)

    if not roledata:
        roledata = [{'role_id': None, 'role': ''}]

    context_var = {
        'roledata': roledata
    }

    print(context_var)

    return render(request, 'editrole.html', context_var)


@login_required(login_url='/identity/')
def editlocation(request):
    check_access(request.user.username, 'resource')
    location_id = 'Null'

    if request.method == "GET":
        if 'location_id' in request.GET:
            location_id = request.GET['location_id']

    if request.method == "POST":
        if 'location_id' in request.POST:
            if 'del_button' in request.POST:
                location_id = models.delete_object(
                    'location_id', request.POST['location_id'], get_user(request).username, 'qagile_person_location')
                return redirect('/manage/team/')
            else:
                location_id = models.update_location(request.POST)

    locationdata = models.get_location_by_id(location_id)
    print("*****LOCCCCCC******")
    print(locationdata)

    if not locationdata:
        locationdata = [{'location_id': None, 'city': '', 'state': None, 'zip': None, 'country': '',
                         'loc_code': ''}]

    context_var = {
        'locationdata': locationdata
    }

    print(context_var)

    return render(request, 'editlocation.html', context_var)


@login_required(login_url='/identity/')
def editrate(request):
    check_access(request.user.username, 'resource')
    rate_id = 'Null'

    if request.method == "GET":
        if 'rate_id' in request.GET:
            rate_id = request.GET['rate_id']

    if request.method == "POST":
        if 'rate_id' in request.POST:
            if 'del_button' in request.POST:
                rate_id = models.delete_object(
                    'rate_id', request.POST['rate_id'], get_user(request).username, 'qagile_person_rate')
                return redirect('/manage/team/')
            else:
                rate_id = models.update_rate(request.POST)

    ratedata = models.get_rate_by_id(rate_id)
    print("*****RATEEEE******")
    print(ratedata)

    if not ratedata:
        ratedata = [{'rate_id': None, 'rate': None, 'category': ''}]


    context_var = {
        'ratedata': ratedata
    }

    print(context_var)

    return render(request, 'editrate.html', context_var)


@login_required(login_url='/identity/')
def admin(request):
    check_access(request.user.username, 'resource')
    return render(request, 'admin.html')


@login_required(login_url='/identity/')
def persons2(request):
    data = {"results": {
        "question": "This is a question",
        "created_by": "Tushar",
        "pub_date": "1-1-1"
    }}
    return JsonResponse(data)


@login_required(login_url='/identity/')
def ldif(request):
    data = models.get_all_person()
    return render(request, 'ldif.html', {'data': data})


@login_required(login_url='/identity/')
def upload(request):
    data = {}
    if request.method == 'POST' and 'myfile' in request.FILES and request.FILES['myfile']:
        print("In FILE Upload")
        myfile = request.FILES['myfile']
        xls = load_workbook(myfile)
        onshore = xls.worksheets[0]
        offshore = xls.worksheets[1]
        records = []
        for idx in range(2, len(onshore['A'])+1):
            record = {
                'name': onshore['B' + str(idx)].value,
                'email_ibm': onshore['C' + str(idx)].value,
                'email_rbs': onshore['D' + str(idx)].value,
                'location': onshore['E' + str(idx)].value,
                'type': onshore['F' + str(idx)].value,
                'billrate': onshore['G' + str(idx)].value,
                'phone': onshore['H' + str(idx)].value,
                'domain': onshore['I' + str(idx)].value,
                'ahid': onshore['J' + str(idx)].value,
                'vnid': onshore['K' + str(idx)].value
            }
            records.append(record.copy())

        for idx in range(2, len(offshore['A'])+1):
            record = {
                'name': offshore['B' + str(idx)].value,
                'email_ibm': offshore['C' + str(idx)].value,
                'email_rbs': offshore['D' + str(idx)].value,
                'location': offshore['E' + str(idx)].value,
                'type': offshore['F' + str(idx)].value,
                'billrate': offshore['G' + str(idx)].value,
                'phone': offshore['H' + str(idx)].value,
                'domain': offshore['I' + str(idx)].value,
                'ahid': offshore['J' + str(idx)].value,
                'vnid': offshore['K' + str(idx)].value
            }
            records.append(record.copy())
        models.load_raw_qmo(records)
    return render(request, 'upload.html', {'data': data})
