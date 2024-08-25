import pdb

from django.shortcuts import render, redirect
from . import models
from ..admin.models import get_all_domain, get_person_by_id
# Create your views here.


def present_in_form(fields, req):
    ret = True
    for field in fields:
        if field not in req:
            ret = False
            break
    return ret


def has_len(fields, req):
    ret = True
    for field in fields:
        if field not in req:
            ret = False
        elif not len(req[field]):
            ret = False
    return ret


def my_projects(request):
    return render(request, "my_projects.html")


def create(request):
    errors = []
    domains = get_all_domain()
    person = get_person_by_id(request.user.username)
    if request.method == 'POST':
        r = request.POST
        fields = ['domain_id', 'ppmid', 'parent_ppmid', 'source', 'source_id', 'project_name']
        if present_in_form(fields, r) and has_len(['domain_id', 'ppmid', 'project_name'], r):
            ppmid = models.create(r)
            redirect("/project/?ppmid=" + ppmid + '&action=create&ppmid=' + r['ppmid'])
        else:
            errors.append('Missing Fields/Values')
    return render(request, "create_project.html", {'errors': errors, 'domains': domains, 'person': person})
