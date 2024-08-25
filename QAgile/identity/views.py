import json
import pdb

from django.shortcuts import render
from . import models
from django.shortcuts import redirect
from django.contrib.auth import authenticate, login, logout
from ..admin.models import get_all_domain
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.contrib.auth.models import User
from django.db import DatabaseError, IntegrityError

# Create your views here.


def login_page(request):
    return render(request, 'login.html')


def logout_user(request):
    logout(request)
    return redirect("/")


def check_login(request):
    username = request.POST['user']
    password = request.POST['password']
    user = authenticate(username=username, password=password)
    if user is None:
        ret = "/identity/?error=Authentication Failed!"
        if 'next' in request.POST:
            ret = ret + "&next=" + request.POST.get('next')
        return redirect(ret)
    else:
        login(request, user)

    if 'next' in request.POST:
        return redirect(request.POST.get('next'))
    else:
        return redirect("/")


def check_sql(request):
    data = get_all_domain()

    return render(request, "sql_check.html", {'data': data})


@login_required(login_url='/identity/')
def profile(request):
    profile_data = models.get_profile(request.username)
    fields = []
    msg = ''

    if request.method == "POST":
        profile_data = models.set_profile(fields)
        msg = 'Profile Updated!'

    return render(request, "profile.html", {'profile_data': profile_data[0], 'msg': msg})


def acessDenied(request):
    return HttpResponse("You are not authorized!")


def change_password(username, password):
    ret = {'status': 'false', 'message': 'init', 'vnid': username}
    try:
        u = User.objects.get(username=username)
        u.set_password(password)
        u.save()
        ret = {'status': 'true', 'message': 'success', 'vnid': username}
    except IntegrityError:
        ret = {'status': 'false', 'message': 'Account Already Exists!', 'vnid': username}
    except DatabaseError:
        ret = {'status': 'false', 'message': 'Something Wrong!', 'vnid': username}

    return ret
