from . import views
from django.urls import path, re_path, include
from django.conf.urls import url

urlpatterns = [

    path('', views.ignite_home),
    re_path(r'.html$', views.ignite_home),
]
