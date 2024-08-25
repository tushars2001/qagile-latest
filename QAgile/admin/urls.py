from django.urls import path, include
from . import views


urlpatterns = [
    path('', views.admin),
    path('team/', views.team),
    path('org-chart/', views.org_chart),
    path('rfs/', views.rfs),
    path('team/person/', views.editperson),
    path('team/domains/', views.editdomains),
    path('team/role/', views.editrole),
    path('team/location/', views.editlocation),
    path('team/rate/', views.editrate),
    path('team/ldif/', views.ldif),
    path('team/upload/', views.upload),
    path('user/create/', views.user_create),
    path('user/reset/', views.user_reset),
]

