from . import views
from django.urls import path, re_path

urlpatterns = [

    path('', views.analytics_home),
    path('load_tenrox', views.load_tenrox),
    path('load_jira/', views.load_jira),
    path('planned-vs-actuals', views.planned_actuals),
    path('resource-plan', views.resource_plan),
    path('capacity', views.capacity),
    path('jira/', views.jira_home),
    path('jira/job-details/', views.get_job_details),
    path('jira/projects/', views.projects),
    path('jira/projects/tracking/', views.tracking),
]
