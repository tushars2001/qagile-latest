from django.urls import path
from . import views

urlpatterns = [
    path('jira/test_cases/', views.jira_test_cases),
]
