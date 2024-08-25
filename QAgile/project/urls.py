from django.urls import path
from . import views

urlpatterns = [
    path('', views.my_projects),
    path('create/', views.create),
]
