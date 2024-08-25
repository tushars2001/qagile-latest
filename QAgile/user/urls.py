from django.urls import path
from . import views

urlpatterns = [
    path('', views.profile),
    path('change-password/', views.change_password),
    path('change-email/', views.change_email),
    path('change-phone/', views.change_phone),
    path('capacity/', views.capacity),
    path('delegation/', views.delegation),
    path('update-field/', views.update_field),
]
