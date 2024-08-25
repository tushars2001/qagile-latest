"""QAgile URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include, re_path
from django.conf import settings
from django.conf.urls.static import static
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from django.conf.urls import include
from django.contrib.auth.models import User
from rest_framework import routers, serializers, viewsets


admin.autodiscover()


# Serializers define the API representation.
class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = ['url', 'username', 'email', 'is_staff']


# ViewSets define the view behavior.
class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.all()
    serializer_class = UserSerializer


urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('QAgile.home.urls')),
    path('rfs/', include('QAgile.rfs.urls')),
    path('manage/', include('QAgile.admin.urls')),
    re_path(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    re_path('api/(?P<version>(v1|v2))/', include('QAgile.admin.urls')),
    path('identity/', include('QAgile.identity.urls')),
    path('analytics/', include('QAgile.analytics.urls')),
    # re_path(r'^saml/', include('django_python3_saml.urls')),
    # path('tools/', include('QAgile.tools.urls')),
    # path('ignite-tracker/', include('QAgile.ignite_tracker.urls')),
    path('test_data/', include('QAgile.test_data.urls')),
    path('test_data_v2/', include('QAgile.test_data_v2.urls')),
    path('project/', include('QAgile.project.urls')),
    path('profile/', include('QAgile.user.urls')),
    # re_path(r'^', include('cms.urls')),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

urlpatterns += staticfiles_urlpatterns()

