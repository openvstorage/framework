from django.conf.urls import patterns, include, url
from django.contrib.auth.models import User, Group
from rest_framework import viewsets, routers

# Routers provide an easy way of automatically determining the URL conf
router = routers.DefaultRouter()

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browseable API.
urlpatterns = patterns('',
    url(r'^', include(router.urls)),
)