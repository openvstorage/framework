from django.conf.urls import patterns, include, url
from django.contrib.auth.models import User, Group
from rest_framework import viewsets, routers


# ViewSets define the view behavior.
class UserViewSet(viewsets.ModelViewSet):
    model = User


class GroupViewSet(viewsets.ModelViewSet):
    model = Group

# Routers provide an easy way of automatically determining the URL conf
router = routers.DefaultRouter()
router.register(r'users', UserViewSet)
router.register(r'groups', GroupViewSet)

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browseable API.
urlpatterns = patterns('',
    url(r'^', include(router.urls)),
)