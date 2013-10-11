from django.conf.urls import patterns, include, url
from views import APIRoot

urlpatterns = patterns('',
    url(r'^$', APIRoot.as_view(), name='root'),
    url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework'))
)