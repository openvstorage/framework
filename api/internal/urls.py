from django.conf.urls import patterns, include, url
from rest_framework.urlpatterns import format_suffix_patterns
from views import Memcached, APIRoot

urlpatterns = patterns('',
    url(r'^$', APIRoot.as_view(), name='root'),
    url(r'^memcached/$', Memcached.as_view(), name='memcached'),
    url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework')),
)

urlpatterns = format_suffix_patterns(urlpatterns)