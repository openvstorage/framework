from django.conf.urls import patterns, include, url
from rest_framework.urlpatterns import format_suffix_patterns
from views import *

urlpatterns = patterns('',
    url(r'^memcached/$', memcached),
)

urlpatterns = format_suffix_patterns(urlpatterns)