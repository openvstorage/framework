from django.conf.urls import *
from django.views.generic import RedirectView
from views import *

urlpatterns = patterns('',
    url(r'^$', RedirectView.as_view(url='full/')),
    url(r'^(?P<mode>[a-z]+?)/$', dashboard, name='dashboard'),
    url(r'^(?P<mode>[a-z]+?)/statistics/$', statistics, name='statistics')
)