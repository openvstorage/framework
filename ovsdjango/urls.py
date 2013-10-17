import settings
from django.conf.urls import patterns, include, url
from django.views.generic import RedirectView
from views import ObtainAuthToken

urlpatterns = patterns('',
    url(r'^auth/',     ObtainAuthToken.as_view()),
    url(r'^customer/', include(settings.SYSTEM_NAME + '.customer.urls')),
    url(r'^internal/', include(settings.SYSTEM_NAME + '.internal.urls')),
    url(r'^$',         RedirectView.as_view(url='customer/')),
)
