import settings
from django.conf.urls import patterns, include, url
from django.views.generic import RedirectView

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    url(r'^customer/', include(settings.BASE_NAME + '.customer.urls')),
    url(r'^internal/', include(settings.BASE_NAME + '.internal.urls')),
    url(r'^$',         RedirectView.as_view(url='customer/')),
)
