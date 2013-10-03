import settings
from django.conf.urls import patterns, include, url
from django.views.generic import RedirectView

# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    url(r'^api/', include(settings.BASE_NAME + '.api.urls')),
    url(r'^portal/', include(settings.BASE_NAME + '.frontend.urls')),
    url(r'^$', RedirectView.as_view(url='portal/')),
)
