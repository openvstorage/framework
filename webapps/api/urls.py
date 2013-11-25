# license see http://www.openvstorage.com/licenses/opensource/
"""
Django URL module for main API
"""
from django.conf.urls import patterns, include, url
from django.views.generic import RedirectView
from rest_framework.reverse import reverse

from views import ObtainAuthToken
from backend.views.statistics import MemcacheViewSet
from backend.views.vmachines import VMachineViewSet
from backend.views.vdisks import VDiskViewSet
from backend.views.users import UserViewSet
from backend.views.tasks import TaskViewSet
from backend.views.messaging import MessagingViewSet
from backend.views.branding import BrandingViewSet
from backend.router import OVSRouter


def build_router_urls(api_mode, docs):
    """
    Creates a router instance to generate API urls for Customer and Internal API
    """
    routes = [
        {'prefix': r'users',               'viewset': UserViewSet,      'base_name': 'users'},
        {'prefix': r'tasks',               'viewset': TaskViewSet,      'base_name': 'tasks'},
        {'prefix': r'vmachines',           'viewset': VMachineViewSet,  'base_name': 'vmachines'},
        {'prefix': r'vdisks',              'viewset': VDiskViewSet,     'base_name': 'vdisks'},
        {'prefix': r'messages',            'viewset': MessagingViewSet, 'base_name': 'messages'},
        {'prefix': r'branding',            'viewset': BrandingViewSet,  'base_name': 'branding'},
        {'prefix': r'statistics/memcache', 'viewset': MemcacheViewSet,  'base_name': 'memcache'}
    ]
    router = OVSRouter(api_mode, docs)
    for route in routes:
        router.register(**route)
    return router.urls

customer_docs = """
Customer API.<br />
This API can be used for integration or automatisation with 3rd party applications.
"""
internal_docs = """
Internal API.<br />
This API is for internal use only (used by the Open vStorage framework) and is subject
to continuous changes without warning. It should not be used by 3rd party applications.
Unauthorized usage of this API can lead to unexpected results, issues or even data loss. See
the <a href='%(customerapi)s'>Customer API</a>.
"""

urlpatterns = patterns('',
    url(r'^auth/',      ObtainAuthToken.as_view()),
    url(r'^customer/',  include(build_router_urls('customer', customer_docs))),
    url(r'^internal/',  include(build_router_urls('internal', internal_docs))),
    url(r'^$',          RedirectView.as_view(url='customer/')),
)
