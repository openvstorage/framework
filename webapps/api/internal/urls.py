"""
Django URL module for Internal API
"""
from views.statistics import MemcacheViewSet
from views.vmachines import VMachineViewSet
from views.vdisks import VDiskViewSet
from views.users import UserViewSet
from views.tasks import TaskViewSet
from views.messaging import MessagingViewSet
from views.branding import BrandingViewSet
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(r'users',     UserViewSet,      base_name='users')
router.register(r'tasks',     TaskViewSet,      base_name='tasks')
router.register(r'vmachines', VMachineViewSet,  base_name='vmachines')
router.register(r'vdisks',    VDiskViewSet,     base_name='vdisks')
router.register(r'messages',  MessagingViewSet, base_name='messages')
router.register(r'branding',  BrandingViewSet,  base_name='branding')
# Test api:
router.register(r'statistics/memcache', MemcacheViewSet, base_name='memcache')
urlpatterns = router.urls
