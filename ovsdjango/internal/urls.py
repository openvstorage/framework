from views.poc_tests import TestViewSet
from views.statistics import MemcacheViewSet
from views.vmachines import VMachineViewSet
from views.vdisks import VDiskViewSet
from views.users import UserViewSet
from views.tasks import TaskViewSet
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(r'users',     UserViewSet,     base_name='users')
router.register(r'tasks',     TaskViewSet,     base_name='tasks')
router.register(r'vmachines', VMachineViewSet, base_name='vmachines')
router.register(r'vdisks',    VDiskViewSet,    base_name='vdisks')
# Test api:
router.register(r'statistics/memcache', MemcacheViewSet, base_name='memcache')
router.register(r'test', TestViewSet, base_name='test')
urlpatterns = router.urls