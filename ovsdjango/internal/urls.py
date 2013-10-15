from views import UserViewSet, MemcacheViewSet, TestViewSet
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(r'users', UserViewSet, base_name='users')
router.register(r'memcache', MemcacheViewSet, base_name='memcache')
router.register(r'test', TestViewSet, base_name='test')
urlpatterns = router.urls