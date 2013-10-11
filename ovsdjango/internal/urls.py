from views import UserViewSet, MemcacheViewSet
from rest_framework.routers import DefaultRouter

router = DefaultRouter()
router.register(r'users', UserViewSet, base_name='users')
router.register(r'memcache', MemcacheViewSet, base_name='memcache')
urlpatterns = router.urls