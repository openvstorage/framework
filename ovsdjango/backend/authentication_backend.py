from django.contrib.auth.models import User
from toolbox import Toolbox
from ovs.lib.user import User as OVSUser
from ovs.dal.hybrids.user import User as HybridUser
from ovs.dal.exceptions import ObjectNotFoundException
from rest_framework.authtoken.models import Token
import logging
import settings

logger = logging.getLogger(settings.SYSTEM_NAME)


class UPAuthenticationBackend(object):
    def authenticate(self, username=None, password=None):
        logger.info('Entered username/password authentication')
        if username is None or password is None:
            return None

        cuser = OVSUser.get_user_by_username(str(username))
        if cuser is None:
            logger.info('User with username %s could not be found' % username)
            return None

        if password != cuser.password:
            logger.info('Wrong password provided for %s' % username)
            return None

        # We have authenticated the user. Let's make sure there is a corresponding User object and return it
        try:
            user = User.objects.get(username=cuser.username)
            logger.info('Loaded user %s' % username)
        except User.DoesNotExist:
            user = User.objects.create_user(cuser.username, 'nobody@example.com')
            logger.info('Created user %s' % username)

        user.is_active = True
        user.is_staff = False
        user.is_superuser = False
        user.save()
        token = Token.objects.get_or_create(user=user)
        logger.info('Token for %s is %s' % (username, token))

        return user

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None


class HashAuthenticationBackend(object):
    def authenticate(self, user_guid=None):
        if user_guid is None:
            return None

        if not Toolbox.is_uuid(user_guid):
            return None

        try:
            cuser = HybridUser(user_guid)
        except ObjectNotFoundException:
            return None

        # We have authenticated the user. Let's make sure there is a corresponding User object and return it
        try:
            user = User.objects.get(username=cuser.username)
        except User.DoesNotExist:
            user = User.objects.create_user(cuser.username, 'nobody@example.com')
            user.is_active = True
            user.is_staff = True
            user.is_superuser = True
            user.save()

        return user

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None