from django.contrib.auth.models import User
from toolbox import Toolbox
from ovs.lib.user import User as LibUser
from ovs.dal.hybrids.user import User as HybridUser
from ovs.dal.exceptions import ObjectNotFoundException
from rest_framework.authentication import BaseAuthentication
from rest_framework import exceptions, HTTP_HEADER_ENCODING
import logging
import settings

logger = logging.getLogger(settings.SYSTEM_NAME)


class UPAuthenticationBackend(object):
    def authenticate(self, username=None, password=None):
        if username is None or password is None:
            return None

        cuser = LibUser.get_user_by_username(str(username))
        if cuser is None:
            logger.error('User with username %s could not be found' % username)
            return None

        if password != cuser.password:
            logger.error('Wrong password provided for %s' % username)
            return None

        if not cuser.is_active:
            msg = 'User inactive or deleted'
            logger.error(msg)
            return None

        # We have authenticated the user. Let's make sure there is a corresponding User object and return it
        try:
            user = User.objects.get(username=cuser.username)
        except User.DoesNotExist:
            user = User.objects.create_user(cuser.username, 'nobody@example.com')
            logger.info('Created user %s' % username)
            user.is_active = cuser.is_active
            user.is_staff = False
            user.is_superuser = False
            user.save()

        return user

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None


class TokenAuthenticationBackend(BaseAuthentication):
    """
    Simple token based authentication, changed implementation from the Django REST Framework

    Clients should authenticate by passing the token key in the "Authorization"
    HTTP header, prepended with the string "Token ".  For example:

        Authorization: Token 401f7ac837da42b97f613d789819ff93537bee6a
    """
    def authenticate(self, request):
        auth = TokenAuthenticationBackend._get_authorization_header(request).split()

        if not auth or auth[0].lower() != b'token':
            return None

        if len(auth) == 1:
            msg = 'Invalid token header. No credentials provided.'
            logger.error(msg)
            raise exceptions.AuthenticationFailed(msg)
        elif len(auth) > 2:
            msg = 'Invalid token header. Token string should not contain spaces.'
            logger.error(msg)
            raise exceptions.AuthenticationFailed(msg)

        return TokenAuthenticationBackend._authenticate_credentials(auth[1])

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None

    @staticmethod
    def _authenticate_credentials(key):
        try:
            cuser = HybridUser(key)
        except ObjectNotFoundException:
            msg = 'Invalid token'
            logger.error(msg)
            raise exceptions.AuthenticationFailed(msg)

        if not cuser.is_active:
            msg = 'User inactive or deleted'
            logger.error(msg)
            raise exceptions.AuthenticationFailed()

        try:
            user = User.objects.get(username=cuser.username)
        except User.DoesNotExist:
            user = User.objects.create_user(cuser.username, 'nobody@example.com')
            logger.info('Created user %s' % cuser.username)
            user.is_active = cuser.is_active
            user.is_staff = False
            user.is_superuser = False
            user.save()

        return user, None

    def authenticate_header(self, request):
        return 'Token'

    @staticmethod
    def _get_authorization_header(request):
        """
        Return request's 'Authorization:' header, as a bytestring.

        Hide some test client ickyness where the header can be unicode.
        """
        auth = request.META.get('HTTP_AUTHORIZATION', b'')
        if isinstance(auth, str):
            # Work around django test client oddness
            auth = auth.encode(HTTP_HEADER_ENCODING)
        return auth


class HashAuthenticationBackend(object):
    def authenticate(self, user_guid=None):
        if user_guid is None:
            logger.error('No guid was passed')
            return None

        if not Toolbox.is_uuid(user_guid):
            logger.error('Invalid guis %s passed' % user_guid)
            return None

        try:
            cuser = HybridUser(user_guid)
        except ObjectNotFoundException:
            logger.error('User with guid %s could not be found' % user_guid)
            return None

        if not cuser.is_active:
            msg = 'User inactive or deleted'
            logger.error(msg)
            return None

        # We have authenticated the user. Let's make sure there is a corresponding User object and return it
        try:
            user = User.objects.get(username=cuser.username)
        except User.DoesNotExist:
            user = User.objects.create_user(cuser.username, 'nobody@example.com')
            logger.info('Created user %s' % cuser.username)
            user.is_active = cuser.is_active
            user.is_staff = True
            user.is_superuser = True
            user.save()

        return user

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None