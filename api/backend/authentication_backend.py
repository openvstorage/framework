from django.contrib.auth.models import User
from toolbox import Toolbox
from ovsapi.user import User as OVSUser
from ovsdal.hybrids.user import User as HybridUser
from ovsdal.exceptions import ObjectNotFoundException


class UPAuthenticationBackend(object):
    def authenticate(self, username=None, password=None):
        if username is None or password is None:
            return None

        cuser = OVSUser.get_user_by_username(username)
        if password != cuser.password:
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