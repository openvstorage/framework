from django.contrib.auth import login, logout, authenticate
from toolbox import Toolbox


class AuthenticationMiddleware(object):
    def process_view(self, request, view_func, view_args, view_kwargs):
        user_guid = request.GET.get('user_guid')
        if user_guid is not None:
            if Toolbox.is_uuid(user_guid):
                user = authenticate(user_guid=user_guid)
                if user:
                    login(request, user)
                else:
                    logout(request)