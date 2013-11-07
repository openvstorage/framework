from rest_framework.views import APIView
from rest_framework import status
from rest_framework import parsers
from rest_framework import renderers
from rest_framework.response import Response
from rest_framework.authtoken.models import Token
from rest_framework.authtoken.serializers import AuthTokenSerializer
from ovs.dal.hybrids.user import User


class ObtainAuthToken(APIView):
    """
    Custom implementation of the Django REST framework ObtainAuthToken class, making
    use of our own model/token system
    """
    throttle_classes = ()
    permission_classes = ()
    parser_classes = (parsers.FormParser, parsers.MultiPartParser, parsers.JSONParser,)
    renderer_classes = (renderers.JSONRenderer,)
    serializer_class = AuthTokenSerializer
    model = Token

    def post(self, request):
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid():
            user = User(serializer.object['user'].username)
            return Response({'token': user.guid})
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
