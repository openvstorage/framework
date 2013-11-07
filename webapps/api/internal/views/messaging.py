import time
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import link, action
from ovs.lib.messaging import MessageController
from backend.decorators import required_roles


class MessagingViewSet(viewsets.ViewSet):
    """
    Information about messages
    """
    permission_classes = (IsAuthenticated,)

    @required_roles(['view'])
    def list(self, request, format=None):
        return Response(MessageController.all_subscriptions(), status=status.HTTP_200_OK)

    @required_roles(['view'])
    def retrieve(self, request, pk=None, format=None):
        try:
            pk = int(pk)
        except (ValueError, TypeError):
            return Response(status=status.HTTP_400_BAD_REQUEST)
        return Response(MessageController.subscriptions(pk), status=status.HTTP_200_OK)

    @required_roles(['view'])
    @link()
    def wait(self, request, pk=None, format=None):
        try:
            pk = int(pk)
            message_id = int(self.request.QUERY_PARAMS.get('message_id', None))
        except (ValueError, TypeError):
            return Response(status=status.HTTP_400_BAD_REQUEST)
        messages = []
        last_message_id = 0
        counter = 0
        while len(messages) == 0:
            messages, last_message_id = MessageController.get_messages(pk, message_id)
            if len(messages) == 0:
                counter += 1
                if counter >= 240:
                    break
                time.sleep(.5)
        if len(messages) == 0:
            last_message_id = MessageController.last_message_id()
        return Response({'messages'       : messages,
                         'last_message_id': last_message_id,
                         'subscriptions'  : MessageController.subscriptions(pk)}, status=status.HTTP_200_OK)

    @required_roles(['view'])
    @link()
    def last(self, request, pk=None, format=None):
        try:
            pk = int(pk)
        except (ValueError, TypeError):
            return Response(status=status.HTTP_400_BAD_REQUEST)
        return Response(MessageController.last_message_id(), status=status.HTTP_200_OK)

    @required_roles(['view'])
    @action()
    def subscribe(self, request, pk=None, format=None):
        try:
            pk = int(pk)
            subscriptions = self.request.DATA
            cleaned_subscriptions = []
            if not isinstance(subscriptions, list):
                raise TypeError
            for s in subscriptions:
                if str(s) in MessageController.Type.ALL:
                    cleaned_subscriptions.append(str(s))
        except (ValueError, TypeError):
            return Response(status=status.HTTP_400_BAD_REQUEST)
        MessageController.subscribe(pk, cleaned_subscriptions)
        return Response(cleaned_subscriptions, status=status.HTTP_200_OK)