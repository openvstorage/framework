# Copyright 2014 CloudFounders NV
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Contains the MessageViewSet
"""

import gevent
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import link, action
from ovs.lib.messaging import MessageController
from backend.decorators import required_roles, expose, discover


class MessagingViewSet(viewsets.ViewSet):
    """
    Information about messages
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'messages'
    base_name = 'messages'

    @expose(internal=True)
    @required_roles(['view'])
    @discover()
    def list(self):
        """
        Provides a list of subscriptions
        """
        return Response(MessageController.all_subscriptions(), status=status.HTTP_200_OK)

    @expose(internal=True)
    @required_roles(['view'])
    @discover()
    def retrieve(self, pk):
        """
        Retrieves the subscriptions for a given subscriber
        """
        try:
            pk = int(pk)
        except (ValueError, TypeError):
            return Response(status=status.HTTP_400_BAD_REQUEST)
        return Response(MessageController.subscriptions(pk), status=status.HTTP_200_OK)

    @staticmethod
    def _wait(subscriber_id, message_id):
        messages = []
        last_message_id = 0
        counter = 0
        while len(messages) == 0:
            messages, last_message_id = MessageController.get_messages(subscriber_id, message_id)
            if len(messages) == 0:
                counter += 1
                if counter >= 240:
                    break
                gevent.sleep(.5)
        if len(messages) == 0:
            last_message_id = MessageController.last_message_id()
        MessageController.reset_subscriptions(subscriber_id)
        return messages, last_message_id

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @discover()
    def wait(self, pk, message_id):
        """
        Wait for messages to appear for a given subscriber
        """
        try:
            pk = int(pk)
            message_id = int(message_id)
        except (ValueError, TypeError):
            return Response(status=status.HTTP_400_BAD_REQUEST)
        thread = gevent.spawn(MessagingViewSet._wait, pk, message_id)
        gevent.joinall([thread])
        messages, last_message_id = thread.value
        return Response({'messages'       : messages,
                         'last_message_id': last_message_id,
                         'subscriptions'  : MessageController.subscriptions(pk)}, status=status.HTTP_200_OK)

    @link()
    @expose(internal=True)
    @required_roles(['view'])
    @discover()
    def last(self, pk):
        """
        Get the last messageid
        """
        try:
            _ = int(pk)
        except (ValueError, TypeError):
            return Response(status=status.HTTP_400_BAD_REQUEST)
        return Response(MessageController.last_message_id(), status=status.HTTP_200_OK)

    @action()
    @expose(internal=True)
    @required_roles(['view'])
    @discover()
    def subscribe(self, request, pk):
        """
        Subscribes a subscriber to a set of types
        """
        try:
            pk = int(pk)
            subscriptions = request.DATA
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
