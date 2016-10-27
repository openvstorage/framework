# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

"""
Contains the MessageViewSet
"""

import gevent
from rest_framework import status, viewsets
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import link, action
from ovs.lib.messaging import MessageController
from backend.decorators import required_roles, load, log


class MessagingViewSet(viewsets.ViewSet):
    """
    Information about messages
    """
    permission_classes = (IsAuthenticated,)
    prefix = r'messages'
    base_name = 'messages'
    skip_spec = True

    @log()
    @required_roles(['read'])
    @load()
    def list(self):
        """
        Provides a list of subscriptions
        """
        return Response(MessageController.all_subscriptions(), status=status.HTTP_200_OK)

    @log()
    @required_roles(['read'])
    @load()
    def retrieve(self, pk):
        """
        Retrieves the subscriptions for a given subscriber
        :param pk: Primary key of subscriber
        :type pk: int
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
                if counter >= 120:  # 120 * 0.5 seconds = 60 seconds  = 1 minute
                    break
                gevent.sleep(.5)
        if len(messages) == 0:
            last_message_id = MessageController.last_message_id()
        MessageController.reset_subscriptions(subscriber_id)
        return messages, last_message_id

    @link()
    @log(log_slow=False)
    @required_roles(['read'])
    @load()
    def wait(self, pk, message_id):
        """
        Wait for messages to appear for a given subscriber
        :param pk: Primary key of subscriber
        :type pk: int
        :param message_id: The last message_id that was already received
        :type message_id: str
        """
        try:
            pk = int(pk)
            message_id = int(message_id)
        except (ValueError, TypeError):
            return Response(status=status.HTTP_400_BAD_REQUEST)
        thread = gevent.spawn(MessagingViewSet._wait, pk, message_id)
        gevent.joinall([thread])
        result = thread.value
        if result is None:
            messages = []
            last_message_id = message_id
        else:
            messages, last_message_id = thread.value
        return Response({'messages': messages,
                         'last_message_id': last_message_id,
                         'subscriptions': MessageController.subscriptions(pk)}, status=status.HTTP_200_OK)

    @link()
    @log()
    @required_roles(['read'])
    @load()
    def last(self, pk):
        """
        Get the last messageid
        :param pk: Primary key of subscriber
        :type pk: int
        """
        try:
            _ = int(pk)
        except (ValueError, TypeError):
            return Response(status=status.HTTP_400_BAD_REQUEST)
        return Response(MessageController.last_message_id(), status=status.HTTP_200_OK)

    @action()
    @log()
    @required_roles(['read'])
    @load()
    def subscribe(self, request, pk):
        """
        Subscribes a subscriber to a set of types
        :param request: Raw request object of the call
        :type request: Request
        :param pk: Primary key of subscriber
        :type pk: int
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
