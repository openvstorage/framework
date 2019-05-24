from flask import Response, current_app, request, json
from ovs.dal.dataobject import DataObject
from ovs.dal.datalist import DataList
from api_flask.backend.serializers.serializers import to_json
CONVERTABLE_TYPES = (list, dict, DataList, DataObject)


class ResponseOVS(Response):
    """
    Extend flask.Response with support for list/dict/OVS dal conversion to JSON.
    """

    def __init__(self, content=None, *args, **kwargs):
        if isinstance(content, CONVERTABLE_TYPES):
            kwargs['mimetype'] = 'application/json'
            extra_arguments = request.args
            content = to_json(content, **extra_arguments)

        super(Response, self).__init__(content, *args, **kwargs)

    @classmethod
    def force_type(cls, response, environ=None):
        """Override with support for list/dict."""
        if isinstance(response, CONVERTABLE_TYPES):
            return cls(response)
        else:
            return super(Response, cls).force_type(response, environ)
