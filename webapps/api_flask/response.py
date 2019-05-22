from flask import Response, current_app, request, json
from ovs.dal.dataobject import DataObject
from ovs.dal.datalist import DataList

CONVERTABLE_TYPES = (list, dict, DataList, DataObject)


class ResponseOVS(Response):
    """
    Extend flask.Response with support for list/dict/OVS dal conversion to JSON.
    """

    def __init__(self, content=None, *args, **kargs):
        if isinstance(content, CONVERTABLE_TYPES):
            kargs['mimetype'] = 'application/json'
            content = to_json(content)

        super(Response, self).__init__(content, *args, **kargs)

    @classmethod
    def force_type(cls, response, environ=None):
        """Override with support for list/dict."""
        if isinstance(response, CONVERTABLE_TYPES):
            return cls(response)
        else:
            return super(Response, cls).force_type(response, environ)


def to_json(content):
    """
    Converts content to json while respecting config options.
    """
    # @todo parse options regarding extra params

    indent = None
    separators = (',', ':')
    if isinstance(content, DataList):
        base_datalist = {u'_contents': None,
                         u'_paging': {u'current_page': 1,
                                     u'end_number': 1,
                                     u'max_page': 1,
                                     u'page_size': 1,
                                     u'start_number': 1,
                                     u'total_items': 1},
                         u'_sorting': [u'name'],
                         u'data': [u'{0}'.format(o.serialize()['guid']) for o in content]}
        out = base_datalist
    else:
        out = content
    if (current_app.config['JSONIFY_PRETTYPRINT_REGULAR'] and not request.is_xhr):
        indent = 2
        separators = (', ', ': ')
    print out
    return (json.dumps(out, indent=indent, separators=separators), '\n')
