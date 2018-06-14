import copy
from ovs_extensions.generic.toolbox import ExtensionsToolbox


class UnsupportContentException(ValueError):
    """
    Exception raised when an unsupported content string has been given
    """
    pass


class ContentOptions(object):
    """
    Content options to give to the serializer
    """
    OPTION_TYPES = {'_relations_depth': (int, None, False),
                    '_relations_content': (str, None, False)}
    OPTION_STARTS = {'_relation_contents_': (str, None, False)}

    def __init__(self, contents=None):
        """
        Initializes a ContentOptions object based on a string representing the contents
        :param contents: Comma separated string or list of contents to serialize
        When contents is given, all non-dynamic properties would be serialized
        Further options are:
        - _dynamics: Include all dynamic properties
        - _relations: Include foreign keys and lists of primary keys of linked objects
        - _relations_contents: Apply the contents to the relations. The relation contents can be a bool or a new contents item
          - If the relations_contents=re-use: the current contents are also applied to the relation object
          - If the relations_contents=contents list: That item is subjected to the same rules as other contents
        - _relation_contents_RELATION_NAME: Apply the contents the the given relation. Same rules as _relation_contents apply here
        _ _relations_depth: Depth of relational serialization. Defaults to 0.
        Specifying a form of _relations_contents change the depth to 1 (if depth was 0) as the relation is to be serialized
        Specifying it 2 with _relations_contents given will serialize the relations of the fetched relation. This causes a chain of serializations
        - dynamic_property_1,dynamic_property_2 (results in static properties plus 2 dynamic properties)
        Properties can also be excluded by prefixing the field with '-':
        - contents=_dynamic,-dynamic_property_2,_relations (static properties, all dynamic properties except for dynamic_property_2 plus all relations)
        Relation serialization can be done by asking for it:
        - contents=_relations,_relations_contents=re-use
        :type contents: list or str
        :raises UnsupportedContentException: If a content string is passed which is not valid
        """
        super(ContentOptions, self).__init__()

        verify_params = copy.deepcopy(self.OPTION_TYPES)
        self.content_options = {}
        self.has_content = False
        if contents is not None:
            if isinstance(contents, basestring):
                contents_list = contents.split(',')
            elif isinstance(contents, list):
                contents_list = contents
            else:
                raise UnsupportContentException('Contents should be a comma-separated list instead of \'{0}\''.format(contents))
        else:
            return
        self.has_content = True
        errors = []
        for option in contents_list:
            if not isinstance(option, basestring):
                errors.append('Provided option \'{0}\' is not a string but \'{1}\''.format(option, type(option)))
                continue
            split_options = option.split('=')
            if len(split_options) > 2:  # Unsupported format
                errors.append('Found \'=\' multiple times for entry {0}'.format(split_options[0]))
                continue
            starts = [v for k, v in self.OPTION_STARTS.iteritems() if option.startswith(k)]
            if len(starts) == 1:
                verify_params[option] = starts[0]
            # Convert to some work-able types
            value = split_options[1] if len(split_options) == 2 else None
            if isinstance(value, str) and value.isdigit():
                value = int(value)
            self.content_options[split_options[0]] = value
        errors.extend(ExtensionsToolbox.verify_required_params(verify_params, self.content_options, return_errors=True))
        if len(errors) > 0:
            raise UnsupportContentException('Contents is using an unsupported format: \n - {0}'.format('\n - '.join(errors)))

    def __contains__(self, item):  # In operator
        return self.has_option(item)

    def has_option(self, option):
        """
        Returns True if the contentOption has the given option
        :param option: Option to search for
        :type option: str
        :return: bool
        """
        return option in self.content_options

    def get_option(self, option, default=None):
        """
        Returns the value of the given option
        :param option: Option to retrieve the value for
        :type option: str
        :param default: Default value when the key does not exist
        :type default: any
        :return: None if the value is not found else the value specified
        :rtype: NoneType or any
        """
        return self.content_options.get(option, default)

    def set_option(self, option, value, must_exist=True):
        """
        Sets an options value
        :param option: Option to set the value for
        :type option: str
        :param value: Value of the option
        :type value: any
        :param must_exist: The option must already exist before setting the option
        :type must_exist: bool
        :return: The given value (None if the key does not exist)
        :rtype: NoneType or any
        """
        if must_exist is True and self.has_option(option) is False:
            return None
        self.content_options[option] = value
        return value

    def increment_option(self, option):
        """
        Increments the value for the given option. If the option is not present or no value passed, this won't do anything
        :param option: Option to increment the value for
        :type option: str
        :return: The new value or None if they key is not found or not an integer
        :rtype: int or NoneType
        """
        value = self.get_option(option)
        if isinstance(value, int):
            return self.set_option(option, value + 1, must_exist=True)
        return None  # For readability

    def decrement_options(self, option):
        """
        Decrements the value for the given option. If the option is not present or no value passed, this won't do anything
        :param option: Option to increment the value for
        :type option: str
        :return: The new value or None if they key is not found or not an integer
        :rtype: int or NoneType
        """
        value = self.get_option(option)
        if isinstance(value, int):
            return self.set_option(option, value - 1, must_exist=True)
        return None  # For readability

contents_str = '_relations,_relation_contents_vdisks=_relations,status,targets,vdisks'
contents = ContentOptions(contents_str)
relation_key = 'vdisks'
relation_content = contents.get_option('_relation_contents_{0}'.format(relation_key))
print relation_content