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
This module contains generic hybrid serializers
"""
import copy
from ovs.dal.helpers import Descriptor
from ovs.dal.relations import RelationMapper
from ovs_extensions.generic.toolbox import ExtensionsToolbox
from rest_framework import serializers


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
            if isinstance(contents, str):
                contents_list = contents.split(',')
            elif isinstance(contents, list):
                contents_list = contents
            else:
                raise UnsupportContentException('Contents should be a comma-separated list')
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


# noinspection PyProtectedMember
class FullSerializer(serializers.Serializer):
    """
    Serializes the persistent and dynamic stack of a hybrid object
    """
    guid = serializers.Field()  # Always include the GUID

    class Meta(object):
        """
        Meta class. Holds some information about the serializer
        - fields: Fields which included by default (can be edited by using the 'fields' attr in the serializer
        - read_only_fields: Indicates which fields are read only (can be edited by using the 'read_only_fields' attr in the serializer
        """
        fields = ('guid',)
        read_only_fields = ('guid',)

    def __init__(self, hybrid, contents=None, depth=None, *args, **kwargs):
        """
        Initializes the serializer, mapping field types
        :param hybrid: Hybrid object to serialize
        :type hybrid: any (ovs.dal.hybrids.X.X)
        :param contents: Contents to serialize. Without contents, only the GUID is serialized
        When contents is given, all non-dynamic properties are serialized
        Further options are:
        - _dynamics: Include all dynamic properties
        - _relations: Include foreign keys and lists of primary keys of linked objects
        - _relations_contents: Apply the contents to the relations. The relation contents can be a bool or a new contents item
          - If the relations_contents=re-use: the current contents are also applied to the relation object
          - If the relations_contents=contents list: That item is subjected to the same rules as other contents
        - _relation_contents_RELATION_NAME: Apply the contents the the given relation. Same rules as _relation_contents apply here
        _ _relations_depth: Depth of relational serialization. Defaults to 1 when relation_contents were specified.
        Specifying a form of _relations_contents change the depth to 1 (if depth was 0) as the relation is to be serialized
        Specifying it 2 with _relations_contents given will serialize the relations of the fetched relation. This causes a chain of serializations
        - dynamic_property_1,dynamic_property_2 (results in static properties plus 2 dynamic properties)
        Properties can also be excluded by prefixing the field with '-':
        - contents=_dynamic,-dynamic_property_2,_relations (static properties, all dynamic properties except for dynamic_property_2 plus all relations)
        Relation serialization can be done by asking for it:
        - contents=_relations,_relations_contents=re-use
        All relational serialization can only be used to get data. This data will be not be set-able when deserializing
        :type contents: list or none
        :param depth: Current depth of serializing, used to serialize relations
        :type depth: int
        Kwarg parameters:
        :param allow_passwords: Allow the attr 'password' to be serialized
        :type allow_passwords: bool
        Parent parameters:
        :param instance: Instance of the object to use for updating
        :type instance: an
        :param data: Initialization data (Will be applied to the instance if an instance is given)
        :type data: list[dict] or dict
        :param many: Indicate that the given instance is to be iterated for serialization
        :type many: bool
        """
        if not isinstance(contents, ContentOptions):
            contents = ContentOptions(contents)
        allow_passwords = kwargs.pop('allow_passwords', False)
        super(FullSerializer, self).__init__(*args, **kwargs)
        self.hybrid = hybrid
        for prop in self.hybrid._properties:
            if 'password' not in prop.name or allow_passwords:
                self.fields[prop.name] = FullSerializer._map_type_to_field(prop.property_type)
        for dynamic in self.hybrid._dynamics:
            if contents.has_content is False or (('_dynamics' in contents or dynamic.name in contents) and '-{0}'.format(dynamic.name) not in contents):
                self.fields[dynamic.name] = serializers.Field()
        for relation in self.hybrid._relations:
            if contents.has_content is False or (('_relations' in contents or relation.name in contents) and '-{0}'.format(relation.name) not in contents):
                self.fields['{0}_guid'.format(relation.name)] = serializers.CharField(required=False)
        foreign_relations = RelationMapper.load_foreign_relations(hybrid)  # To many side of things, items pointing towards this object
        if foreign_relations is not None:
            for key, info in foreign_relations.iteritems():
                if contents.has_content is False or (('_relations' in contents or key in contents) and '-{0}'.format(key) not in contents):
                    if info['list'] is True:
                        self.fields['%s_guids' % key] = serializers.Field()
                    else:
                        self.fields['%s_guid' % key] = serializers.Field()

        # Check is a relation needs to be serialized
        foreign_relations = RelationMapper.load_foreign_relations(hybrid)  # To many side of things, items pointing towards this object
        if contents.has_content is False or (foreign_relations is None and len(hybrid._relations) == 0) or depth == 0:
            return
        # Foreign relations is a dict, relations is a relation object, need to differentiate
        relation_contents = contents.get_option('_relations_contents')
        relation_contents_options = copy.deepcopy(contents) if relation_contents == 're-use' else ContentOptions(relation_contents)
        relations_data = {'foreign': foreign_relations or {}, 'own': hybrid._relations}
        for relation_type, relations in relations_data.iteritems():
            for relation in relations:
                relation_key = relation.name if relation_type == 'own' else relation
                relation_hybrid = relation.foreign_type if relation_type == 'own' else Descriptor().load(relations[relation]['class']).get_object()
                # Possible extra content supplied for a relation
                relation_content = contents.get_option('_relation_content_{0}'.format(relation_key))
                if relation_content is None and relation_contents == 're-use':
                    relation_content_options = relation_contents_options
                else:
                    relation_content_options = ContentOptions(relation_content)
                # Use the depth given by the contents when it's the first item to serialize
                relation_depth = contents.get_option('_relations_depth', 1 if relation_content_options.has_content else 0) if depth is None else depth
                if relation_depth is None:  # Can be None when no value is give to _relations_depth
                    relation_depth = 0
                if relation_depth == 0:
                    continue
                # @Todo prevent the same one-to-one relations from being serialized multiple times? Not sure if helpful though
                self.fields[relation_key] = FullSerializer(relation_hybrid, contents=relation_content_options, depth=relation_depth - 1, *args, **kwargs)

    def get_identity(self, data):
        """
        This hook makes sure the guid is returned as primary key
        By default the serializer class will use the id key on the incoming data to determine the canonical identity of an object
        """
        return data.get('guid', None)

    def restore_object(self, attrs, instance=None):
        """
        Provides deserializing functionality for persistent properties
        Required if we want our serializer to support deserialization into fully fledged object instances.
         If we don't define this method, then deserializing data will simply return a dictionary of items.
        """
        if instance is not None:
            for prop in self.hybrid._properties:
                setattr(instance, prop.name, attrs.get(prop.name, getattr(instance, prop.name)))
            for relation in self.hybrid._relations:
                guid_key = '{0}_guid'.format(relation.name)
                if guid_key in attrs and attrs[guid_key] != getattr(instance, guid_key):
                    setattr(instance, relation.name, None if attrs[guid_key] is None else relation.foreign_type(attrs[guid_key]))
            return instance
        return self.hybrid(data=attrs)

    @staticmethod
    def _map_type_to_field(field_type):
        """
        Maps the given field type to a serializer field
        """
        if isinstance(field_type, list):
            field_type = type(field_type[0])
        if field_type is str:
            return serializers.CharField(required=False)
        if field_type is int:
            return serializers.IntegerField(required=False)
        if field_type is bool:
            return serializers.BooleanField(required=False)
        if field_type is dict:
            return serializers.WritableField(required=False)
        return serializers.Field()

    def deserialize(self):
        _ = self.errors  # Trigger deserialization
        return self.object
