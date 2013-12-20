# license see http://www.openvstorage.com/licenses/opensource/
"""
This module contains generic hybrid serializers
"""
from rest_framework import serializers


class SimpleSerializer(serializers.Serializer):
    """
    Serializes only the guid of a hybrid object
    """
    guid = serializers.Field()

    class Meta:
        """
        Meta class
        """
        fields = ('guid',)
        read_only_fields = ('guid',)


class FullSerializer(SimpleSerializer):
    """
    Serializes the persistent and dynamic stack of a hybrid object
    """
    def __init__(self, hybrid, *args, **kwargs):
        """
        Initializes the serializer, mapping field types
        """
        super(FullSerializer, self).__init__(*args, **kwargs)
        self.hybrid = hybrid
        for key, default in self.hybrid._blueprint.iteritems():
            self.fields[key] = FullSerializer._map_type_to_field(default[1])
        for key in self.hybrid._expiry:
            self.fields[key] = serializers.Field()
        for key in self.hybrid._relations:
            self.fields['%s_guid' % key] = serializers.Field()

    def get_identity(self, data):
        """
        This hook makes sure the guid is returned as primary key
        """
        return data.get('guid', None)

    def restore_object(self, attrs, instance=None):
        """
        Provides deserializing functionality for persistent properties
        """
        if instance is not None:
            for key in self.hybrid._blueprint:
                setattr(instance, key, attrs.get(key, getattr(instance, key)))
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
            return serializers.CharField()
        if field_type is int:
            return serializers.IntegerField()
        if field_type is bool:
            return serializers.BooleanField()
        return serializers.Field()
