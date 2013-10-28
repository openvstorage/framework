from rest_framework import serializers
from django import forms
from ovs.dal.hybrids.vmachine import vMachine


class SimpleSerializer(serializers.Serializer):
    guid = serializers.Field()

    class Meta:
        fields = ('guid',)
        read_only_fields = ('guid',)


class FullSerializer(SimpleSerializer):
    def __init__(self, hybrid, *args, **kwargs):
        super(FullSerializer, self).__init__(*args, **kwargs)
        self.hybrid = hybrid
        for key, default in self.hybrid._blueprint.iteritems():
            self.fields[key] = FullSerializer._map_type_to_field(default[1])
        for key in self.hybrid._expiry:
            self.fields[key] = serializers.Field()

    def restore_object(self, attrs, instance=None):
        if instance is not None:
            for key in self.hybrid._blueprint:
                setattr(instance, key, attrs.get(key, getattr(instance, key)))
            return instance
        return vMachine(data=attrs)

    @staticmethod
    def _map_type_to_field(type):
        if type is str:
            return serializers.CharField()
        if type is int:
            return serializers.IntegerField()
        if type is bool:
            return serializers.BooleanField()
        return serializers.Field()