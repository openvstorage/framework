from rest_framework import serializers
from django import forms
from ovs.dal.hybrids.vmachine import vMachine


class SimpleVMachineSerializer(serializers.Serializer):
    guid = serializers.Field()

    class Meta:
        fields = ('guid',)
        read_only_fields = ('guid',)


class VMachineSerializer(SimpleVMachineSerializer):
    name = serializers.CharField(required=True, widget=forms.TextInput)

    def restore_object(self, attrs, instance=None):
        if instance is not None:
            instance.name = attrs.get('name', instance.name)
            return instance
        return vMachine(data=attrs)

    class Meta:
        fields = ('guid', 'name')
        read_only_fields = ('guid',)
