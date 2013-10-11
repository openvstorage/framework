from rest_framework import serializers
from django import forms
from ovsdal.hybrids.user import User


class UserSerializer(serializers.Serializer):
    guid     = serializers.Field()
    username = serializers.CharField(required=True, widget=forms.TextInput)
    password = serializers.CharField(required=True, widget=forms.PasswordInput)
    email    = serializers.EmailField(widget=forms.TextInput)

    def restore_object(self, attrs, instance=None):
        if instance is not None:
            instance.username = attrs.get('username', instance.username)
            instance.password = attrs.get('password', instance.password)
            instance.email    = attrs.get('email', instance.email)
            return instance
        return User(data=attrs)

    class Meta:
        fields = ('guid', 'username', 'password', 'email')
        read_only_fields = ('guid',)


class PasswordSerializer(serializers.Serializer):
    password = serializers.CharField(required=True)

    class Meta:
        fields = ('password',)