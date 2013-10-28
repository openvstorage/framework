from rest_framework import serializers


class PasswordSerializer(serializers.Serializer):
    password = serializers.CharField(required=True)

    class Meta:
        fields = ('password',)