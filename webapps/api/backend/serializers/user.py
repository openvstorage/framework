# license see http://www.openvstorage.com/licenses/opensource/
"""
Contains specific User-related serializers
"""
from rest_framework import serializers


class PasswordSerializer(serializers.Serializer):
    """
    Serializes received passwords
    """
    current_password = serializers.CharField(required=True)
    new_password = serializers.CharField(required=True)

    class Meta:
        """
        Metaclass
        """
        fields = ('current_password', 'new_password')
