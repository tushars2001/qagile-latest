from rest_framework import serializers
from .models import get_all_person


class PersonsSerializer(serializers.ModelSerializer):
    class Meta:
        model = get_all_person
        fields = ("VNID", "first_name")


class YourSerializer(serializers.Serializer):
    """Your data serializer, define your fields here."""
    comments = serializers.IntegerField()
    likes = serializers.IntegerField()