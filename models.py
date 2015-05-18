# -*- coding: utf-8 -*-
# REDUCED
# CANDEDAT TO DELETE

import inspect
import sys

from django.db.models import Model
from django.db.models.signals import post_save
from django.db.models.signals import pre_delete


class SearchableModelMixin(object):

    @classmethod
    def get_bulk_qs(cls):
        return cls.objects.all()

    @classmethod
    def post_save_handler(cls, instance, **kwargs):
        cls.SearchSchema.put(instance)

    @classmethod
    def pre_delete_handler(cls, instance, **kwargs):
        cls.SearchSchema.delete(instance)


class SearchablePublishedMixin(SearchableModelMixin):

    @classmethod
    def get_bulk_qs(cls):
        return cls.objects.published()

    @classmethod
    def post_save_handler(cls, instance, **kwargs):
        print instance
        if instance.published:
            cls.SearchSchema.put(instance)
        else:
            if cls.SearchSchema.exists(instance.pk):
                cls.SearchSchema.delete(instance)


def search_trigger(name):
    module = sys.modules[name]
    gen = (tupl[1] for tupl in inspect.getmembers(module)
           if inspect.isclass(tupl[1]) and issubclass(tupl[1], Model)
           and issubclass(tupl[1], SearchableModelMixin))
    for cls in gen:
        post_save.connect(cls.post_save_handler, sender=cls)
        pre_delete.connect(cls.pre_delete_handler, sender=cls)
