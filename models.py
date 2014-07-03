# -*- coding: utf-8 -*-

import sys
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
        if instance.published:
            cls.SearchSchema.put(instance)
        else:
            if cls.SearchSchema.exists(instance.pk):
                cls.SearchSchema.delete(instance)


def search_trigger(name):
    module = sys.modules[name]
    for obj_name in dir(module):
        obj = getattr(module, obj_name)
        if isinstance(obj, SearchableModelMixin):
            post_save.connect(obj.post_save_handler, sender=obj)
            pre_delete.connect(obj.pre_delete_handler, sender=obj)
