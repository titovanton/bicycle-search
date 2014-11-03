# -*- coding: utf-8 -*-

from ..fields import StringField


class PublishedSchemaMixin(object):

    @classmethod
    def get_bulk_qs(cls):
        return cls.get_model().objects.published()

    @classmethod
    def post_save_handler(cls, instance, **kwargs):
        if instance.published:
            cls.put(instance)
        else:
            if cls.exists(instance.pk):
                cls.delete(instance)


class TitleSchemaMixin(object):
    title = StringField(boost=4)
