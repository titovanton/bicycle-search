# -*- coding: utf-8 -*-

import json
import inspect

import requests
from django.conf import settings
from django.db.models.signals import post_save
from django.db.models.signals import pre_delete

from ..fields import BaseField
from ..queryset import SearchQuerySet


INDEX_PATTERN = {
    'settings': {
        'analysis': {
            'analyzer': {
                'my_analyzer': {
                    'type': 'custom',
                    'tokenizer': 'standard',
                    'filter': ['lowercase', 'russian_morphology',
                               'english_morphology', 'my_stopwords']
                }
            },
            'filter': {
                'my_stopwords': {
                    'type': 'stop',
                    'stopwords': u'а,без,более,бы,был,была,были,было,'
                        u'быть,в,вам,вас,весь,во,вот,все,всего,всех,вы,'
                        u'где,да,даже,для,до,его,ее,если,есть,еще,же,за,'
                        u'здесь,и,из,или,им,их,к,как,ко,когда,кто,ли,'
                        u'либо,мне,может,мы,на,надо,наш,не,него,нее,нет,'
                        u'ни,них,но,ну,о,об,однако,он,она,они,оно,от,'
                        u'очень,по,под,при,с,со,так,также,такой,там,те,'
                        u'тем,то,того,тоже,той,только,том,ты,у,уже,хотя,'
                        u'чего,чей,чем,что,чтобы,чье,чья,эта,эти,это,я,'
                        u'a,an,and,are,as,at,be,but,by,for,if,in,into,'
                        u'is,it,no,not,of,on,or,such,that,the,their,'
                        u'then,there,these,they,this,to,was,will,with'
                }
            }
        }
    },
    'mappings': {}
}


class SearchSchemaBase(object):

    @classmethod
    def get_model(cls):
        return cls.model

    @classmethod
    def get_bulk_qs(cls):
        return cls.get_model().objects.all()

    @classmethod
    def get_mappings(cls):
        properties = {key: value.data for key, value in cls.get_fields()}
        mappings = {
            cls.get_type(): {
                '_all': {'analyzer': 'my_analyzer'},
                'properties': properties
            }
        }
        return mappings

    @classmethod
    def get_field(cls, name):
        field = getattr(cls, name, None)
        return isinstance(field, BaseField) and field or None

    @classmethod
    def get_fields(cls):
        return (tupl for tupl in inspect.getmembers(cls) if isinstance(tupl[1], BaseField))

    @classmethod
    def get_index(cls):
        return settings.INDEX_NAME

    @classmethod
    def get_host(cls):
        return settings.SEARCH_HOST

    @classmethod
    def get_type(cls):
        model = cls.get_model()
        return '%s_%s' % (model._meta.app_label, model.__name__.lower())

    @classmethod
    def index_exists(cls, index_name):
        """Delete if already exists"""
        url = 'http://%s/%s/' % (cls.get_host(), index_name)
        exists = requests.head(url).status_code
        if exists == 200:
            url = 'http://%s/%s/?pretty' % (cls.get_host(), index_name)
            response = requests.delete(url)
            r = response.json()
            acknowledged = r.get('acknowledged', False)
            if acknowledged:
                print 'DELETE http://%s/%s/' % (cls.get_host(), index_name), response.text
            else:
                error = r.get('error', 'unknown error')
                raise Exception(error)

    @classmethod
    def create(cls, schemas):
        """
        Creat an indeces
        """
        indices = {}
        for obj in schemas:
            index_name = obj.get_index()
            if index_name not in indices:
                cls.index_exists(index_name)
                indices[index_name] = getattr(settings, 'INDEX_PATTERN', INDEX_PATTERN).copy()
            from pprint import pprint
            pprint(obj.get_mappings())
            indices[index_name]['mappings'].update(obj.get_mappings())
        for index_name in indices:
            url = 'http://%s/%s/?pretty' % (cls.get_host(), index_name)
            response = requests.put(url, data=json.dumps(indices[index_name]))
            r = response.json()
            acknowledged = r.get('acknowledged', False)
            if acknowledged:
                print 'PUT http://%s/%s/' % (cls.get_host(), index_name), response.text
            else:
                error = r.get('error', 'unknown error')
                raise Exception(error)

    @classmethod
    def bulk(cls, schemas):
        """
        Put all datas to indeces
        """
        data = ''
        _index_pattern = '{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}\n'
        for schema in schemas:
            for item in schema.get_bulk_qs():
                data += _index_pattern % (schema.get_index(), schema.get_type(), item.pk)
                properties = {}
                for name, field in schema.get_fields():
                    value = getattr(schema, '%s_to_index' % name,
                                    lambda o: getattr(o, name))(item)
                    properties.update(field.put_pattern(name, value))
                data += json.dumps(properties) + '\n'
        url = 'http://%s/_bulk?pretty' % cls.get_host()
        response = requests.put(url, data=data)
        print 'PUT ' + url, response.text

    @classmethod
    def put(cls, obj):
        data = {}
        for name, field in cls.get_fields():
            value = getattr(cls, '%s_to_index' % name, lambda o: getattr(o, name))(obj)
            data.update(field.put_pattern(name, value))
        url = 'http://%s/%s/%s/%s' % (cls.get_host(), cls.get_index(), cls.get_type(), obj.pk)
        response = requests.put(url, data=json.dumps(data))

    @classmethod
    def exists(cls, pk):
        url = 'http://%s/%s/%s/%s' % (cls.get_host(), cls.get_index(), cls.get_type(), pk)
        return requests.head(url).status_code == 200

    @classmethod
    def delete(cls, obj):
        url = 'http://%s/%s/%s/%s' % (cls.get_host(), cls.get_index(), cls.get_type(), obj.pk)
        response = requests.delete(url)

    #############
    #  SIGNALS  #
    #############

    @classmethod
    def post_save_handler(cls, instance, **kwargs):
        cls.put(instance)

    @classmethod
    def pre_delete_handler(cls, instance, **kwargs):
        cls.delete(instance)


class QueryStringMixin(object):
    # TODO: provide query_sting with several types and indices

    @classmethod
    def search(cls, query):
        return SearchQuerySet(query, cls)


class SearchSchema(QueryStringMixin, SearchSchemaBase):
    pass


def bind_signals_for(schema):
    post_save.connect(schema.post_save_handler, sender=schema.get_model())
    pre_delete.connect(schema.pre_delete_handler, sender=schema.get_model())
