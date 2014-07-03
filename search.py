# -*- coding: utf-8 -*-

import json
import inspect
from importlib import import_module
from itertools import chain

import requests
from django.conf import settings

from fields import BaseField
from fields import StringField


__all__ = ['SearchSchema', 'TitleSchemaMixin']

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
                    'stopwords': u'а,без,более,бы,был,была,были,было,'\
                        u'быть,в,вам,вас,весь,во,вот,все,всего,всех,вы,'\
                        u'где,да,даже,для,до,его,ее,если,есть,еще,же,за,'\
                        u'здесь,и,из,или,им,их,к,как,ко,когда,кто,ли,'\
                        u'либо,мне,может,мы,на,надо,наш,не,него,нее,нет,'\
                        u'ни,них,но,ну,о,об,однако,он,она,они,оно,от,'\
                        u'очень,по,под,при,с,со,так,также,такой,там,те,'\
                        u'тем,то,того,тоже,той,только,том,ты,у,уже,хотя,'\
                        u'чего,чей,чем,что,чтобы,чье,чья,эта,эти,это,я,'\
                        u'a,an,and,are,as,at,be,but,by,for,if,in,into,'\
                        u'is,it,no,not,of,on,or,such,that,the,their,'\
                        u'then,there,these,they,this,to,was,will,with'
                }
            }
        }
    },
    'mappings': {}
}


class SearchSchemaBase(object):

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
        return '%s_%s' % (cls.model._meta.app_label, cls.model.__name__.lower())

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
            pprint (obj.get_mappings())
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
            for item in schema.model.get_bulk_qs():
                data += _index_pattern % (schema.get_index(),schema.get_type(), item.pk)
                properties = {}
                for name, field in schema.get_fields():
                    v = getattr(item, name)
                    value = getattr(item, '%s_to_index' % name, lambda: v)()
                    properties.update(field.put_pattern(name, value))
                data += json.dumps(properties) + '\n'
        url = 'http://%s/_bulk?pretty' % cls.get_host()
        response = requests.put(url, data=data)
        print 'PUT ' + url, response.text

    @classmethod
    def put(cls, obj):
        data = {}
        for name, field in cls.get_fields():
            v = getattr(obj, name)
            value = getattr(obj, '%s_to_index' % name, lambda: v)()
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


class SearchQuerySetOld(object):
    """
    Works like a Django query_set
    """

    @classmethod
    def instance(cls, response, model, order):
        """
        Get instance SearchQuerySet from response object and model class object
        """
        hits = response.json()["hits"]["hits"]
        assert False
        ids = {hit['_id']: hit['_score'] for hit in hits}
        qs = model.objects.filter(pk__in=ids.keys())
        return cls(ids, qs)

    def __init__(self, ids, qs):
        self.__ids = ids
        self.__qs = qs

    def scored(self, asc=False):
        """
        return list of qs objects sorted by score from highest to lowest

        :param asc: from lowest to highest, defaults to False
        :type asc: bool
        """
        reverse = not asc
        object_list = sorted(chain(self.__qs), reverse=reverse,
                             key=lambda obj: self.__ids[str(obj.pk)])
        return object_list

    class __MethodWrapper(object):

        def __init__(self, name, qs, ids, klass):
            methods = dict(inspect.getmembers(qs.__class__, inspect.ismethod))
            if name in methods:
                self.__method = methods[name]
                self.__qs = qs
                self.__ids = ids
                self.__klass = klass
            else:
                msg = '%s has not %s method' % (qs.__class__, name)
                raise AttributeError(msg)

        def __call__(self, *args, **kwargs):
            qs = self.__method(self.__qs, *args, **kwargs)
            return self.__klass(self.__ids, qs)

    def __getattribute__(self, name):
        # hardcode
        # unfortunately a special methods like __len__ cannot be reloaded like bellow
        # this is a python speed price
        query_set_attrs = ('all', 'filter', 'exclude', 'order_by', 'distinct', 'aggregate',
                           'annotate', 'extra',)
        if name in query_set_attrs:
            return self.__MethodWrapper(name, self.__qs, self.__ids, self.__class__)
        else:
            return super(SearchQuerySet, self).__getattribute__(name)

    def __deepcopy__(self, memo):
        return self.__qs.__deepcopy__(memo)

    def __getstate__(self):
        return self.__qs.__getstate__()

    def __setstate__(self, state):
        return self.__qs.__setstate__(state)

    def __reduce__(self):
        return self.__qs.__reduce__()

    def __repr__(self):
        return self.__qs.__repr__()

    def __len__(self):
        return self.__qs.__len__()

    def __iter__(self):
        return self.__qs.__iter__()

    def __nonzero__(self):
        return self.__qs.__nonzero__()

    def __getitem__(self, key):
        return self.__qs.__getitem__(key)

    def __and__(self, other):
        return self.__qs.__and__(other)

    def __or__(self, other):
        return self.__qs.__or__(other)


class SearchQuerySet(object):

    def __init__(self, query, schema):
        self.query = query
        self.schema = schema
        self.sort = None
        self.cache = None

    def __get_query(self):
        p = {
            'query': {
                'query_string': {
                    'query': self.query
                }, 
                'analyze_wildcard': True,
                '_source': False
            }
        }
        if self.sort is not None:
            p['sort'] = []
            for field_name in self.sort:
                desc = False
                if field_name[0] == '-':
                    desc = True
                    field_name = field_name[1:]
                if field_name == '_score':
                    desc = not desc
                if field_name != '_score':
                    field = self.schema.get_field(field_name)
                    if field is not None:
                        field_name = field.sort_mapping(field_name)
                    else:
                        field_name = None
                if field_name is not None:
                    p['sort'] += [{field_name: {'order': desc and 'desc' else 'asc'}}]
        return p

    def __get_host(self):
        return self.schema.get_host()

    def __get_index(self):
        return self.schema.get_index()

    def __get_type(self):
        return self.schema.get_type()

    def __get_url(self):
        url = u'http://%s/%s/%s/_search' % (self.__get_host(), self.__get_index(), 
                                            self.__get_type())
        if self.__size and self.__page:
            url += '?size=%d,from=%d' % (self.__size, (self.__page-1) * self.__size)
        return url

    def order_by(self, *args):
        self.sort = args


class QueryStringMixin(object):
    # TODO: provide query_sting with several types and indices
    @classmethod
    def get_query_pattern(cls, query, sort=None):
        p = {
            'query': {
                'query_string': {
                    'query': query
                }, 
                'analyze_wildcard': True,
                '_source': False
            }
        }
        if sort is not None:
            if sort[0] == '-':
                p['sort'] = {
                    sort[1:]: {
                        'order': 'desc'
                    }
                }
            else:
                p['sort'] = {
                    sort: {
                        'order': 'asc'
                    }
                }
        return p

    @classmethod
    def search(cls, query, size=10, page=1, order=None):
        """
        Search in index

        :param size: how many results should be returned, defaults to 10
        :type size: int starts by 1

        :param page: page number
        :type page: int starts by 1

        :param order: field of schema and model for witch you want to order, defaults to None
        :type order: string

        :rtype: list if parameter order set to 'score'('-score'), but SearchQuerySet if 
            parameter order set to None or some schema field name

        .. note::
            - If you set order parameter to None, first size of items will be highest score,
            becouse score desc is default ordering for index search

        .. note::
            - You can pass order parameter with minus as first character, in Django style
            and expect reverse ordering (DESC)

        .. note::
            - You have to specify fields in SearchSchema, for witch you want to order, otherwise
            first 10 items will be ordered by score, not by field you want to.
        """
        url = u'http://%s/%s/%s/_search' % (cls.get_host(), cls.get_index(), cls.get_type())
        url += '?size=%d,from=%d' % (size, (page-1)*size)
        data = cls.get_query_pattern(query, order)
        response = requests.post(url, data=json.dumps(data))
        return SearchQuerySetOld.instance(response, cls.model, order)


class SearchSchema(SearchSchemaBase, QueryStringMixin):
    pass


class TitleSchemaMixin(SearchSchema):
    title = StringField(boost=4)
