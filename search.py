# -*- coding: utf-8 -*-

import json
import inspect
from importlib import import_module
from itertools import chain

import requests
from django.conf import settings
from django.utils import six

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


class SearchException(Exception):
    pass


class SearchQuerySet(object):
    # TODO: filter
    """
    Search query set in Django style
    """

    def __init__(self, query, schema):
        """
        Basic initial

        :param query: search query
        :type query: string

        :param schema: search schema
        :type schema: subclass of SearchSchemaBase
        """
        self.__count = None
        self.__query = query
        self.__schema = schema
        self.__cache = None
        # query options such as limit, page, sort, etc...
        self.opts = {}

    #####################
    #  PRIVATE METHODS  #
    #####################

    # depends on opts
    def __get_query(self, count=False):
        """
        Generate query data for ElasticSearch request

        .. note::
            ElasticSearch API does not support counting while sorting, that's why we needs
            in count flag

        :param count: flag, defaults to False
        :type count: bool
        """
        p = {
            'query': {
                'query_string': {
                    'query': self.__query
                }, 
                'analyze_wildcard': True,
                '_source': False
            }
        }
        if self.opts.get('sort', False) and not count:
            p['sort'] = []
            for field_name in self.opts['sort']:
                desc = False
                if field_name[0] == '-':
                    desc = True
                    field_name = field_name[1:]
                if field_name == '_score':
                    desc = not desc
                if field_name != '_score':
                    field = self.__schema.get_field(field_name)
                    if field is not None:
                        field_name = field.sort_mapping(field_name)
                    else:
                        field_name = None
                if field_name is not None:
                    p['sort'] += [{field_name: desc and 'desc' or 'asc'}]
        return p

    # depends on opts
    def __fill_cache(self):
        url = self.__get_url()
        query = self.__get_query()
        response = requests.post(url, data=json.dumps(query))
        jsn = response.json()
        if jsn.get('status', 200) == 404:
            raise SearchException(jsn['error'])
        hits = jsn["hits"]["hits"]
        self.__count = int(jsn['hits']['total'])
        self.opts['score'] = {hit['_id']: hit['_score'] for hit in hits}
        self.__cache = self.__schema.model.objects.filter(pk__in=self.opts['score'].keys())
        if self.opts.get('sort', False):
            if '_score' not in self.opts['sort'] and '-_score' not in self.opts['sort']:
                self.__cache = self.__cache.order_by(*self.opts['sort'])
            else:
                self.__scored()

    def __clone(self):
        obj = self.__class__(self.__query, self.__schema)
        obj.opts = self.opts.copy()
        return obj

    # depends on opts
    def __len(self):
        if self.opts.get('slice', False):
            return self.__get_size()
        ptrn = (self.__get_host(), self.__get_index(), self.__get_type())
        url = u'http://%s/%s/%s/_count' % ptrn
        query = self.__get_query(True)
        response = requests.post(url, data=json.dumps(query))
        jsn = response.json()
        if jsn.get('status', 200) == 404:
            raise SearchException(jsn['error'])
        return int(jsn['count'])

    def __get_host(self):
        return self.__schema.get_host()

    def __get_index(self):
        return self.__schema.get_index()

    def __get_type(self):
        return self.__schema.get_type()

    def __get_from(self):
        if self.opts.get('slice', False):
            return self.opts['slice'].start
        else:
            return 0

    def __get_size(self):
        if self.opts.get('slice', False):
            start = self.__get_from()
            stop = self.opts['slice'].stop
            return stop - start
        else:
            return self.__len__()

    def __get_url(self):
        ptrn = (self.__get_host(), self.__get_index(), self.__get_type(), 
                self.__get_size(), self.__get_from())
        return u'http://%s/%s/%s/_search?size=%d&from=%d' % ptrn

    ###################
    #  CHAIN METHODS  #
    ###################

    def order_by(self, *args):
        # TODO: multifields sort with _score
        # TODO: lookups
        """
        Works like Django order_by.

        :param args: schema(model) ordered fields
        :type args: unpacked list of unnamed strings


        - You can pass order parameter with minus as first character, in Django style
        and expect reverse ordering (DESC)
        - You have to specify fields in SearchSchema, for witch you want to order, otherwise
        first 10 items(default size of search result) will be ordered by score, not by field 
        you want to.
        """
        obj = self.__clone()
        obj.opts['sort'] = args
        return obj

    ####################
    #  LIST EMULATION  #
    ####################

    def __repr__(self):
        if self.__cache is None:
            return u'<%sSearchQuerySet: [%d uncached item(s)]>' % (self.__schema.model.__name__,
                                                                   self.__len__())
        else:
            return repr(self.__cache)

    def __len__(self):
        if self.__count is None:
            self.__count = self.__len()
        return self.__count

    def __getitem__(self, k):
        # TODO: slice step support
        """
        Retrieves an item or slice from the set of results.
        """
        if not isinstance(k, (slice,) + six.integer_types):
            raise TypeError
        assert ((not isinstance(k, slice) and (k >= 0)) or
                (isinstance(k, slice) and (k.start is None or k.start >= 0) and
                 (k.stop is None or k.stop >= 0))), \
            "Negative indexing is not supported."
        if isinstance(k, slice):
            obj = self.__clone()
            if k.start is not None:
                start = int(k.start)
            else:
                start = 0
            if k.step is not None:
                step = int(k.step)
            else:
                step = 1
            if k.stop is not None:
                stop = int(k.stop)
            else:
                stop = self.__len__()
            if start > self.__len__() or stop > self.__len__():
                raise IndexError('slice out of list range')
            obj.opts['slice'] = slice(start, stop, step)
            return obj
        else:
            if k > self.__len__():
                raise IndexError('list index out of range')
            if self.__cache is None:
                self.__fill_cache()
            return self.__cache[k]

    def __iter__(self):
        if self.__cache is None:
            self.__fill_cache()
        return iter(self.__cache)


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


class QueryStringMixin(object):
    # TODO: provide query_sting with several types and indices
    @classmethod
    def search(cls, query):
        return SearchQuerySet(query, cls)


class SearchSchema(SearchSchemaBase, QueryStringMixin):
    pass


class TitleSchemaMixin(SearchSchema):
    title = StringField(boost=4)
