# -*- coding: utf-8 -*-

import json
from itertools import chain

import requests
from django.conf import settings
from django.utils import six


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
        self.__defer = None

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

                try:
                    if field_name[0] == '-':
                        desc = True
                        field_name = field_name[1:]
                except IndexError:
                    field_name = None

                if field_name == '_score':
                    desc = not desc

                if field_name != '_score' and field_name is not None:
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
        self.__cache = self.__schema.get_model().objects.filter(pk__in=self.opts['score'].keys())
        sort = self.opts.get('sort', [])

        if sort and '_score' not in sort and '-_score' not in sort:
            self.__cache = self.__cache.order_by(*sort)

        if self.__defer is not None:
            self.__cache = self.__cache.defer(*self.__defer)

        if not sort or '_score' in sort or '-_score' in sort:
            self.__cache = self.__scored()

    def __scored(self):
        return sorted(chain(self.__cache), reverse=True,
                      key=lambda obj: self.opts['score'][str(obj.pk)])

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

    def defer(self, *args):
        """
        Works like Django defer.

        :param args: model fields
        :type args: unpacked list of unnamed strings
        """
        obj = self.__clone()
        obj.__defer = args

        return obj

    ####################
    #  LIST EMULATION  #
    ####################

    def __repr__(self):

        if self.__cache is None:
            return u'<%sSearchQuerySet: [%d uncached item(s)]>' % (
                self.__schema.get_model().__name__, self.__len__())
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
