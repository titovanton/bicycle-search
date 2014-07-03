# -*- coding: utf-8 -*-


class BaseField(object):

    def __init__(self, **kwargs):
        self.data = {
            'type': self.mapping(),
            'analyzer': 'my_analyzer'
        }
        self.data.update(kwargs)

    def mapping(self):
        return 'integer'

    def put_pattern(self, name, value):
        return {name: value}

    def sort_mapping(self, name):
        return name


class StringField(BaseField):

    def __init__(self, **kwargs):
        super(StringField, self).__init__(**kwargs)
        self.data['fields'] = {
            "raw": { 
                "type":  "string",
                "index": "not_analyzed"
            }
        }

    def mapping(self):
        return 'string'

    def put_pattern(self, name, value):
        pattern = {self.sort_mapping(name): value}
        origin = super(StringField, self).put_pattern(name, value)
        origin.update(pattern)
        return origin

    def sort_mapping(self, name):
        return name + '.raw'


class IntegerField(BaseField):
    pass


class LongField(BaseField):

    def mapping(self):
        return 'long'


class FloatField(BaseField):

    def mapping(self):
        return 'float'


class DoubleField(BaseField):

    def mapping(self):
        return 'double'


class BooleanField(BaseField):

    def mapping(self):
        return 'boolean'


class NullField(BaseField):

    def mapping(self):
        return 'null'
