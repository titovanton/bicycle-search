# coding: UTF-8

import inspect
import importlib

from django.db.models import get_app
from django.db.models import get_models
from django.conf import settings
from django.core.management.base import BaseCommand
from django.core.management.base import CommandError
from bicycle.search2 import SearchSchema


def isschema(value):
    return inspect.isclass(value) and issubclass(value, SearchSchema)


class Command(BaseCommand):
    def handle(self, *args, **options):
        not_found = []
        for app_label in settings.INSTALLED_APPS:
            try:
                module = importlib.import_module(app_label+'.search_schema')
                members = dict(inspect.getmembers(module, isschema)).values()
                schemas = [v for v in members if inspect.getmodule(v) is module]
                if schemas:
                    schemas[0].create(schemas)
                    schemas[0].bulk(schemas)
                    print u'++ %s: %s schemas found' % (app_label, len(schemas))
                else:
                    not_found += [app_label]
            except ImportError:
                not_found += [app_label]
        if not_found:
            print u'-- following apps has no schemas: %s' % (', '.join(not_found))
