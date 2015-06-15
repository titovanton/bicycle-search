# Full text search based on ElasticSearch

### Settings

    # settings.py

    INSTALLED_APPS = (
        # ...

        'bicycle.search2',

        # ...
    )

    INDEX_NAME = 'project_name'
    SEARCH_HOST = 'localhost:9200'

### Schema

Create search_schema.py in your app dirrectory. For example:

    # search_schema.py
    from bicycle.search2 import fields as es_fields
    from bicycle.search2.schema import bind_signals_for
    from bicycle.search2.schema import SearchSchema
    from bicycle.search2.schema.mixins import TitleSchemaMixin

    from models import Product


    class ProductSchema(TitleSchemaMixin, SearchSchema):
        model = Product
        catalog = es_fields.StringField(boost=2)
        text = es_fields.StringField()

        # string representation for ForeignKeyField, etc.
        @classmethod
        def catalog_to_index(cls, obj):
            return obj.catalog.title

        # return qs for indexing
        # there are PublishedSchemaMixin that already contain it and the following just for example
        @classmethod
        def get_bulk_qs(cls):
            return cls.get_model().objects.published()

        # example based on get_bulk_qs behavior
        @classmethod
        def post_save_handler(cls, instance, **kwargs):
            if instance.published:
                cls.put(instance)
            else:
                if cls.exists(instance.pk):
                    cls.delete(instance)


    # every model_obj.save(), will be triggered a signal
    # which must be handled to update search index
    bind_signals_for(ProductSchema)

### Next

You have to run manage command to create new index based on schemas specified in search_schema.py

    ./manage.py searchschema

### Views

There is the SearchView that is the subclass of the ListView, wich behavior is the same of that.
Easy one time to see [the source](https://github.com/titovanton/bicycle-search/blob/master/views.py),
than read any descriptions.

### Raw usage

Each schema has a search method, which get query(string) as a single parametr and return a queryset
of a model wich specified as model attribute.

    ProductSchema.search('foo')
