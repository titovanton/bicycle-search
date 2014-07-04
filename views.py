# -*- coding: utf-8 -*-

from django.views.generic import ListView

from forms import SearchForm


class SearchView(ListView):
    paginate_by = 10
    query = None
    schema = None
    search_form = SearchForm
    template_name = 'search2/object_list.html'

    def get_queryset(self):
        form = self.search_form(self.request.GET)
        if form.is_valid():
            self.query = form.cleaned_data['q']
            return self.schema.search(self.query)
        else:
            return []

    def get_context_data(self, *args, **kwargs):
        context = super(SearchView, self).get_context_data(*args, **kwargs)
        context.update({
            'search_query': self.query,
            # 'result_objects': context['object_list'],
        })
        return context
