# -*- coding: utf-8 -*-

from django import forms


class SearchForm(forms.Form):
    q = forms.CharField(max_length=120, required=False)
