from django.urls import path

from .views import *

app_name = 'TimeTable'

urlpatterns = [
    path('', view_index, name='view_index'),
    path('search/', view_search, name='view_search'),
    path('api_search_by_keyword/', api_search_by_keyword, name='view_search'),
]
