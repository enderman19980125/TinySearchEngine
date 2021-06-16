from django.urls import path

from .views import *

app_name = 'TimeTable'

urlpatterns = [
    path('', view_index, name='view_index'),
    path('r/', view_result, name='view_result'),
]
