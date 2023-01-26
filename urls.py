from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('mongodb/', views.mongodb ,name='mongodb'),
    path('mongodb/ls/', views.mongodb_ls, name='mongdb_ls'),
    path('mongodb/mkdir/', views.mongodb_mkdir, name='mongdb_mkdir'),
    path('mongodb/cat/', views.mongodb_cat, name='mongdb_cat'),
    path('mongodb/rm/', views.mongodb_rm, name='mongdb_rm'),
    path('mongodb/put/', views.mongodb_put, name='mongdb_put'),
    path('mongodb/search/', views.mongodb_search, name='mongdb_search'),
    path('mongodb/analyze/', views.mongodb_analyze, name='mongdb_analyze'),
    path('firebase/', views.firebase ,name='firebase'),
    path('firebase/ls/', views.firebase_ls, name='firebase_ls'),
    path('firebase/mkdir/', views.firebase_mkdir, name='firebase_mkdir'),
    path('firebase/cat/', views.firebase_cat, name='firebase_cat'),
    path('firebase/rm/', views.firebase_rm, name='firebase_rm'),
    path('firebase/put/', views.firebase_put, name='firebase_put'),
    path('mysql/', views.mysql ,name='mysql'),
    path('mysql/ls/', views.mysql_ls, name='fmysql_ls'),
    path('mysql/mkdir/', views.mysql_mkdir, name='mysql_mkdir'),
    path('mysql/cat/', views.mysql_cat, name='mysql_cat'),
    path('mysql/rm/', views.mysql_rm, name='mysql_rm'),
    path('mysql/put/', views.mysql_put, name='mysql_put'),
]