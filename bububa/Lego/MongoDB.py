#!/usr/bin/env python
# encoding: utf-8
"""
MongoDB.py

Created by Syd on 2009-11-08.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""
from settings import *
from datetime import datetime
from mongokit import Document as MongoDocument
from mongokit import INDEX_ASCENDING, INDEX_DESCENDING
from mongokit import Connection
from pymongo.errors import *

try:
    conn = Connection(MongoHost, MongoPort)
except AutoReconnect, err:
    conn = None

class SiteDocument(MongoDocument):
    structure = {
        'url': unicode,
        'effective_url': unicode,
        'url_hash': unicode,
        'etag': unicode,
        'last_modified': unicode,
        'body': unicode,
        'start_no': int,
        'end_no': int,
        'duration': int,
        'max_depth': int,
    }
    required_fields = ['url']
    indexes = [{'fields':'duration'}, {'fields':'url_hash'}]
    default_values = {'duration':10*60, 'start_no':0, 'end_no':5, 'max_depth':3}
    use_dot_notation = True
    

class PageDocument(MongoDocument):
    structure = {
        'label': unicode,
        'url': unicode,
        'effective_url': unicode,
        'url_hash': unicode,
        'domain': unicode,
        'etag': unicode,
        'last_modified': unicode,
        'failed_freq': float,
        'update_freq': float,
        'body': unicode,
        'wrapper': unicode,
        'inserted_at': datetime,
        'last_updated_at': datetime,
        'in_database': int,
        'updated_times': int,
        'rank': int
    }
    required_fields = ['url_hash']
    indexes = [{'fields':'url_hash', 'unique':True}, {'fields':[('domain', INDEX_ASCENDING), ('rank', INDEX_ASCENDING), ('in_database', INDEX_ASCENDING)]}, {'fields':'domain'}, {'fields':'in_database'}, {'fields':'rank'}]
    default_values = {'failed_freq':0.0, 'update_freq':1.0, 'updated_times':1, 'in_database':0, 'rank':30, 'inserted_at':datetime.utcnow, 'last_updated_at':datetime.utcnow}
    use_dot_notation=True


class URLTrieDocument(MongoDocument):
    structure = {
        'label': unicode,
        'url': unicode,
        'url_hash': unicode,
        'depth': int,
        'is_target': int,
        'in_database': int,
        'inserted_at': datetime
    }
    required_fields = ['url_hash']
    indexes = [{'fields':'url_hash', 'unique':True}, {'fields':[('label', INDEX_ASCENDING), ('url_hash', INDEX_ASCENDING)]}, {'fields':'label'}, {'fields':[('label', INDEX_ASCENDING), ('depth', INDEX_ASCENDING)]}, {'fields':[('label', INDEX_ASCENDING), ('depth', INDEX_ASCENDING), ('url_hash', INDEX_ASCENDING)]}, {'fields':'depth'}, {'fields':'is_target'}, {'fields':'in_database'}, {'fields':[('label', INDEX_ASCENDING), ('is_target', INDEX_ASCENDING)]}, {'fields':[('label', INDEX_ASCENDING), ('in_database', INDEX_ASCENDING)]}, {'fields':[('label', INDEX_ASCENDING), ('is_target', INDEX_ASCENDING), ('in_database', INDEX_ASCENDING)]}]
    default_values = {'inserted_at':datetime.utcnow, 'is_target':0, 'in_database':0}
    use_dot_notation=True


class KeywordDocument(MongoDocument):
    structure = {
        'name': unicode,
        'ori_id': int, 
        'idf': float,
        'hits': int,
        'searches': int,
        'sidf': float,
        'siteid': int,
        'inserted_at': datetime,
        'updated_at': datetime
    }
    required_fields = ['name']
    indexes = [{'fields':'siteid'}]
    default_values = {'idf':0.0, 'ori_id':0, 'searches':0.0, 'sidf':0.0, 'inserted_at':datetime.utcnow, 'updated_at':datetime.utcnow}
    use_dot_notation = True


class DocDocument(MongoDocument):
    structure = {
        'ori_id': int,
        'content': unicode,
        'keywords': list,
        'hits': list,
        'tfs': list, 
        'siteid': int,
        'vertical': int, 
        'inserted_at': datetime,
        'updated_at': datetime
    }
    indexes = [{'fields':['keywords', 'siteid']}, {'fields':'siteid'}, {'fields':'keywords'}]
    default_values = {'vertical':0, 'inserted_at':datetime.utcnow, 'updated_at':datetime.utcnow}
    use_dot_notation = True


class SearchDocument(MongoDocument):
    structure = {
        'ori_id': int,
        'content': unicode,
        'keywords': list,
        'hits': list,
        'tfs': list, 
        'siteid': int,
        'vertical': int, 
        'inserted_at': datetime,
        'updated_at': datetime
    }
    indexes = [{'fields':['keywords', 'siteid']}, {'fields':'siteid'}, {'fields':'keywords'}]
    default_values = {'vertical':0, 'inserted_at':datetime.utcnow, 'updated_at':datetime.utcnow}
    use_dot_notation = True


class KeywordCOEFDocument(MongoDocument):
    structure = {
        'keywords':list,
        'siteid': int
    }
    indexes = [{'fields':'siteid'}]


def reconnect():
    try:
        conn = Connection(MongoHost, MongoPort)
    except AutoReconnect, err:
        conn = None
    return conn
    

def Site(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([SiteDocument])
    return conn.crawldb.sites.SiteDocument


def Page(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([PageDocument])
    return conn.crawldb.pages.PageDocument


def URLTrie(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([URLTrieDocument])
    return conn.crawldb.urltrie.URLTrieDocument


def Keyword(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([KeywordDocument])
    return conn.collective.keywords.KeywordDocument


def Doc(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([DocDocument])
    return conn.collective.docs.DocDocument


def Search(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([SearchDocument])
    return conn.collective.searches.SearchDocument


def KeywordCOEF(conn=None):
    if not conn: conn = reconnect()
    if not conn: return None
    conn.register([KeywordCOEFDocument])
    return conn.collective.coefs.KeywordCOEFDocument


def New(obj):
    return obj()