#!/usr/bin/env python
# encoding: utf-8
"""
MongoDB.py

Created by Syd on 2009-11-08.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""
from datetime import datetime
from mongokit import *

class Site(MongoDocument):
    db_name = 'crawldb'
    collection_name = 'sites'
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
    
    
class Page(MongoDocument):
    db_name = 'crawldb'
    collection_name = 'pages'
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
    indexes = [{'fields':'url_hash', 'unique':True}, {'fields':['domain', 'rank', 'in_database']}, {'fields':'domain'}, {'fields':'in_database'}, {'fields':'rank'}]
    default_values = {'failed_freq':0.0, 'update_freq':1.0, 'updated_times':1, 'in_database':0, 'rank':30, 'inserted_at':datetime.utcnow, 'last_updated_at':datetime.utcnow}
    use_dot_notation=True


class Keyword(MongoDocument):
    db_name = 'intellectual'
    collection_name = 'keywords'
    structure = {
        'name': unicode,
        'ori_id': int, 
        'idf': float,
        'hits': int,
        'siteid': int, 
        'inserted_at': datetime,
        'updated_at': datetime
    }
    required_fields = ['name']
    indexes = [{'fields':'siteid'}]
    default_values = {'idf':0.0, 'ori_id':0, 'inserted_at':datetime.utcnow, 'updated_at':datetime.utcnow}
    use_dot_notation = True


class Doc(MongoDocument):
    db_name = 'intellectual'
    collection_name = 'docs'
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

class KeywordCOEF(MongoDocument):
    db_name = 'intellectual'
    collection_name = 'coefs'
    structure = {
        'keywords':list,
        'siteid': int
    }
    indexes = [{'fields':'siteid'}]