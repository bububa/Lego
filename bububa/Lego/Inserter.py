#!/usr/bin/env python
# encoding: utf-8
"""
Inserter.py

Created by Syd on 2009-11-18.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

from hashlib import md5
from datetime import datetime
try:
    import cPickle as pickle
except:
    import pickle
import simplejson
from yaml import YAMLObject
from bububa.Lego.Base import Base
try:
    from bububa.Lego.MongoDB import Page, Keyword, KeywordCOEF
except:
    pass
from bububa.Lego.Helpers import Inserter
from bububa.SuperMario.utils import Traceback


class BaseInserter(YAMLObject, Base):
    yaml_tag = u'!BaseInserter'
    def __init__(self, starturl, host, port, user, passwd, db, table, data, callback=None, debug=None):
        self.starturl = starturl
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.table = table
        self.data = data
        self.callback = callback
        self.debug = debug
    
    def __repr__(self):
        return "%s(starturl=%r, host=%r, port=%r, user=%r, passwd=%r, db=%r, table=%r)" %(self.__class__.__name__, self.starturl, self.host, self.port, self.user, self.passwd, self.db, self.table)
    
    def run(self):
        self.iterate_callables(exceptions='callback')
        inserter = Inserter(self.host, self.port, self.user, self.passwd, self.db, self.table)
        self.output = inserter.insert(self.data)
        try:
            self.callback.run()
        except:
            if hasattr(self, 'debug') and self.debug:
                raise InserterError("!BaseInserter: failed during callback.\n%r"%Traceback())
        return self.output


class IterateInserter(YAMLObject, Base):
    yaml_tag = u'!IterateInserter'
    def __init__(self, host, port, user, passwd, db, table, keys=None, callback=None, debug=None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.table = table
        self.keys = keys
        self.callback = callback
        self.debug = debug
    
    def __repr__(self):
        return "%s(host=%r, port=%r, user=%r, passwd=%r, db=%r, table=%r)" %(self.__class__.__name__, self.host, self.port, self.user, self.passwd, self.db, self.table)
    
    def run(self):
        self.iterate_callables(exceptions='callback')
        self.output = []
        for page in Page.all({'in_database':0}):
            inserter = Inserter(self.host, self.port, self.user, self.passwd, self.db, self.table)
            data = page['wrapper']
            data['url'] = page['url']
            data['effective_url'] = page['effective_url']
            data['inserted_at'] = page['inserted_at']
            data['last_updated_at'] = page['last_updated_at']
            data['updated_times'] = page['updated_times']
            data['rank'] = page['rank']
            data['label'] = page['label']
            data['url_hash'] = page['url_hash']
            if hasattr(self, 'keys') and self.keys.has_key('mongo_keys'):
                if self.keys.has_key('db_keys'):
                    data = [{d:data[m]} for m, d in zip(self.keys['mongo_keys'], self.keys['db_keys']) if m in data]
                else:
                    data = [data[k] for k in self.keys['mongo_keys'] if k in data]
            
            try:
                last_id = inserter.insert(data)
            except:
                try:
                    last_id = inserter.update(data, "url_hash='%s'"%data['url_hash'])
                except:
                    last_id = None
            if last_id:
                self.output.append(last_id)
                page['in_database'] = last_id
                page.save()
            elif hasattr(self, 'debug') and self.debug:
                raise InserterError("!IterateInserter: failed to insert.\n%r"%data)
        try:
            self.callback.run()
        except:
            if hasattr(self, 'debug') and self.debug:
                raise InserterError("!IterateInserter: failed during callback.\n%r"%Traceback())
        return self.output

class IDFUpdater(YAMLObject, Base):
    yaml_tag = u'!IDFUpdater'
    def __init__(self, host, port, user, passwd, db, table, siteid, vertical=None, column=None, based_on_id=None, debug=None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.table = table
        self.siteid = siteid
        self.vertical = vertical
        self.column = column
        self.based_on_id = based_on_id
        self.debug = debug

    def __repr__(self):
        return "%s(host=%r, port=%r, user=%r, passwd=%r, db=%r, table=%r)" %(self.__class__.__name__, self.host, self.port, self.user, self.passwd, self.db, self.table)

    def run(self):
        self.iterate_callables()
        self.output = 0
        if hasattr(self,'siteid') and self.siteid: siteid = self.siteid
        else: siteid = None
        if hasattr(self,'debug') and self.debug: debug = self.debug
        else: debug = None
        if hasattr(self, 'vertical'): vertical = self.vertical
        else: vertical = 0
        if hasattr(self, 'column'): column = self.column
        else: column = None
        if hasattr(self, 'based_on_id'): based_on_id = self.based_on_id
        else: based_on_id = None
        keywords = [{'ori_id':keyword['ori_id'], 'name':keyword['name'], 'idf':keyword['idf']} for keyword in Keyword.all({'siteid':siteid})]
        for keyword in iter(keywords):
            try:
                self.save(keyword, vertical, column, based_on_id, debug)
            except:
                continue
        return self.output

    def save(self, keyword, vertical, column, based_on_id, debug):
        inserter = Inserter(self.host, self.port, self.user, self.passwd, self.db, self.table)
        name_hash = md5(keyword['name'].encode('utf-8').lower()).hexdigest()
        if based_on_id:
            where = 'id="%s"'%keyword['ori_id']
        else:
            where = 'name_hash="%s"'%name_hash
        if column:
            inserter.update({column:keyword['idf']}, where)
        else:
            inserter.update({'idf%d'%vertical:keyword['idf']}, where)
        self.output += 1
        if debug: print "!IDFUpdater: Updateded:%s"%name_hash  


class COEFInserter(YAMLObject, Base):
    yaml_tag = u'!COEFInserter'
    def __init__(self, host, port, user, passwd, db, table, siteid, clientid=None, vertical=None, debug=None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.table = table
        self.siteid = siteid
        self.clientid = clientid
        self.vertical = vertical
        self.debug = debug

    def __repr__(self):
        return "%s(host=%r, port=%r, user=%r, passwd=%r, db=%r, table=%r)" %(self.__class__.__name__, self.host, self.port, self.user, self.passwd, self.db, self.table)

    def run(self):
        self.iterate_callables()
        self.output = 0
        if hasattr(self,'siteid') and self.siteid: siteid = self.siteid
        else: siteid = None
        if hasattr(self,'clientid') and self.clientid: clientid = self.clientid
        else: clientid = siteid
        if hasattr(self,'debug') and self.debug: debug = self.debug
        else: debug = None
        if hasattr(self, 'vertical'): vertical = self.vertical
        else: vertical = 0
        for keywordCoef in KeywordCOEF.all({'siteid':siteid}):
            keyword = Keyword.get_from_id(keywordCoef['_id'])
            tmp_keywords = [pickle.loads(coef.encode('utf-8')) for coef in keywordCoef['keywords']]
            tmp_keywords = sorted(tmp_keywords,cmp=lambda x,y:cmp(y['rank'],x['rank']))
            related_keywords = []
            for k in tmp_keywords[0:10]:
                rk = Keyword.get_from_id(k['_id'])
                related_keywords.append({'id':rk['ori_id'], 'name':rk['name'], 'coef':k['coef'], 'rank':k['rank']})
            if not related_keywords: continue
            try:
                self.save(md5(keyword['name'].encode('utf-8').lower()).hexdigest(), keyword['ori_id'], simplejson.dumps(related_keywords), vertical, debug)
            except:
                continue
        self.save(md5('').hexdigest(), 0, simplejson.dumps(self.top10(siteid)), vertical, debug)
        return self.output

    def top10(self, siteid):
        keywords = [{'name':keyword['name'], 'ori_id':keyword['ori_id'], 'idf':keyword['idf']} for keyword in Keyword.all({'siteid':siteid}) if keyword['idf']>0]
        keywords = sorted(keywords, cmp=lambda x,y:cmp(x['idf'], y['idf']))
        return [{'name':k['name'], 'idf':k['idf'], 'id':k['ori_id']} for k in keywords[0:10]]

    def save(self, name_hash, keyword_id, json, vertical, debug):
        inserter = Inserter(self.host, self.port, self.user, self.passwd, self.db, self.table)
        try:
            inserter.insert({'name_hash':name_hash, 'keyword_id':keyword_id, 'json':json, 'vertical':vertical})
        except Exception, err:
            #inserter.update({'name_hash':name_hash, 'keyword_id':keyword_id, 'json':json, 'vertical':vertical}, 'name_hash="%s" AND vertical=%d'%(name_hash, vertical))
            pass
        self.output += 1
        if debug: print "!COEFInserter: Inserted:%s"%name_hash


class COEFUpdater(YAMLObject, Base):
    yaml_tag = u'!COEFUpdater'
    def __init__(self, host, port, user, passwd, db, table, siteid, debug=None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.table = table
        self.siteid = siteid
        self.debug = debug

    def __repr__(self):
        return "%s(host=%r, port=%r, user=%r, passwd=%r, db=%r, table=%r)" %(self.__class__.__name__, self.host, self.port, self.user, self.passwd, self.db, self.table)

    def run(self):
        self.iterate_callables()
        self.output = 0
        if hasattr(self,'siteid') and self.siteid: siteid = self.siteid
        else: siteid = None
        if hasattr(self,'debug') and self.debug: debug = self.debug
        else: debug = None
        for keywordCoef in KeywordCOEF.all({'siteid':siteid}):
            keyword = Keyword.get_from_id(keywordCoef['_id'])
            tmp_keywords = [pickle.loads(coef.encode('utf-8')) for coef in keywordCoef['keywords']]
            tmp_keywords = sorted(tmp_keywords,cmp=lambda x,y:cmp(y['rank'],x['rank']))
            related_keywords = []
            for k in tmp_keywords[0:10]:
                rk = Keyword.get_from_id(k['_id'])
                related_keywords.append({'ori_id':rk['ori_id'], 'name':rk['name'], 'coef':k['coef'], 'rank':k['rank']})
            if not related_keywords: continue
            try:
                self.save(keyword, related_keywords, debug)
            except:
                continue
        return self.output

    def save(self, keyword, related_keywords, debug):
        inserter = Inserter(self.host, self.port, self.user, self.passwd, self.db, self.table)
        name_hash = md5(keyword['name'].encode('utf-8').lower()).hexdigest()
        try:
            inserter.remove('keyword_id=%d'%keyword['ori_id'])
            for k in related_keywords:
                try:
                    inserter.insert({'keyword_id':keyword['ori_id'], 'related_keyword_id': k['ori_id'], 'coefficient':k['coef'], 'rank':k['rank'])
                except:
                    continue
        except:
            return
        self.output += 1
        if debug: print "!COEFUpdater: Updateded:%s"%name_hash


class InserterError(Exception):

    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)