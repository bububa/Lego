#!/usr/bin/env python
# encoding: utf-8
"""
Intellectural.py

Created by Syd on 2009-11-23.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import sys
import os
import datetime
from math import log
from hashlib import md5
import time
from datetime import datetime
from itertools import groupby
try:
    import cPickle as pickle
except:
    import pickle
from yaml import YAMLObject
from bububa.Lego.Base import Base
try:
    from bububa.Lego.MongoDB import Keyword, Doc, KeywordCOEF
except:
    pass
from bububa.Lego.Helpers import ThreadPool, ConnectionPool, DatabaseConnector, Inserter, DB
from bububa.SuperMario.utils import Traceback, random_sleep


class ImportKeywords(YAMLObject, Base):
    yaml_tag = u'!ImportKeywords'
    def __init__(self, host, port, user, passwd, db, table, query, siteid, debug=None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.table = table
        self.query = query
        self.siteid = siteid
        self.debug = debug
    
    def __repr__(self):
        return "%s(host=%r, port=%r, user=%r, passwd=%r, db=%r, table=%r, query=%r)" % (self.__class__.__name__, self.host, self.port, self.user, self.passwd, self.db, self.table, self.query)
    
    def run(self):
        if hasattr(self, 'debug'): debug = self.debug
        else: debug = None
        try:
            keywords = self.readdb(True)
        except:
            if debug: raise IntellectualError('Fail to get keywords! %r'%Traceback())
        if not keywords:
            if debug: raise IntellectualError('No keywords!')
        self.output = 0
        for keyword in keywords:
            for k, v in keyword.items():
                if isinstance(v, str): keyword[k] = v.decode('utf-8')
            if hasattr(self, 'siteid') and self.siteid:
                name_hash = md5('%s:%d'%(keyword['name'].encode('utf-8').lower(), self.siteid)).hexdigest().decode('utf-8')
            else:
                name_hash = md5(keyword['name'].encode('utf-8').lower()).hexdigest().decode('utf-8')
            keywordObj = Keyword.get_from_id(name_hash)
            if keywordObj: continue
            keywordObj = Keyword()
            keywordObj['_id'] = name_hash
            keywordObj['name'] = keyword['name']
            if 'ori_id' in keyword: keywordObj['ori_id'] = int(keyword['ori_id'])
            if hasattr(self, 'siteid') and self.siteid: keywordObj['siteid'] = self.siteid
            keywordObj.save()
            if debug: print 'ImportKeywords: %s, %s'%(name_hash, keyword['name'])
            self.output += 1
        return self.output
    
    def readdb(self, iterate=None):
        con = DB(self.host, self.port, self.user, self.passwd, self.db, self.table)
        if iterate:
            return con.iterget(self.query)
        else:
            return con.get(self.query)
        

class Indexer(YAMLObject, Base):
    yaml_tag = u'!Indexer'
    def __init__(self, host, port, user, passwd, db, table, query, siteid=None, strict=None, debug=None):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.table = table
        self.query = query
        self.siteid = siteid
        self.strict = strict
        self.debug = debug
    
    def __repr__(self):
        return "%s(host=%r, port=%r, user=%r, passwd=%r, db=%r, table=%r, query=%r)" % (self.__class__.__name__, self.host, self.port, self.user, self.passwd, self.db, self.table, self.query)
    
    def run(self):
        if hasattr(self, 'debug'): debug = self.debug
        else: debug = None
        if hasattr(self, 'strict'): strict = self.strict
        else: strict = None
        if hasattr(self, 'siteid'): siteid = self.siteid
        else: siteid = None
        chunk = 500
        from_id = 0
        query = self.chunk_query(from_id, chunk)
        try:
            docs = self.readdb(query, True)
        except:
            if debug: raise IntellectualError('Fail to get docs! %r'%Traceback())
        if not docs:
            if debug: raise IntellectualError('No docs!')
        self.output = []
        if siteid: ks = list(Keyword.all({'siteid':siteid}))
        else: ks = list(Keyword.all())
        while docs:
            threadPool = ThreadPool(chunk)
            for doc in docs:
                threadPool.run(self.update, callback=self.result_collection, doc=doc, ks=ks, siteid=siteid, strict=strict, debug=debug)
                #self.update(doc, siteid, strict, debug)
                max_id = doc['ori_id']
            threadPool.killAllWorkers()
            self.save_docs(siteid, debug)
            query = self.chunk_query(max_id, chunk)
            docs = self.readdb(query, True)
        return self.output
    
    def chunk_query(self, from_id, chunk):
        if 'where' in self.query.lower(): return '%s and id> %d limit %d'%(self.query, from_id, chunk)
        else: return '%s where id> %d limit %d'%(self.query, from_id, chunk)
    
    def result_collection(self, result):
        self.output.append(result)
        
    def update(self, doc, ks, siteid, strict, debug):
        if siteid: 
            doc['_id'] = '%d:%d'%(doc['_id'], siteid)
        else:
            doc['_id'] = str(doc['_id'])
        if debug: print 'Indexer: doc=%r'%doc['_id']
        for k, v in doc.items():
            if isinstance(v, str): doc[k] = v.decode('utf-8')
        return self.tf(doc, ks, siteid, strict)
    
    def save_docs(self, siteid, debug):
        for res in self.output:
            doc, keywords, hits, tfs = res
            docObj = Doc.get_from_id(doc['_id'])
            if not docObj:
                docObj = Doc()
                docObj['_id'] = doc['_id']
            if 'ori_id' in doc: docObj['ori_id'] = int(doc['ori_id'])
            if 'vertical' in doc: docObj['vertical'] = doc['vertical']
            if siteid: docObj['siteid'] = siteid
            docObj['keywords'] = keywords
            docObj['hits'] = hits
            docObj['tfs'] = tfs
            docObj['content'] = doc['content']
            docObj['updated_at'] = datetime.utcnow()
            docObj.save()
            if debug: print 'Saved: doc=%r'%doc['_id']
        self.output = []
        
    def tf(self, doc, ks, siteid, strict):
        keywords = []
        hits = []
        tfs = []
        total = 0
        doc_content = doc['content'].lower()
        black_list = ('the', 'for', 'on', 'at', 'of', 'to', 'in')
        for keyword in ks:
            hit = 0
            if not strict:
                kk = keyword['name'].lower().split()
                h = sorted([doc_content.count(k.strip()) for k in kk if k.strip() and len(k.strip()) > 1 and k.strip() not in black_list])
                if not h: continue
                hit = h[0]
            else:
                hit = doc_content.count(keyword['name'].lower())
            if not hit: continue
            total += hit
            keywords.append(keyword['_id'])
            hits.append(hit)
        return doc, keywords, hits, [1.0 * hit/total for hit in hits]
        
    def readdb(self, query, iterate=None):
        print query
        con = DB(self.host, self.port, self.user, self.passwd, self.db, self.table)
        if iterate:
            return con.iterget(query)
        else:
            return con.get(query)
        

class IDF(YAMLObject, Base):
    yaml_tag = u'!IDF'
    def __init__(self, siteid=None, debug=None):
        self.siteid=siteid
        self.debug = debug
    
    def __repr__(self):
        return self.__class__.__name__
    
    def run(self):
        if hasattr(self,'siteid') and self.siteid: siteid = self.siteid
        else: siteid = None
        if hasattr(self,'debug') and self.debug: debug = self.debug
        else: debug = None
        self.output = None
        Ddocs = self.docs_count(siteid)
        if siteid: keywords = list(Keyword.all({'siteid':siteid}))
        max_chunk = 50
        total_workers = len(keywords)
        for i in xrange(0, total_workers, max_chunk):
            ks = keywords[i:i + max_chunk]
            threadPool = ThreadPool(len(ks))
            for keyword in ks:
                threadPool.run(self.idf, callback=self.update, keyword=keyword, Dd=Ddocs, debug=debug)
            threadPool.killAllWorkers()
        return self.output
    
    def idf(self, keyword, Dd, debug):
        while True:
            try:
                Dk = Doc.all({'keywords':keyword['_id']}).count()
                break
            except:
                random_sleep(1, 3)
        if not Dk: 
            Dk=0
            res = 0.0
        else: res = log(Dd/Dk)
        if debug: print '!IDF: keyword:%s, %s, %f'%(keyword['_id'], keyword['name'], res)
        return keyword, Dk, res
    
    def update(self, response):
        keyword, hits, idf = response
        keyword['idf'] = idf
        keyword['hits'] = hits
        keyword['udpated_at'] = datetime.utcnow()
        keyword.save()
        
    def docs_count(self, siteid):
        if siteid: return Doc.all({'siteid':siteid}).count()
        return Doc.all().count()


class COEF(YAMLObject, Base):
    yaml_tag = u'!COEF'
    def __init__(self, siteid=None, running=None, max_relations=None, debug=None):
        self.siteid = siteid
        self.running = running
        self.max_relations = max_relations
        self.debug = debug
    
    def __repr__(self):
        return self.__class__.__name__
    
    def run(self):
        self.output = {}
        if hasattr(self,'siteid') and self.siteid: siteid = self.siteid
        else: siteid = None
        if hasattr(self,'debug') and self.debug: debug = self.debug
        else: debug = None
        if siteid: keywords = ({'_id':keyword['_id'], 'name':keyword['name'], 'hits':keyword['hits']} for keyword in Keyword.all({'siteid':siteid}) if keyword['idf'])
        else: keywords = ({'_id':keyword['_id'], 'name':keyword['name'], 'hits':keyword['hits']} for keyword in Keyword.all() if keyword['idf'])
        if hasattr(self, 'running') and self.running:
            keywords = [k for k in keywords if not KeywordCOEF.get_from_id(k['_id'])]
        max_chunk = 20
        total_workers = len(keywords)
        for i in xrange(0, total_workers, max_chunk):
            ks = keywords[i:i + max_chunk]
            threadPool = ThreadPool(len(ks))
            for keyword in ks:
                threadPool.run(self.coef, callback=self.update, keyword=keyword, debug=debug)
                #response = self.coef(keyword, debug)
                #self.update(response)
            threadPool.killAllWorkers()
            self.output = {}
        return self.output
    
    def update(self, response):
        if not response: return
        keyword_id, coefs = response
        coefs = [pickle.dumps(c).decode('utf-8') for c in coefs]
        while True:
            try:
                coefObj = KeywordCOEF.get_from_id(keyword_id)
                if not coefObj: 
                    coefObj = KeywordCOEF()
                    coefObj['_id'] = keyword_id
                coefObj['keywords'].extend(coefs)
                coefObj.save()
                break
            except:
                print Traceback()
                random_sleep(1,2)
        self.output[keyword_id] = None
        self.output.pop(keyword_id)
        
    def coef(self, keyword, debug):
        while True:
            try:
                docs = reduce(list.__add__, [doc['keywords'] for doc in Doc.all({'keywords':keyword['_id']})])
                break
            except:
                random_sleep(1,2)
        keywords = [(a,len(list(b))) for a,b in groupby(sorted(docs))]
        max_chunk = 50
        total_workers = len(keywords)
        for i in xrange(0, total_workers, max_chunk):
            ks = keywords[i:i + max_chunk]
            threadPool = ThreadPool(len(ks))
            for k in ks:
                threadPool.run(self.rank, callback=self.result_collection, keyword=keyword, k=k, debug=debug)
                #response = self.rank(keyword, k, debug)
                #if not response: continue
                #self.output.append(response)
            threadPool.killAllWorkers()
        if debug: print "!COEF: Finished:%s, %s"%(keyword['_id'], keyword['name'])
        return keyword['_id'], self.output[keyword['_id']]
    
    def result_collection(self, response):
        if not response: return
        keyword_id, coef = response
        if keyword_id not in self.output: self.output[keyword_id] = []
        self.output[keyword_id].append(coef)
        if hasattr(self, 'max_relations') and self.max_relations and len(self.output[keyword_id]) > (self.max_relations + 35):
            tmp = sorted(self.output[keyword_id],cmp=lambda x,y:cmp(y['rank'],x['rank']))
            self.output[keyword_id] = tmp[:self.max_relations]
            tmp = None
        
    def rank(self, keyword, k, debug):
        keyword_id, Dab = k
        while True:
            try:
                rk = Keyword.get_from_id(keyword_id)
                break
            except:
                #if debug: print "!IDF: reconnect Dk(%d). keyword: %s"%(reconnect, keyword['_id'])
                random_sleep(1,2)
        try:
            cf = 1.0 * Dab/(keyword['hits'] + rk['hits'] - Dab)
            if debug: print "!IDF: k(%s, %s), r(%s, %s) %f"%(keyword['_id'], keyword['name'], rk['_id'], rk['name'], cf)
            return keyword['_id'], {'_id':rk['_id'], 'ori_id':rk['ori_id'], 'coef':cf, 'rank':cf * rk['idf']}
        except:
            return None
            


class IntellectualError(Exception):

    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)

