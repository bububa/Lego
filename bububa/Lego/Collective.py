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
try:
    from mmseg import mmseg
    mmseg.dict_load_defaults()
except:
    pass
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
            if debug: print 'ImportKeywords: %s, %r'%(name_hash, keyword['name'])
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
            for doc in iter(docs):
                threadPool.run(self.update, callback=self.result_collection, doc=doc, ks=ks, siteid=siteid, strict=strict, debug=debug)
                #self.update(doc, siteid, strict, debug)
                max_id = doc['ori_id']
            threadPool.killAllWorkers()
            self.save_docs(siteid, debug)
            query = self.chunk_query(max_id, chunk)
            docs = self.readdb(query, False)
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
        black_list = (u'a', u'about', u'above', u'above', u'across', u'after', u'afterwards', u'again', u'against', u'all', u'almost', u'alone', u'along', u'already', u'also','although','always','am','among', u'amongst', u'amoungst', u'amount', u'an', u'and', u'another', u'any', u'anyhow', u'anyone', u'anything', u'anyway', u'anywhere', u'are', u'around', u'as', u'at', u'back', u'be', u'became', u'because', u'become', u'becomes', u'becoming', u'been', u'before', u'beforehand', u'behind', u'being', u'below', u'beside', u'besides', u'between', u'beyond', u'bill', u'both', u'bottom','but', u'by', u'call', u'can', u'cannot', u'cant', u'co', u'con', u'could', u'couldnt', u'cry', u'de', u'describe', u'detail', u'do', u'done', u'down', u'due', u'during', u'each', u'eg', u'eight', u'either', u'eleven','else', u'elsewhere', u'empty', u'enough', u'etc', u'even', u'ever', u'every', u'everyone', u'everything', u'everywhere', u'except', u'few', u'fifteen', u'fify', u'fill', u'find', u'fire', u'first', u'five', u'for', u'former', u'formerly', u'forty', u'found', u'four', u'from', u'front', u'full', u'further', u'get', u'give', u'go', u'had', u'has', u'hasnt', u'have', u'he', u'hence', u'her', u'here', u'hereafter', u'hereby', u'herein', u'hereupon', u'hers', u'herself', u'him', u'himself', u'his', u'how', u'however', u'hundred', u'ie', u'if', u'in', u'inc', u'indeed', u'interest', u'into', u'is', u'it', u'its', u'itself', u'keep', u'last', u'latter', u'latterly', u'least', u'less', u'ltd', u'made', u'many', u'may', u'me', u'meanwhile', u'might', u'mill', u'mine', u'more', u'moreover', u'most', u'mostly', u'move', u'much', u'must', u'my', u'myself', u'name', u'namely', u'neither', u'never', u'nevertheless', u'next', u'nine', u'no', u'nobody', u'none', u'noone', u'nor', u'not', u'nothing', u'now', u'nowhere', u'of', u'off', u'often', u'on', u'once', u'one', u'only', u'onto', u'or', u'other', u'others', u'otherwise', u'our', u'ours', u'ourselves', u'out', u'over', u'own','part', u'per', u'perhaps', u'please', u'put', u'rather', u're', u'same', u'see', u'seem', u'seemed', u'seeming', u'seems', u'serious', u'several', u'she', u'should', u'show', u'side', u'since', u'sincere', u'six', u'sixty', u'so', u'some', u'somehow', u'someone', u'something', u'sometime', u'sometimes', u'somewhere', u'still', u'such', u'system', u'take', u'ten', u'than', u'that', u'the', u'their', u'them', u'themselves', u'then', u'thence', u'there', u'thereafter', u'thereby', u'therefore', u'therein', u'thereupon', u'these', u'they', u'thickv', u'thin', u'third', u'this', u'those', u'though', u'three', u'through', u'throughout', u'thru', u'thus', u'to', u'together', u'too', u'top', u'toward', u'towards', u'twelve', u'twenty', u'two', u'un', u'under', u'until', u'up', u'upon', u'us', u'very', u'via', u'was', u'we', u'well', u'were', u'what', u'whatever', u'when', u'whence', u'whenever', u'where', u'whereafter', u'whereas', u'whereby', u'wherein', u'whereupon', u'wherever', u'whether', u'which', u'while', u'whither', u'who', u'whoever', u'whole', u'whom', u'whose', u'why', u'will', u'with', u'within', u'without', u'would', u'yet', u'you', u'your', u'yours', u'yourself', u'yourselves', u'the', u'?', u'、', u'。', u'“', u'”', u'《', u'》', u'！', u'，', u'：', u'；', u'？', u'啊', u'阿', u'哎', u'哎呀', u'哎哟', u'唉', u'俺', u'俺们', u'按', u'按照', u'吧', u'吧哒', u'把', u'罢了', u'被', u'本', u'本着', u'比', u'比方', u'比如', u'鄙人', u'彼', u'彼此', u'边', u'别', u'别的', u'别说', u'并', u'并且', u'不比', u'不成', u'不单', u'不但', u'不独', u'不管', u'不光', u'不过', u'不仅', u'不拘', u'不论', u'不怕', u'不然', u'不如', u'不特', u'不惟', u'不问', u'不只', u'朝', u'朝着', u'趁', u'趁着', u'乘', u'冲', u'除', u'除此之外', u'除非', u'除了', u'此', u'此间', u'此外', u'从', u'从而', u'打', u'待', u'但', u'但是', u'当', u'当着', u'到', u'得', u'的', u'的话', u'等', u'等等', u'地', u'第', u'叮咚', u'对', u'对于', u'多', u'多少', u'而', u'而况', u'而且', u'而是', u'而外', u'而言', u'而已', u'尔后', u'反过来', u'反过来说', u'反之', u'非但', u'非徒', u'否则', u'嘎', u'嘎登', u'该', u'赶', u'个', u'各', u'各个', u'各位', u'各种', u'各自', u'给', u'根据', u'跟', u'故', u'故此', u'固然', u'关于', u'管', u'归', u'果然', u'果真', u'过', u'哈', u'哈哈', u'呵', u'和', u'何', u'何处', u'何况', u'何时', u'嘿', u'哼', u'哼唷', u'呼哧', u'乎', u'哗', u'还是', u'还有', u'换句话说', u'换言之', u'或', u'或是', u'或者', u'极了', u'及', u'及其', u'及至', u'即', u'即便', u'即或', u'即令', u'即若', u'即使', u'几', u'几时', u'己', u'既', u'既然', u'既是', u'继而', u'加之', u'假如', u'假若', u'假使', u'鉴于', u'将', u'较', u'较之', u'叫', u'接着', u'结果', u'借', u'紧接着', u'进而', u'尽', u'尽管', u'经', u'经过', u'就', u'就是', u'就是说', u'据', u'具体地说', u'具体说来', u'开始', u'开外', u'靠', u'咳', u'可', u'可见', u'可是', u'可以', u'况且', u'啦', u'来', u'来着', u'离', u'例如', u'哩', u'连', u'连同', u'两者', u'了', u'临', u'另', u'另外', u'另一方面', u'论', u'嘛', u'吗', u'慢说', u'漫说', u'冒', u'么', u'每', u'每当', u'们', u'莫若', u'某', u'某个', u'某些', u'拿', u'哪', u'哪边', u'哪儿', u'哪个', u'哪里', u'哪年', u'哪怕', u'哪天', u'哪些', u'哪样', u'那', u'那边', u'那儿', u'那个', u'那会儿', u'那里', u'那么', u'那么些', u'那么样', u'那时', u'那些', u'那样', u'乃', u'乃至', u'呢', u'能', u'你', u'你们', u'您', u'宁', u'宁可', u'宁肯', u'宁愿', u'哦', u'呕', u'啪达', u'旁人', u'呸', u'凭', u'凭借', u'其', u'其次', u'其二', u'其他', u'其它', u'其一', u'其余', u'其中', u'起', u'起见', u'起见', u'岂但', u'恰恰相反', u'前后', u'前者', u'且', u'然而', u'然后', u'然则', u'让', u'人家', u'任', u'任何', u'任凭', u'如', u'如此', u'如果', u'如何', u'如其', u'如若', u'如上所述', u'若', u'若非', u'若是', u'啥', u'上下', u'尚且', u'设若', u'设使', u'甚而', u'甚么', u'甚至', u'省得', u'时候', u'什么', u'什么样', u'使得', u'是', u'是的', u'首先', u'谁', u'谁知', u'顺', u'顺着', u'似的', u'虽', u'虽然', u'虽说', u'虽则', u'随', u'随着', u'所', u'所以', u'他', u'他们', u'他人', u'它', u'它们', u'她', u'她们', u'倘', u'倘或', u'倘然', u'倘若', u'倘使', u'腾', u'替', u'通过', u'同', u'同时', u'哇', u'万一', u'往', u'望', u'为', u'为何', u'为了', u'为什么', u'为着', u'喂', u'嗡嗡', u'我', u'我们', u'呜', u'呜呼', u'乌乎', u'无论', u'无宁', u'毋宁', u'嘻', u'吓', u'相对而言', u'像', u'向', u'向着', u'嘘', u'呀', u'焉', u'沿', u'沿着', u'要', u'要不', u'要不然', u'要不是', u'要么', u'要是', u'也', u'也罢', u'也好', u'一', u'一般', u'一旦', u'一方面', u'一来', u'一切', u'一样', u'一则', u'依', u'依照', u'矣', u'以', u'以便', u'以及', u'以免', u'以至', u'以至于', u'以致', u'抑或', u'因', u'因此', u'因而', u'因为', u'哟', u'用', u'由', u'由此可见', u'由于', u'有', u'有的', u'有关', u'有些', u'又', u'于', u'于是', u'于是乎', u'与', u'与此同时', u'与否', u'与其', u'越是', u'云云', u'哉', u'再说', u'再者', u'在', u'在下', u'咱', u'咱们', u'则', u'怎', u'怎么', u'怎么办', u'怎么样', u'怎样', u'咋', u'照', u'照着', u'者', u'这', u'这边', u'这儿', u'这个', u'这会儿', u'这就是说', u'这里', u'这么', u'这么点儿', u'这么些', u'这么样', u'这时', u'这些', u'这样', u'正如', u'吱', u'之', u'之类', u'之所以', u'之一', u'只是', u'只限', u'只要', u'只有', u'至', u'至于', u'诸位', u'着', u'着呢', u'自', u'自从', u'自个儿', u'自各儿', u'自己', u'自家', u'自身', u'综上所述', u'总的来看', u'总的来说', u'总的说来', u'总而言之', u'总之', u'纵', u'纵令', u'纵然', u'纵使', u'遵照', u'作为', u'兮', u'呃', u'呗', u'咚', u'咦', u'喏', u'啐', u'喔唷', u'嗬', u'嗯', u'嗳')
        for keyword in ks:
            hit = 0
            name = keyword['name'].lower()
            if not strict:
                try:
                    if isinstance(name, unicode): name = name.encode('utf-8')
                    algor = mmseg.Algorithm(name)
                    kk = [tok.text.decode('utf-8') for tok in algor]
                except:
                    kk = name.split()
                h = sorted([doc_content.count(k.strip()) for k in kk if k.strip() and len(k.strip()) > 1 and k.strip() not in black_list])
                if not h: continue
                hit = h[0]
            else:
                hit = doc_content.count(name)
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
        if debug: print '!IDF: keyword:%s, %r, %f'%(keyword['_id'], keyword['name'], res)
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
            for keyword in iter(ks):
                threadPool.run(self.coef, callback=self.update, keyword=keyword, debug=debug)
                #response = self.coef(keyword, debug)
                #self.update(response)
            threadPool.killAllWorkers()
            self.output = {}
        return self.output
    
    def update(self, response):
        if not response: return
        keyword_id, siteid, coefs = response
        coefs = [pickle.dumps(c).decode('utf-8') for c in coefs]
        while True:
            try:
                coefObj = KeywordCOEF.get_from_id(keyword_id)
                if not coefObj: 
                    coefObj = KeywordCOEF()
                    coefObj['_id'] = keyword_id
                coefObj['siteid'] = siteid
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
            for k in iter(ks):
                threadPool.run(self.rank, callback=self.result_collection, keyword=keyword, k=k, debug=debug)
                #response = self.rank(keyword, k, debug)
                #if not response: continue
                #self.output.append(response)
            threadPool.killAllWorkers()
        if debug: print "!COEF: Finished:%s, %r"%(keyword['_id'], keyword['name'])
        return keyword['_id'], self.siteid, self.output[keyword['_id']]
    
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
            if debug: print "!IDF: k(%s, %r), r(%s, %r) %f"%(keyword['_id'], keyword['name'], rk['_id'], rk['name'], cf)
            return keyword['_id'], {'_id':rk['_id'], 'ori_id':rk['ori_id'], 'coef':cf, 'rank':cf * rk['idf']}
        except:
            return None
            


class IntellectualError(Exception):

    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)

