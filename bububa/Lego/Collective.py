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
            if debug: print 'ImportKeywords: %s, %s'%(name_hash, keyword['name'].encode('utf-8'))
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
        black_list = ('a', 'about', 'above', 'above', 'across', 'after', 'afterwards', 'again', 'against', 'all', 'almost', 'alone', 'along', 'already', 'also','although','always','am','among', 'amongst', 'amoungst', 'amount',  'an', 'and', 'another', 'any','anyhow','anyone','anything','anyway', 'anywhere', 'are', 'around', 'as',  'at', 'back','be','became', 'because','become','becomes', 'becoming', 'been', 'before', 'beforehand', 'behind', 'being', 'below', 'beside', 'besides', 'between', 'beyond', 'bill', 'both', 'bottom','but', 'by', 'call', 'can', 'cannot', 'cant', 'co', 'con', 'could', 'couldnt', 'cry', 'de', 'describe', 'detail', 'do', 'done', 'down', 'due', 'during', 'each', 'eg', 'eight', 'either', 'eleven','else', 'elsewhere', 'empty', 'enough', 'etc', 'even', 'ever', 'every', 'everyone', 'everything', 'everywhere', 'except', 'few', 'fifteen', 'fify', 'fill', 'find', 'fire', 'first', 'five', 'for', 'former', 'formerly', 'forty', 'found', 'four', 'from', 'front', 'full', 'further', 'get', 'give', 'go', 'had', 'has', 'hasnt', 'have', 'he', 'hence', 'her', 'here', 'hereafter', 'hereby', 'herein', 'hereupon', 'hers', 'herself', 'him', 'himself', 'his', 'how', 'however', 'hundred', 'ie', 'if', 'in', 'inc', 'indeed', 'interest', 'into', 'is', 'it', 'its', 'itself', 'keep', 'last', 'latter', 'latterly', 'least', 'less', 'ltd', 'made', 'many', 'may', 'me', 'meanwhile', 'might', 'mill', 'mine', 'more', 'moreover', 'most', 'mostly', 'move', 'much', 'must', 'my', 'myself', 'name', 'namely', 'neither', 'never', 'nevertheless', 'next', 'nine', 'no', 'nobody', 'none', 'noone', 'nor', 'not', 'nothing', 'now', 'nowhere', 'of', 'off', 'often', 'on', 'once', 'one', 'only', 'onto', 'or', 'other', 'others', 'otherwise', 'our', 'ours', 'ourselves', 'out', 'over', 'own','part', 'per', 'perhaps', 'please', 'put', 'rather', 're', 'same', 'see', 'seem', 'seemed', 'seeming', 'seems', 'serious', 'several', 'she', 'should', 'show', 'side', 'since', 'sincere', 'six', 'sixty', 'so', 'some', 'somehow', 'someone', 'something', 'sometime', 'sometimes', 'somewhere', 'still', 'such', 'system', 'take', 'ten', 'than', 'that', 'the', 'their', 'them', 'themselves', 'then', 'thence', 'there', 'thereafter', 'thereby', 'therefore', 'therein', 'thereupon', 'these', 'they', 'thickv', 'thin', 'third', 'this', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'to', 'together', 'too', 'top', 'toward', 'towards', 'twelve', 'twenty', 'two', 'un', 'under', 'until', 'up', 'upon', 'us', 'very', 'via', 'was', 'we', 'well', 'were', 'what', 'whatever', 'when', 'whence', 'whenever', 'where', 'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which', 'while', 'whither', 'who', 'whoever', 'whole', 'whom', 'whose', 'why', 'will', 'with', 'within', 'without', 'would', 'yet', 'you', 'your', 'yours', 'yourself', 'yourselves', 'the', '?', '、', '。', '“', '”', '《', '》', '！', '，', '：', '；', '？', '啊', '阿', '哎', '哎呀', '哎哟', '唉', '俺', '俺们', '按', '按照', '吧', '吧哒', '把', '罢了', '被', '本', '本着', '比', '比方', '比如', '鄙人', '彼', '彼此', '边', '别', '别的', '别说', '并', '并且', '不比', '不成', '不单', '不但', '不独', '不管', '不光', '不过', '不仅', '不拘', '不论', '不怕', '不然', '不如', '不特', '不惟', '不问', '不只', '朝', '朝着', '趁', '趁着', '乘', '冲', '除', '除此之外', '除非', '除了', '此', '此间', '此外', '从', '从而', '打', '待', '但', '但是', '当', '当着', '到', '得', '的', '的话', '等', '等等', '地', '第', '叮咚', '对', '对于', '多', '多少', '而', '而况', '而且', '而是', '而外', '而言', '而已', '尔后', '反过来', '反过来说', '反之', '非但', '非徒', '否则', '嘎', '嘎登', '该', '赶', '个', '各', '各个', '各位', '各种', '各自', '给', '根据', '跟', '故', '故此', '固然', '关于', '管', '归', '果然', '果真', '过', '哈', '哈哈', '呵', '和', '何', '何处', '何况', '何时', '嘿', '哼', '哼唷', '呼哧', '乎', '哗', '还是', '还有', '换句话说', '换言之', '或', '或是', '或者', '极了', '及', '及其', '及至', '即', '即便', '即或', '即令', '即若', '即使', '几', '几时', '己', '既', '既然', '既是', '继而', '加之', '假如', '假若', '假使', '鉴于', '将', '较', '较之', '叫', '接着', '结果', '借', '紧接着', '进而', '尽', '尽管', '经', '经过', '就', '就是', '就是说', '据', '具体地说', '具体说来', '开始', '开外', '靠', '咳', '可', '可见', '可是', '可以', '况且', '啦', '来', '来着', '离', '例如', '哩', '连', '连同', '两者', '了', '临', '另', '另外', '另一方面', '论', '嘛', '吗', '慢说', '漫说', '冒', '么', '每', '每当', '们', '莫若', '某', '某个', '某些', '拿', '哪', '哪边', '哪儿', '哪个', '哪里', '哪年', '哪怕', '哪天', '哪些', '哪样', '那', '那边', '那儿', '那个', '那会儿', '那里', '那么', '那么些', '那么样', '那时', '那些', '那样', '乃', '乃至', '呢', '能', '你', '你们', '您', '宁', '宁可', '宁肯', '宁愿', '哦', '呕', '啪达', '旁人', '呸', '凭', '凭借', '其', '其次', '其二', '其他', '其它', '其一', '其余', '其中', '起', '起见', '起见', '岂但', '恰恰相反', '前后', '前者', '且', '然而', '然后', '然则', '让', '人家', '任', '任何', '任凭', '如', '如此', '如果', '如何', '如其', '如若', '如上所述', '若', '若非', '若是', '啥', '上下', '尚且', '设若', '设使', '甚而', '甚么', '甚至', '省得', '时候', '什么', '什么样', '使得', '是', '是的', '首先', '谁', '谁知', '顺', '顺着', '似的', '虽', '虽然', '虽说', '虽则', '随', '随着', '所', '所以', '他', '他们', '他人', '它', '它们', '她', '她们', '倘', '倘或', '倘然', '倘若', '倘使', '腾', '替', '通过', '同', '同时', '哇', '万一', '往', '望', '为', '为何', '为了', '为什么', '为着', '喂', '嗡嗡', '我', '我们', '呜', '呜呼', '乌乎', '无论', '无宁', '毋宁', '嘻', '吓', '相对而言', '像', '向', '向着', '嘘', '呀', '焉', '沿', '沿着', '要', '要不', '要不然', '要不是', '要么', '要是', '也', '也罢', '也好', '一', '一般', '一旦', '一方面', '一来', '一切', '一样', '一则', '依', '依照', '矣', '以', '以便', '以及', '以免', '以至', '以至于', '以致', '抑或', '因', '因此', '因而', '因为', '哟', '用', '由', '由此可见', '由于', '有', '有的', '有关', '有些', '又', '于', '于是', '于是乎', '与', '与此同时', '与否', '与其', '越是', '云云', '哉', '再说', '再者', '在', '在下', '咱', '咱们', '则', '怎', '怎么', '怎么办', '怎么样', '怎样', '咋', '照', '照着', '者', '这', '这边', '这儿', '这个', '这会儿', '这就是说', '这里', '这么', '这么点儿', '这么些', '这么样', '这时', '这些', '这样', '正如', '吱', '之', '之类', '之所以', '之一', '只是', '只限', '只要', '只有', '至', '至于', '诸位', '着', '着呢', '自', '自从', '自个儿', '自各儿', '自己', '自家', '自身', '综上所述', '总的来看', '总的来说', '总的说来', '总而言之', '总之', '纵', '纵令', '纵然', '纵使', '遵照', '作为', '兮', '呃', '呗', '咚', '咦', '喏', '啐', '喔唷', '嗬', '嗯', '嗳')
        for keyword in ks:
            hit = 0
            if not strict:
                try:
                    mmseg.dict_load_defaults()
                    algor = mmseg.Algorithm(keyword['name'].lower())
                    kk = [tok.text for tok in algor]
                except:
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
        if debug: print '!IDF: keyword:%s, %s, %f'%(keyword['_id'], keyword['name'].encode('utf-8'), res)
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
        if debug: print "!COEF: Finished:%s, %s"%(keyword['_id'], keyword['name'].encode('utf-8'))
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
            if debug: print "!IDF: k(%s, %s), r(%s, %s) %f"%(keyword['_id'], keyword['name'].encode('utf-8'), rk['_id'], rk['name'].encode('utf-8'), cf)
            return keyword['_id'], {'_id':rk['_id'], 'ori_id':rk['ori_id'], 'coef':cf, 'rank':cf * rk['idf']}
        except:
            return None
            


class IntellectualError(Exception):

    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)

