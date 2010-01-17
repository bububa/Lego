#!/usr/bin/env python
# encoding: utf-8
"""
Crawler.py

Created by Syd on 2009-11-07.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""
import os
import re
import subprocess
import random
from hashlib import md5
import time
from datetime import datetime
from pprint import pprint
from urlparse import urljoin
from BeautifulSoup import BeautifulSoup
from yaml import YAMLObject, YAMLError
from yaml import load, dump
try:
    from yaml import CLoader as Loader
    from yaml import CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
from Helpers import Converter, ThreadPool, WrapperParser
from bububa.Lego.Base import Base
try:
    from bububa.Lego.MongoDB import Page
    from bububa.Lego.MongoDB import URLTrie
except:
    pass
from bububa.SuperMario.Mario import Mario, MarioBatch
from bububa.SuperMario.utils import URL, Traceback
from bububa.SuperMario.Parser import FullRssParser

class BaseCrawler(YAMLObject, Base):
    yaml_tag = u'!BaseCrawler'
    def __init__(self, starturl=None, url_pattern=None, duplicate_pattern=None, wrapper=None, max_depth=0, callback=None, executable=None, debug=None):
        self.starturl = starturl
        self.url_pattern = url_pattern
        self.duplicate_pattern = duplicate_pattern
        self.wrapper = wrapper
        self.max_depth = max_depth
        self.callback = callback
        self.executable = executable
        self.debug = debug
    
    def __repr__(self):
        return "%s(starturl=%r)" %(self.__class__.__name__, self.starturl)
    
    def run(self, starturl=None, label=None):
        if not starturl: starturl = self.starturl
        self.iterate_callables(exceptions=('callback', 'executable'))
        if hasattr(self, 'executable') and not self.executable.run(label = label): return self.output
        self.output = []
        self.url_cache = []
        self.inner_duplicate_urls = []
        self.new_urls = []
        self.contents = []
        mario = Mario(callback=self.parser)
        mario.get(starturl)
        if not self.output: 
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!BaseCrawler: failed to fetch starturl.\nurl: %s"%self.starturl)
            return self.output
        if hasattr(self, 'url_pattern'): url_pattern = self.url_pattern
        else: url_pattern = ''
        depth = 0
        while self.new_urls and (depth < self.max_depth or not self.max_depth):
            self.fetch(self.new_urls)
            depth += 1
        self.url_cache = self.contents = self.new_urls = None
        del(self.url_cache)
        del(self.contents)
        del(self.new_urls)
        del(self.inner_duplicate_urls)
        try:
            if hasattr(self, 'register'): self.callback.regist(self.register)
            self.callback.run()
        except:
            #if hasattr(self, 'debug') and self.debug:
            #    raise CrawlerError("!BaseCrawler: failed during callback.\n%r"%Traceback())
            pass
        return self.output
    
    def fetch(self, links):
        mario = MarioBatch(callback = self.parser)
        for link in links:
            mario.add_job(link)
        mario(10)
        self.new_urls = self.remove_inner_duplicate()
        return
    
    def remove_inner_duplicate(self):
        response = list(set(self.new_urls) - set(self.url_cache))
        if not hasattr(self, 'duplicate_pattern') and not self.duplicate_pattern: return response
        pattern = re.compile(self.duplicate_pattern, re.I)
        res = []
        for u in response:
            key = pattern.findall(u)
            if not key: continue
            key = key[0]
            if key not in self.inner_duplicate_urls:
                self.inner_duplicate_urls.append(key)
                res.append(u)
        response = None
        del(response)
        return res
    
    def parser(self, response):
        if hasattr(self, 'wrapper'):
            pattern = re.compile(self.wrapper, re.S)
            wrapper = pattern.findall(response.body)
            if wrapper: 
                wrapper = wrapper[0]
            else: wrapper = ''
        else:
            wrapper = response.body
        if hasattr(self, 'furthure'):
            furthure = self.furthure_parser(wrapper)
            if furthure: wrapper.update(furthure)
        content = md5(wrapper).hexdigest()
        if content in self.contents: return
        self.contents.append(content)
        if hasattr(self, 'debug') and self.debug: pprint(wrapper)
        if hasattr(self, 'url_pattern'): url_pattern = self.url_pattern
        else: url_pattern = ''
        soup = BeautifulSoup(wrapper, fromEncoding='utf-8')
        self.url_cache.append(response.url)
        if hasattr(self, 'duplicate_pattern') and self.duplicate_pattern:
            pattern = re.compile(self.duplicate_pattern, re.I)
            key = pattern.findall(response.url)
            if key:
                key = key[0]
                if key not in self.inner_duplicate_urls:
                    self.inner_duplicate_urls.append(key)
        for url in (URL.normalize(urljoin(response.url, a['href'])) for a in iter(soup.findAll('a')) if a.has_key('href') and a['href'] and re.match(url_pattern, a['href'])):
            if url in self.url_cache or url in self.new_urls:continue
            self.new_urls.append(url)
        self.output.append({'url':response.url, 'effective_url':response.effective_url, 'code':response.code, 'body':response.body, 'size':response.size, 'wrapper':wrapper, 'etag':response.etag, 'last_modified':response.last_modified})
    

class PaginateCrawler(YAMLObject, Base):
    yaml_tag = u'!PaginateCrawler'
    def __init__(self, url_pattern=None, start_no=None, end_no=None, step=None, wrapper=None, multithread=None, callback=None, executable=None, proxies=None, sleep=None, debug=None):
        self.url_pattern = url_pattern
        self.wrapper = wrapper
        self.start_no = start_no
        self.end_no = end_no
        self.step = step
        self.callback = callback
        self.executable = executable
        self.proxies = proxies
        self.multithread = multithread
        self.sleep = sleep
        self.debug = debug
    
    def __repr__(self):
        return "%s(url_pattern=%r, start_no=%r, end_no=%r)" %(self.__class__.__name__, self.url_pattern, start_no, end_no)
    
    def run(self):
        self.iterate_callables(exceptions=('callback'))
        if hasattr(self, 'logger'): self.setup_logger(self.logger['filename'])
        if hasattr(self, 'executable') and not self.executable: return self.output
        if hasattr(self, 'step'): step = self.step
        else: step = 1
        self.output = []
        self.contents = []
        pattern = re.compile('{NUMBER}')
        if hasattr(self, 'proxies') and self.proxies: proxies = self.proxies
        else: proxies = None
        links = [pattern.sub(str(no), self.url_pattern) for no in xrange(self.start_no, (self.end_no + 1)*step, step)]
        if hasattr(self, 'multithread') and self.multithread:
            max_chunk = random.choice(range(5,10))
            total_workers = len(links)
            for i in xrange(0, total_workers, max_chunk):
                us = links[i:i + max_chunk]
                self.run_workers(us)
                if hasattr(self, 'sleep') and self.sleep: time.sleep(self.sleep)
            self.contents = None
        else:
            if len(links) == 1:
                mario = Mario()
                mario.set_proxies_list(proxies)
                if hasattr(self, 'log'): self.log.info('mario: %s'%links[0])
                self.fetch(links[0])
            else:
                mario = MarioBatch(callback = self.parser)
                mario.set_proxies_list(proxies)
                for url in iter(links):
                    if hasattr(self, 'log'): self.log.info('mario: %s'%url)
                    mario.add_job(url)
                mario(10)
        self.contents = None
        del(self.contents)
        try:
            if hasattr(self, 'register'): self.callback.regist(self.register)
            self.callback.run()
        except:
            #if hasattr(self, 'debug') and self.debug:
            #    raise CrawlerError("!PaginateCrawler: failed during callback.\n%r"%Traceback())
            pass
        return self.output
    
    def run_workers(self, urls):
        threadPool = ThreadPool(len(urls))
        for url in urls:
            threadPool.run(self.fetch, url=url)
        if hasattr(self, 'timeout') and self.timeout: wait = self.timeout
        else: wait = None
        threadPool.killAllWorkers(wait)

    def fetch(self, url):
        if hasattr(self, 'log'): self.log.info('mario: %s'%url)
        if hasattr(self, 'proxies') and self.proxies: 
            retry = 5
            while retry:
                mario = Mario()
                mario.set_proxies_list(self.proxies)
                response = mario.get(url)
                if response: break
                retry -= 1
        else:
            mario = Mario()
            response = mario.get(url)
        self.parser(response)
        
    def parser(self, response):
        if not response: return
        if hasattr(self, 'log'): self.log.info('parse: %s'%response.effective_url)
        if hasattr(self, 'wrapper'):
            pattern = re.compile(self.wrapper, re.S)
            wrapper = pattern.findall(response.body)
            if wrapper:
                wrapper = wrapper[0]
            else: wrapper = ''
        else:
            wrapper = response.body
        if hasattr(self, 'furthure'):
            furthure = self.furthure_parser(wrapper)
            if furthure: wrapper.update(furthure)
        content = md5(wrapper).hexdigest()
        if hasattr(self, 'debug') and self.debug: pprint(wrapper)
        if content in self.contents: 
            if hasattr(self, 'log'): self.log.warn('Duplicate content: %s'%response.url)
            return
        self.contents.append(content)
        if hasattr(self, 'url_pattern'): url_pattern = self.url_pattern
        else: url_pattern = ''
        self.output.append({'url':response.url, 'effective_url':response.effective_url, 'code':response.code, 'body':response.body, 'size':response.size, 'wrapper':wrapper, 'etag':response.etag, 'last_modified':response.last_modified})
    

class DictParser(YAMLObject, Base):
    yaml_tag = u'!DicParser'
    def __init__(self, page=None, wrapper=None):
        self.page = page
        self.wrapper = wrapper
    
    def __repr__(self):
        return "%s(page=%r, wrapper=%r)" % (self.__class__.__name__, self.page, self.wrapper)
    
    def run(self):
        self.iterate_callables()
        self.output = WrapperParser(self.page['wrapper'], self.wrapper)
        return self.output
    

class ArrayParser(YAMLObject, Base):
    yaml_tag = u'!ArrayParser'
    def __init__(self, page=None, wrapper=None):
        self.page = page
        self.wrapper = wrapper
    
    def __repr__(self):
        return "%s(page=%r, wrapper=%r)" % (self.__class__.__name__, self.page, self.wrapper)
    
    def run(self):
        self.iterate_callables()
        self.output = []
        pattern = re.compile(self.wrapper, re.S)
        self.parse(self.page['wrapper'], pattern)
        return self.output
    
    def parse(self, page, pattern):
        res = pattern.findall(page)
        if not res: return []
        self.output += res
    

class URLCrawler(YAMLObject, Base):
    yaml_tag = u'!URLCrawler'
    def __init__(self, urls=None, wrapper=None, url_pattern=None, duplicate_pattern=None, essential_fields=None, remove_external_duplicate=None, multithread=True, save_output=None, user_info=None, sleep=None, callback=None, proxies=None, debug=None):
        self.urls = urls
        self.url_pattern = url_pattern
        self.duplicate_pattern = duplicate_pattern
        self.essential_fields = essential_fields
        self.remove_external_duplicate = remove_external_duplicate
        self.wrapper = wrapper
        self.multithread = multithread
        self.save_output = save_output
        self.user_info = user_info
        self.callback = callback
        self.sleep = sleep
        self.proxies = proxies
        self.debug = debug
    
    def __repr__(self):
        return "%s(urls=%r, wrapper=%r)" % (self.__class__.__name__, self.urls, self.wrapper)
    
    def run(self, urls=None):
        self.iterate_callables(exceptions='callback')
        if hasattr(self, 'logger'): self.setup_logger(self.logger['filename'])
        self.output = []
        if isinstance(urls, (str, unicode)): self.urls = (urls, )
        elif isinstance(urls, (list, set)): self.urls = list(urls)
        if not self.urls: 
            if hasattr(self, 'log'): self.log.warn('no urls need to be crawled.')
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!URLCrawler: no links need to be crawled.")
            return self.output
        self.contents = []
        self.inner_duplicate_urls = []
        if hasattr(self, 'url_pattern') and self.url_pattern: url_pattern = self.url_pattern
        else: url_pattern = ''
        pattern = re.compile(url_pattern, re.I)
        if hasattr(self, 'duplicate_pattern') and self.duplicate_pattern:
            pattern_key = re.compile(self.duplicate_pattern, re.I)
            links = []
            for link in set(self.urls):
                key = pattern_key.findall(link)
                if key:
                    key = key[0]
                    if key not in self.inner_duplicate_urls:
                        self.inner_duplicate_urls.append(key)
                        links.append(URL.normalize(link))
        else:
            links = [URL.normalize(link) for link in set(self.urls)]
        if not links: 
            if hasattr(self, 'log'): self.log.warn('no urls need to be crawled after remove inner duplicates.')
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!URLCrawler: no links need to be crawled.")
            return self.output
        if hasattr(self, 'remove_external_duplicate') and self.remove_external_duplicate:
            links = [link for link in links if not self.is_external_duplicate(link)]
        if not links: 
            if hasattr(self, 'log'): self.log.warn('no urls need to be crawled after remove external duplicates.')
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!URLCrawler: no links need to be crawled")
            return self.output
        if hasattr(self, 'multithread') and self.multithread:
            max_chunk = random.choice(range(5,10))
            total_workers = len(links)
            for i in xrange(0, total_workers, max_chunk):
                us = links[i:i + max_chunk]
                self.run_workers(us)
                if hasattr(self, 'sleep') and self.sleep: time.sleep(self.sleep)
            self.contents = None
        else:
            if len(links) == 1:
                mario = Mario()
                if hasattr(self, 'log'): self.log.info('mario: %s'%links[0])
                self.fetch(links[0])
            else:
                mario = MarioBatch(callback = self.parser)
                for link in links:
                    if hasattr(self, 'log'): self.log.info('mario: %s'%link)
                    mario.add_job(link)
                mario(10)
        return self.output
    
    def run_workers(self, urls):
        threadPool = ThreadPool(len(urls))
        for url in urls:
            threadPool.run(self.fetch, url=url)
        if hasattr(self, 'timeout') and self.timeout: wait = self.timeout
        else: wait = None
        threadPool.killAllWorkers(wait)
    
    def fetch(self, url):
        if hasattr(self, 'log'): self.log.info('mario: %s'%url)
        if hasattr(self, 'proxies') and self.proxies: 
            retry = 5
            while retry:
                mario = Mario()
                mario.set_proxies_list(self.proxies)
                response = mario.get(url)
                if response: break
                retry -= 1
        else:
            mario = Mario()
            response = mario.get(url)
        self.parser(response)

    def parser(self, response):
        if hasattr(self, 'log'): self.log.info('parse: %s'%response.effective_url)
        if not response: return
        if hasattr(self, 'user_info') and isinstance(self.user_info, dict) and self.user_info:
            user_info = self.user_info
        else:
            user_info = None
        wrapper = WrapperParser(response.body, self.wrapper, user_info, self.miss_fields)
        if hasattr(self, 'furthure'):
            furthure = self.furthure_parser(wrapper)
            if furthure: wrapper.update(furthure)
        content = md5(repr(wrapper)).hexdigest()
        if content in self.contents: return
        self.contents.append(content)
        page = {'url':response.url, 'effective_url':response.effective_url, 'code':response.code, 'body':response.body, 'size':response.size, 'wrapper':wrapper, 'etag':response.etag, 'last_modified':response.last_modified}
        if hasattr(self, 'debug') and self.debug: pprint(wrapper)
        if hasattr(self, 'save_output') and self.save_output:
            self.output.append(page)
        else:
            self.output = True
        if not hasattr(self, 'callback'): return
        try:
            if hasattr(self, 'register'): self.callback.regist(self.register)
            self.callback.run(page)
        except:
            #if hasattr(self, 'debug') and self.debug:
            #    raise CrawlerError("!URLCrawler: fail during callback.\n%r"%Traceback())
            pass
    
    def is_external_duplicate(self, link):
        url_hash = md5(link).hexdigest().decode('utf-8')
        try:
            return Page.get_from_id(url_hash)
        except:
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!URLCrawler: fail to check external duplicate.\nurl: %s"%link)
            return None
    
    def miss_fields(self, wrapper):
        if not hasattr(self, 'essential_fields'): return None
        for k in self.essential_fields:
            if k not in self.essential_fields or not wrapper[k]: 
                if hasattr(self, 'log'): self.log.warn('missing field: %s'%k)
                if hasattr(self, 'debug') and self.debug:
                    raise CrawlerError("!URLCrawler: missing field: %s"%k)
                return True
        return None
    

class URLsFinder(YAMLObject, Base):
    yaml_tag = u'!URLsFinder'
    def __init__(self, starturl=None, label=None, target_url=None, url_pattern=None, max_depth=0, callback=None, proxies=None, debug=None):
        self.starturl = starturl
        self.target_url = target_url
        self.url_pattern = url_pattern
        self.label = label
        self.max_depth = max_depth
        self.callback = callback
        self.proxies = proxies
        self.debug = debug
    
    def __repr__(self):
        return "%s(starturl=%r)" %(self.__class__.__name__, self.starturl)
    
    def run(self, starturl=None, label=None):
        if not starturl: starturl = self.starturl
        if not label: label = self.label
        self.iterate_callables(exceptions=('callback'))
        self.output = []
        if hasattr(self, 'url_pattern'): url_pattern = self.url_pattern
        else: url_pattern = ''
        pattern = re.compile(url_pattern, re.I)
        for depth in xrange(self.max_depth, 0, -1):
            if depth == self.max_depth: 
                urls = [self.starturl, ]
                self.save_links(urls, label, depth)
            else:
                urls = [u['url'] for u in URLTrie.all({'label':label, 'depth':depth})]
            self.depth_crawl(urls, label, depth)
        try:
            if hasattr(self, 'register'): self.callback.regist(self.register)
            self.callback.run()
        except:
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!BaseCrawler: failed during callback.\n%r"%Traceback())
            pass
        return self.output
    
    def depth_crawl(self, urls, label, depth):
        max_chunk = random.choice(range(5,10))
        total_workers = len(urls)
        for i in xrange(0, total_workers, max_chunk):
            us = urls[i:i + max_chunk]
            self.run_workers(us, label, depth)
    
    def run_workers(self, urls, label, depth):
        self.url_cache = []
        threadPool = ThreadPool(len(urls))
        for url in urls:
            threadPool.run(self.fetch, url=url, label=label, depth=depth)
        if hasattr(self, 'timeout') and self.timeout: wait = self.timeout
        else: wait = None
        threadPool.killAllWorkers(wait)
        self.save_links(self.url_cache, label, depth-1)
        self.url_cache = None
    
    def fetch(self, url, label, depth):
        if hasattr(self, 'proxies') and self.proxies: 
            retry = 5
            while retry:
                mario = Mario()
                mario.set_proxies_list(self.proxies)
                response = mario.get(url)
                if response: break
                retry -= 1
        else:
            mario = Mario()
            response = mario.get(url)
        if not response: return
        if hasattr(self, 'url_pattern'): url_pattern = self.url_pattern
        else: url_pattern = ''
        soup = BeautifulSoup(response.body, fromEncoding='utf-8')
        for url in (URL.normalize(urljoin(response.url, a['href'])) for a in iter(soup.findAll('a')) if a.has_key('href') and a['href'] and re.match(url_pattern, a['href'])):
            if url in self.url_cache:continue
            self.url_cache.append(url)
    
    def save_links(self, links, label, depth):
        if hasattr(self, 'debug') and self.debug:
            print 'saving links for depth: %d'%depth
        pattern = re.compile(self.target_url, re.I)
        for link in links:
            ident = md5('url:%s, label:%s'%(link, label.encode('utf-8'))).hexdigest().decode('utf-8')
            if self.is_external_duplicate(ident):
                continue
            while True:
                try:
                    urlTrie = URLTrie()
                    urlTrie['_id'] = ident
                    if isinstance(link, str): link = link.decode('utf-8')
                    if isinstance(label, str): label = label.decode('utf-8')
                    urlTrie['url'] = link
                    urlTrie['url_hash'] = md5(link.encode('utf-8')).hexdigest().decode('utf-8')
                    urlTrie['depth'] = depth
                    urlTrie['label'] = label
                    urlTrie['inserted_at'] = datetime.utcnow()
                    if pattern.match(link):
                        self.output.append(link)
                        urlTrie['is_target'] = 1
                    urlTrie.save()
                    if hasattr(self, 'debug') and self.debug:
                        print 'saved:%s, %d'%(link.encode('utf-8'), depth)
                    break
                except Exception, err:
                    print err
                    continue
    
    def is_external_duplicate(self, ident):
        try:
            return URLTrie.get_from_id(ident)
        except Exception, err:
            return None
    

class DetailCrawler(YAMLObject, Base):
    yaml_tag = u'!DetailCrawler'
    def __init__(self, pages=None, url_pattern=None, wrapper=None, essential_fields=None, user_info=None, remove_external_duplicate=None, multithread=True, save_output=None, sleep=None, callback=None, page_callback=None, proxies=None, debug=None):
        self.pages = pages
        self.url_pattern = url_pattern
        self.essential_fields = essential_fields
        self.wrapper = wrapper
        self.user_info = user_info
        self.remove_external_duplicate = remove_external_duplicate
        self.multithread = multithread
        self.save_output = save_output
        self.callback = callback
        self.page_callback = page_callback
        self.sleep = sleep
        self.proxies = proxies
        self.debug = debug
    
    def __repr__(self):
        return "%s(pages=%r, url_pattern=%r, wrapper=%r)" % (self.__class__.__name__, self.pages, self.url_pattern, self.wrapper)
    
    def run(self):
        self.iterate_callables(exceptions=('callback', 'page_callback'))
        if hasattr(self, 'logger'): self.setup_logger(self.logger['filename'])
        self.output = []
        self.tmp_pages = {}
        if not self.pages: 
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!DetailCrawler: no pages need to be crawled.")
            return self.output
        self.contents = []
        page_count = 0
        for i, page in enumerate(self.pages):
            page_num = str(i)
            self.tmp_pages[page_num] = []
            if self.fetch(page, page_num):
                page_count += 1
            if hasattr(self, 'callback'):
                try:
                    if hasattr(self, 'register'): self.callback.regist(self.register)
                    self.callback.run(self.tmp_pages[page_num])
                except:
                    #if hasattr(self, 'debug') and self.debug:
                    #    raise CrawlerError("!DetailCrawler: during callback.\n%r"%Traceback())
                    pass
            if hasattr(self, 'sleep') and self.sleep and self.tmp_pages[page_num]: time.sleep(self.sleep)
            self.tmp_pages[page_num] = None
        self.contents = None
        del(self.contents)
        if not self.output: self.output = page_count
        return self.output
    
    def fetch(self, page, page_num):
        links = []
        try:
            soup = BeautifulSoup(page['wrapper'], fromEncoding='utf-8')
            links = [URL.normalize(urljoin(page['effective_url'], a['href'])) for a in iter(soup.findAll('a')) if a.has_key('href') and a['href'] and re.match(self.url_pattern, a['href'])]
        except:
            urls = re.findall('href=["|\'|](.*?)["|\'|]',page['wrapper'],re.I|re.M)
            if urls:
                links = [URL.normalize(urljoin(page['effective_url'],url)) for url in iter(urls) if re.match(self.url_pattern, url)]
        if not links: 
            if hasattr(self, 'log'): self.log.warning('no links in page: %s after remove unmatched links'%page['effective_url'])
            return 0
        if hasattr(self, 'remove_external_duplicate') and self.remove_external_duplicate:
            links = [link for link in links if not self.is_external_duplicate(link)]
        if not links: 
            if hasattr(self, 'log'): self.log.warn('no links in page: %s after remove external duplcates'%page['effective_url'])
            return 0
        if hasattr(self, 'multithread') and self.multithread:
            max_chunk = random.choice(range(5,10))
            total_workers = len(links)
            for i in xrange(0, total_workers, max_chunk):
                us = links[i:i + max_chunk]
                self.run_workers(us, page_num)
        else:
            if len(links) == 1:
                mario = Mario()
                self.thread_fetch(links[0], page_num)
            else:
                if hasattr(self, 'proxies') and self.proxies: proxies = self.proxies
                else: proxies = None
                mario = MarioBatch(callback = self.parser)
                mario.set_proxies_list(proxies)
                for link in set(links):
                    mario.add_job(link)
                mario(10)
        return len(links)
    
    def run_workers(self, urls, page_num):
        threadPool = ThreadPool(len(urls))
        for url in urls:
            threadPool.run(self.thread_fetch, url=url, page_num=page_num)
        if hasattr(self, 'timeout') and self.timeout: wait = self.timeout
        else: wait = None
        threadPool.killAllWorkers(wait)
    
    def thread_fetch(self, url, page_num):
        if hasattr(self, 'log'): self.log.info('fetch url: %s in page: %d'%(url, int(page_num)))
        if hasattr(self, 'proxies') and self.proxies: 
            retry = 5
            while retry:
                mario = Mario()
                mario.set_proxies_list(self.proxies)
                response = mario.get(url)
                if response: break
                retry -= 1
        else:
            mario = Mario()
            response = mario.get(url)
        self.parser(response, page_num)
    
    def parser(self, response, page_num):
        if not response: return
        if hasattr(self, 'log'): self.log.info('parse url: %s in page: %d'%(response.effective_url, int(page_num)))
        if hasattr(self, 'user_info') and isinstance(self.user_info, dict) and self.user_info:
            user_info = self.user_info
        else:
            user_info = None
        wrapper = WrapperParser(response.body, self.wrapper, user_info, self.miss_fields)
        if hasattr(self, 'furthure'):
            furthure = self.furthure_parser(wrapper)
            if furthure: wrapper.update(furthure)
        content = md5(repr(wrapper)).hexdigest()
        if content in self.contents: 
            if hasattr(self, 'log'): self.log.warn('Duplicate content: %s'%response.url)
            return
        self.contents.append(content)
        page = {'url':response.url, 'effective_url':response.effective_url, 'code':response.code, 'body':response.body, 'size':response.size, 'wrapper':wrapper, 'etag':response.etag, 'last_modified':response.last_modified}
        if hasattr(self, 'debug') and self.debug: pprint(wrapper)
        if hasattr(self, 'page_callback'):
            try:
                if hasattr(self, 'register'): self.page_callback.regist(self.register)
                self.page_callback.run(page)
            except Exception, err:
                print err
                pass
        if hasattr(self, 'save_output') and self.save_output:
            self.output.append(page)
        else:
            self.output = True
        self.tmp_pages[page_num].append(page)
    
    def is_external_duplicate(self, link):
        url_hash = md5(link).hexdigest().decode('utf-8')
        try:
            return Page.get_from_id(url_hash)
        except:
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!DetailCrawler: fail to check external duplicate.\nurl: %r"%link)
            return None
    
    def miss_fields(self, wrapper):
        if not hasattr(self, 'essential_fields'): return None
        for k in self.essential_fields:
            if k not in wrapper or not wrapper[k]: 
                if hasattr(self, 'log'): self.log.warn('missing field: %s'%k)
                if hasattr(self, 'debug') and self.debug:
                    raise CrawlerError("!DetailCrawler: missing field: %s"%k)
                return True
        return None
    

class FullRSSCrawler(YAMLObject, Base):
    yaml_tag = u'!FullRSSCrawler'
    def __init__(self, starturl=None, etag=None, last_modified=None, check_baseurl=None, essential_fields=None, user_info=None, remove_external_duplicate=None, callback=None, executable=None, multithread=None, proxies=None, debug=None):
        self.starturl = starturl
        self.etag = etag
        self.last_modified = last_modified
        self.check_baseurl = check_baseurl
        self.essential_fields = essential_fields
        self.user_info = user_info
        self.remove_external_duplicate = remove_external_duplicate
        self.callback = callback
        self.executable = executable
        self.multithread = multithread
        self.proxies = proxies
        self.debug = debug
    
    def __repr__(self):
        return "%s(starturl=%r)" % (self.__class__.__name__, self.starturl)
    
    def run(self, starturl=None, label=None):
        if not starturl: starturl = self.starturl
        self.iterate_callables(exceptions=('callback', 'executable'))
        if hasattr(self, 'executable') and not self.executable.run(label = label): return self.output
        self.output = {'rss':{}, 'entries':[]}
        etag = last_modified = check_baseurl = multithread = None
        if hasattr(self, 'check_baseurl'): check_baseurl = self.check_baseurl
        if hasattr(self, 'etag'): etag = self.etag
        if hasattr(self, 'last_modified'): last_modified = self.last_modified
        if hasattr(self, 'multithread'): multithread = self.multithread
        fullRssParser = FullRssParser(url=starturl, etag = etag, last_modified=last_modified, callback=self.parser, check_baseurl=check_baseurl, multithread=multithread, proxies=proxies)
        self.output['rss'] = fullRssParser.rss_response
        try:
            if hasattr(self, 'register'): self.callback.regist(self.register)
            self.callback.run(self.output)
        except:
            #if hasattr(self, 'debug') and self.debug:
            #    raise CrawlerError(Traceback())
            pass
        return self.output
    
    def parser(self, response):
        if not response: return None
        if hasattr(self, 'remove_external_duplicate') and self.remove_external_duplicate and self.is_external_duplicate(URL.normalize(response['url'])): 
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!FullRSSCrawler: external duplicate url.\n%r"%response)
            return
        try:
            pubdate = datetime(*response['updated_parsed'][:6])
        except TypeError:
            pubdate = datetime.utcnow()
        wrapper = {'title':response['title'], 'content':response['content'], 'author':response['author'], 'pubdate':pubdate, 'domain':response['baseurl']}
        if hasattr(self, 'user_info') and isinstance(self.user_info, dict) and self.user_info:
            for k, v in self.user_info.iteritems():
                wrapper[k] = v
        if hasattr(self, 'furthure'):
            furthure = self.further_parser(wrapper)
            if furthure: wrapper.update(furthure)
        if hasattr(self, 'debug') and self.debug: pprint(wrapper)
        if self.miss_fields(wrapper): 
            return
        page = {'url':response['url'], 'effective_url':response['url'], 'code':'200', 'body':'', 'size':len(response['content']), 'wrapper':wrapper, 'etag':response['etag'], 'last_modified':response['last_modified']}
        self.output['entries'].append(page)
    
    def is_external_duplicate(self, link):
        url_hash = md5(link).hexdigest().decode('utf-8')
        try:
            return Page.get_from_id(url_hash)
        except:
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!FullRSSCrawler: check external duplicate url failed.\nurl: %r"%link)
            return None
    
    def miss_fields(self, wrapper):
        if not hasattr(self, 'essential_fields'): return None
        for k in self.essential_fields:
            if k not in wrapper or not wrapper[k]: 
                if hasattr(self, 'debug') and self.debug:
                    raise CrawlerError("!FullRSSCrawler: missing fieldd: %r"%k)
                return True
        return None


class ThreadCrawler(YAMLObject, Base):
    yaml_tag = u'!ThreadCrawler'
    def __init__(self, urls=None, executable=None, callback=None, timeout=None):
        self.urls = urls
        self.executable = executable
        self.callback = callback
        self.timeout = timeout

    def __repr__(self):
        return "%s(urls=%r)" % (self.__class__.__name__, self.urls)
    
    def run(self):
        self.iterate_callables(exceptions='callback')
        self.output = []
        if hasattr(self, 'executable') and not self.executable: return self.output
        max_chunk = random.choice(range(5,10))
        total_workers = len(self.urls)
        for i in xrange(0, total_workers, max_chunk):
            urls = self.urls[i:i + max_chunk]
            self.run_workers(urls)
        return self.output
    
    def results(self, response):
        if not response: return
        try:
            if hasattr(self, 'register'): self.callback.regist(self.register)
            self.callback.run(response)
        except:
            pass
        self.output.append(response)
    
    def run_workers(self, urls):
        threadPool = ThreadPool(len(urls))
        for u in urls:
            if not isinstance(u, dict): continue
            label = tuple(u)[0]
            url = u[label]
            worker = self.callback
            threadPool.run(worker.run, callback=self.results, starturl=url, label=label)
        if hasattr(self, 'timeout') and self.timeout: wait = self.timeout
        else: wait = None
        threadPool.killAllWorkers(wait)
    

class ThreadLego(YAMLObject, Base):
    yaml_tag = u'!ThreadLego'
    def __init__(self, yaml_files=None, callback=None, debug=None):
        self.yaml_files = yaml_files
        self.callback = callback

    def __repr__(self):
        return "%s(yaml_files=%r)" % (self.__class__.__name__, self.yaml_files)

    def run(self):
        self.iterate_callables(exceptions='callback')
        self.output = []
        max_chunk = random.choice(range(5,10))
        total_workers = len(self.yaml_files)
        for i in xrange(0, total_workers, max_chunk):
            yaml_files = self.yaml_files[i:i + max_chunk]
            self.run_workers(yaml_files)
        return self.output
    
    def results(self, response):
        self.output.append(response)
    
    def run_workers(self, yaml_files):
        threadPool = ThreadPool(len(yaml_files))
        for yaml_file in yaml_files:
            worker = self.callback
            threadPool.run(worker.run, callback=self.results, yaml_file=yaml_file)
        threadPool.killAllWorkers()
    

class SubprocessLego(YAMLObject, Base):
    yaml_tag = u'!SubprocessLego'
    def __init__(self, yaml_files, callback, concurrent, debug=None):
        self.yaml_files = yaml_files
        self.concurrent = concurrent
        self.callback = callback
    
    def __repr__(self):
        return "%s(yaml_files=%r)" % (self.__class__.__name__, self.yaml_files)
    
    def run(self):
        self.iterate_callables(exceptions='callback')
        if hasattr(self, 'logger'): self.setup_logger(self.logger['filename'])
        if hasattr(self, 'concurrent'): concurrent = self.concurrent
        else: concurrent = 10
        total_processes = len(self.yaml_files)
        yaml_files = self.yaml_files
        running_process = {}
        self.output = []
        for yaml_file in yaml_files:
            while len(running_process) >= concurrent:
                running_process = self.recycle(running_process)
            if isinstance(self.callback, dict) and self.callback.has_key('cmd') and self.callback.has_key('timeout'):
                cmd = '%s -c %s'%(self.callback['cmd'], yaml_file)
                timeout = self.callback['cmd']
            else:
                cmd = '%s -c %s'%(self.callback, yaml_file)
                timeout = 0
            if hasattr(self, 'log'): self.log.info(cmd)
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            running_process[cmd] = {'process':p, 'starttime': datetime.now(), 'timeout':timeout}
        while running_process:
            running_process = self.recycle(running_process)
        return self.output
    
    def recycle(self, running_process):
        if not running_process: return []
        remove_cmd = []
        for cmd, process in running_process.items():
            if process['process'].poll() != None: 
                remove_cmd.append(cmd)
                self.output.append((cmd, process['process'].stdout.read()))
            if not process['timeout']: continue
            start = process['starttime']
            now = datetime.now()
            if process['timeout'] and (now - start).seconds > process['timeout']:
                os.kill(process['process'].pid, signal.SIGKILL)
                os.waitpid(-1, os.WNOHANG)
                remove_cmd.append(cmd)
                self.output.append((cmd, None))
        if not remove_cmd: return running_process
        for cmd in remove_cmd:
            if hasattr(self, 'log'): self.log.info('Recycle: %s'%cmd)
            running_process.pop(cmd)
        return running_process
    

class Crawlable(YAMLObject, Base):
    yaml_tag = u'!Crawlable'
    def __init__(self, yaml_file, label):
        self.yaml_file = yaml_file
        self.label = label
    
    def __repr__(self):
        return "%s(yaml_file=%r, label=%r)" % (self.__class__.__name__, self.yaml_file, self.label)
    
    def run(self, label=None, yaml_file=None):
        self.iterate_callables()
        if hasattr(self, 'logger'): self.setup_logger(self.logger['filename'])
        data = None
        if not yaml_file: yaml_file = self.yaml_file
        if not label: label = self.label
        yaml_file_path = os.path.join(os.getcwd(), yaml_file)
        if hasattr(self, 'log'): self.log.info('Check %s'%yaml_file_path)
        try:
            fp = file(yaml_file_path, 'r')
            stream = fp.read()
            fp.close()
        except IOError, err:
            if hasattr(self, 'log'): self.log.error("Can't read YAML file: %s"%yaml_file_path)
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!Crawlable: can't read YAML file.\n%r"%Traceback())
            return self.output
        try:
            data = load(stream, Loader=Loader)
        except YAMLError, exc:
            if hasattr(self, 'log'): self.log.execption("Can't read YAML data.")
            if hasattr(self, 'debug') and self.debug:
                if hasattr(exc, 'problem_mark'):
                    mark = exc.problem_mark
                raise StorageError("!Crawlable: position: (%s:%s)\nYAML file: %r" % (mark.line+1, mark.column+1, yaml_file))
            return self.output
        if not label or not label in data: return self.output
        data = data[label]
        if 'last_updated_at' not in data or 'duration' not in data:
            self.output = data
            return self.output
        if (time.mktime(time.gmtime()) - float(data['last_updated_at'])) > float(data['duration']):
            self.output = data
        elif hasattr(self, 'log'): self.log.warn("Could not crawl, please wait...")
        return self.output
    

class SiteCrawler(YAMLObject, Base):
    yaml_tag = u'!SiteCrawler'
    def __init__(self, check, crawler):
        self.check = check
        self.crawler = crawler
    
    def __repr__(self):
        return "%s(yaml_files=%r, label=%r)" % (self.__class__.__name__, self.check, self.crawler)
    
    def run(self):
        self.output = None
        params = self.check.run()
        if not params: return None
        self.crawler.regist(params)
        self.output = self.crawler.run()
        return self.output
    

class CrawlerError(Exception):
    def __init__(self, value):
        self.parameter = value
    
    def __str__(self):
        return repr(self.parameter)
    