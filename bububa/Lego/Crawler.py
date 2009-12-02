#!/usr/bin/env python
# encoding: utf-8
"""
Crawler.py

Created by Syd on 2009-11-07.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""
import re
from hashlib import md5
from datetime import datetime
from urlparse import urljoin
from BeautifulSoup import BeautifulSoup
from yaml import YAMLObject
from Helpers import Converter, ThreadPool
from bububa.Lego.Base import Base
try:
    from bububa.Lego.MongoDB import Page
except:
    pass
from bububa.SuperMario.Mario import Mario, MarioBatch
from bububa.SuperMario.utils import URL, Traceback
from bububa.SuperMario.Parser import FullRssParser

class BaseCrawler(YAMLObject, Base):
    yaml_tag = u'!BaseCrawler'
    def __init__(self, starturl, url_pattern=None, duplicate_pattern=None, wrapper=None, max_depth=0, callback=None, executable=None, debug=None):
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
        content = md5(wrapper).hexdigest()
        if content in self.contents: return
        self.contents.append(content)
        if hasattr(self, 'url_pattern'): url_pattern = self.url_pattern
        else: url_pattern = ''
        soup = BeautifulSoup(wrapper)
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
        self.output.append({'url':response.url, 'effective_url':response.effective_url, 'code':response.code, 'body':response.body, 'size':response.size, 'wrapper':wrapper})


class PaginateCrawler(YAMLObject, Base):
    yaml_tag = u'!PaginateCrawler'
    def __init__(self, url_pattern, start_no, end_no, step, wrapper=None, callback=None, executable=None, debug=None):
        self.url_pattern = url_pattern
        self.wrapper = wrapper
        self.start_no = start_no
        self.end_no = end_no
        self.step = step
        self.callback = callback
        self.executable = executable
        self.debug = debug

    def __repr__(self):
        return "%s(url_pattern=%r, start_no=%r, end_no=%r)" %(self.__class__.__name__, self.url_pattern, start_no, end_no)

    def run(self):
        self.iterate_callables(exceptions=('callback'))
        if hasattr(self, 'executable') and not self.executable: return self.output
        if hasattr(self, 'step'): step = self.step
        else: step = 1
        self.output = []
        self.contents = []
        pattern = re.compile('{NUMBER}')
        mario = MarioBatch(callback = self.parser)
        for no in xrange(self.start_no, self.end_no*step, step):
            url = pattern.sub(str(no), self.url_pattern)
            mario.add_job(url)
        mario(10)
        self.contents = None
        del(self.contents)
        try:
            self.callback.run()
        except:
            #if hasattr(self, 'debug') and self.debug:
            #    raise CrawlerError("!PaginateCrawler: failed during callback.\n%r"%Traceback())
            pass
        return self.output

    def parser(self, response):
        if hasattr(self, 'wrapper'):
            pattern = re.compile(self.wrapper, re.S)
            wrapper = pattern.findall(response.body)
            if wrapper:
                wrapper = wrapper[0]
            else: wrapper = ''
        else:
            wrapper = response.body
        content = md5(wrapper).hexdigest()
        if content in self.contents: return
        self.contents.append(content)
        if hasattr(self, 'url_pattern'): url_pattern = self.url_pattern
        else: url_pattern = ''
        soup = BeautifulSoup(wrapper)
        self.output.append({'url':response.url, 'effective_url':response.effective_url, 'code':response.code, 'body':response.body, 'size':response.size, 'wrapper':wrapper})


class DictParser(YAMLObject, Base):
    yaml_tag = u'!DicParser'
    def __init__(self, page, wrapper):
        self.page = page
        self.wrapper = wrapper
    
    def __repr__(self):
        return "%s(page=%r, wrapper=%r)" % (self.__class__.__name__, self.page, self.wrapper)
    
    def run(self):
        self.iterate_callables()
        pattern = re.compile(self.wrapper, re.S)
        self.output = self.parse(self.page['wrapper'], pattern)
        return self.output
    
    def parse(self, page, pattern):
        match = pattern.match(page)
        return match.groupdict()


class ArrayParser(YAMLObject, Base):
    yaml_tag = u'!ArrayParser'
    def __init__(self, page, wrapper):
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
    def __init__(self, urls, url_pattern, wrapper, duplicate_pattern=None, essential_fields=None, remove_external_duplicate=None, user_info=None, callback=None, debug=None):
        self.urls = urls
        self.url_pattern = url_pattern
        self.duplicate_pattern = duplicate_pattern
        self.essential_fields = essential_fields
        self.remove_external_duplicate = remove_external_duplicate
        self.wrapper = wrapper
        self.user_info = user_info
        self.callback = callback
        self.debug = debug

    def __repr__(self):
        return "%s(urls=%r, wrapper=%r)" % (self.__class__.__name__, self.urls, self.wrapper)

    def run(self):
        self.iterate_callables(exceptions='callback')
        self.output = []
        if not self.urls: 
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!URLCrawler: no links need to be crawled.")
            return self.output
        self.contents = []
        self.inner_duplicate_urls = []
        if hasattr(self, 'url_pattern'): url_pattern = self.url_pattern
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
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!URLCrawler: no links need to be crawled.")
            return self.output
        if hasattr(self, 'remove_external_duplicate') and self.remove_external_duplicate:
            links = [link for link in links if not self.is_external_duplicate(link)]
        if not links: 
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!URLCrawler: no links need to be crawled")
            return self.output
        mario = MarioBatch(callback = self.parser)
        for link in links:
            mario.add_job(link)
        mario(10)
        self.contents = None
        del(self.contents)
        return self.output

    def parser(self, response):
        pattern = re.compile('.*%s'%self.wrapper, re.S)
        wrapper = pattern.match(response.body)
        if not wrapper: 
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!URLCrawler: no matched content.\n%r"%response)
            return
        wrapper = wrapper.groupdict()
        if hasattr(self, 'user_info') and isinstance(self.user_info, dict) and self.user_info:
            for k, v in self.user_info.iteritems():
                wrapper[k] = v
        for k, v in wrapper.items():
            if k.endswith('_number') and isinstance(v, (str, unicode)): wrapper[k] = Converter.toNumber(v)
            elif k.endswith('_datetime') and isinstance(v, (str, unicode)): wrapper[k] = Converter.toDatetime(v)
        if self.miss_fields(wrapper): return
        content = md5(repr(wrapper)).hexdigest()
        if content in self.contents: return
        self.contents.append(content)
        page = {'url':response.url, 'effective_url':response.effective_url, 'code':response.code, 'body':response.body, 'size':response.size, 'wrapper':wrapper}
        self.output.append(page)
        if not hasattr(self, 'callback'): return
        try:
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
                if hasattr(self, 'debug') and self.debug:
                    raise CrawlerError("!URLCrawler: missing field: %s"%k)
                return True
        return None


class DetailCrawler(YAMLObject, Base):
    yaml_tag = u'!DetailCrawler'
    def __init__(self, pages, url_pattern, wrapper, essential_fields=None, user_info=None, remove_external_duplicate=None, callback=None, debug=None):
        self.pages = pages
        self.url_pattern = url_pattern
        self.essential_fields = essential_fields
        self.wrapper = wrapper
        self.user_info = user_info
        self.remove_external_duplicate = remove_external_duplicate
        self.callback = callback
        self.debug = debug
    
    def __repr__(self):
        return "%s(pages=%r, url_pattern=%r, wrapper=%r)" % (self.__class__.__name__, self.pages, self.url_pattern, self.wrapper)
    
    def run(self):
        self.iterate_callables(exceptions='callback')
        self.output = []
        if not self.pages: 
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!DetailCrawler: no pages need to be crawled.")
            return self.output
        self.contents = []
        for page in self.pages:
            self.fetch(page)
            if not hasattr(self, 'callback'): continue
            for p in self.tmp_pages:
                try:
                    self.callback.run(p)
                except:
                    #if hasattr(self, 'debug') and self.debug:
                    #    raise CrawlerError("!DetailCrawler: during callback.\n%r"%Traceback())
                    pass
        self.contents = None
        del(self.contents)
        return self.output
    
    def fetch(self, page):
        self.tmp_pages = []
        soup = BeautifulSoup(page['wrapper'])
        links = [URL.normalize(urljoin(page['effective_url'], a['href'])) for a in iter(soup.findAll('a')) if a.has_key('href') and a['href'] and re.match(self.url_pattern, a['href'])]
        if not links: return
        if hasattr(self, 'remove_external_duplicate') and self.remove_external_duplicate:
            links = [link for link in links if not self.is_external_duplicate(link)]
        if not links: return
        mario = MarioBatch(callback = self.parser)
        for link in set(links):
            mario.add_job(link)
        mario(10)
        return
    
    def parser(self, response):
        pattern = re.compile('.*%s'%self.wrapper, re.S)
        wrapper = pattern.match(response.body)
        if not wrapper: 
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!DetailCrawler: no matched content.\n%r"%response)
            return
        wrapper = wrapper.groupdict()
        if hasattr(self, 'user_info') and isinstance(self.user_info, dict) and self.user_info:
            for k, v in self.user_info.iteritems():
                wrapper[k] = v
        for k, v in wrapper.items():
            if k.endswith('_number') and isinstance(v, (str, unicode)): wrapper[k] = Converter.toNumber(v)
            elif k.endswith('_datetime') or k.endswith('_at') or k.endswith('_on') and isinstance(v, (str, unicode)): wrapper[k] = Converter.toDatetime(v)
        if self.miss_fields(wrapper): return
        content = md5(repr(wrapper)).hexdigest()
        if content in self.contents: return
        self.contents.append(content)
        page = {'url':response.url, 'effective_url':response.effective_url, 'code':response.code, 'body':response.body, 'size':response.size, 'wrapper':wrapper}
        self.output.append(page)
        self.tmp_pages.append(page)
    
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
            if k not in self.essential_fields or not wrapper[k]: 
                if hasattr(self, 'debug') and self.debug:
                    raise CrawlerError("!DetailCrawler: missing field: %s"%k)
                return True
        return None


class FullRSSCrawler(YAMLObject, Base):
    yaml_tag = u'!FullRSSCrawler'
    def __init__(self, starturl, check_baseurl=None, essential_fields=None, user_info=None, remove_external_duplicate=None, callback=None, executable=None, debug=None):
        self.starturl = starturl
        self.check_baseurl = check_baseurl
        self.essential_fields = essential_fields
        self.user_info = user_info
        self.remove_external_duplicate = remove_external_duplicate
        self.callback = callback
        self.executable = executable
        self.debug = debug

    def __repr__(self):
        return "%s(starturl=%r)" % (self.__class__.__name__, self.starturl)

    def run(self, starturl=None, label=None):
        if not starturl: starturl = self.starturl
        self.iterate_callables(exceptions=('callback', 'executable'))
        if hasattr(self, 'executable') and not self.executable.run(label = label): return self.output
        self.output = []
        if hasattr(self, 'check_baseurl'): check_baseurl = self.check_baseurl
        else: check_baseurl = None
        fullRssParser = FullRssParser(url=starturl, callback=self.parser, check_baseurl=check_baseurl)
        try:
            self.callback.run(self.output)
        except:
            #if hasattr(self, 'debug') and self.debug:
            #    raise CrawlerError(Traceback())
            pass
        return self.output

    def parser(self, response):
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
        if self.miss_fields(wrapper): 
            return
        page = {'url':response['url'], 'effective_url':response['url'], 'code':'200', 'body':'', 'size':len(response['content']), 'wrapper':wrapper}
        self.output.append(page)

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
            if k not in self.essential_fields or not wrapper[k]: 
                if hasattr(self, 'debug') and self.debug:
                    raise CrawlerError("!FullRSSCrawler: missing fieldd: %r"%k)
                return True
        return None


class ThreadCrawler(YAMLObject, Base):
    yaml_tag = u'!ThreadCrawler'
    def __init__(self, urls, executable=None, callback=None, timeout=None):
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
        max_chunk = 10
        total_workers = len(self.urls)
        for i in xrange(0, total_workers, max_chunk):
            urls = self.urls[i:i + max_chunk]
            self.run_workers(urls)
        return self.output
    
    def results(self, response):
        if not response: return
        try:
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
    def __init__(self, yaml_files, callback=None, debug=None):
        self.yaml_files = yaml_files
        self.callback = callback

    def __repr__(self):
        return "%s(yaml_files=%r)" % (self.__class__.__name__, self.yaml_files)

    def run(self):
        self.iterate_callables(exceptions='callback')
        self.output = []
        max_chunk = 10
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
        if hasattr(self, 'concurrent'): concurrent = self.concurrent
        else: concurrent = 10
        total_processes = len(self.yaml_files)
        yaml_files = self.yaml_files
        running_processes = {}
        for yaml_file in yaml_files:
            while len(running_process) >= concurrent:
                running_process = self.recycle(running_process)
            if isinstance(self.callback, dict) and self.callback.has_key('cmd') and self.callback.has_key('timeout'):
                cmd = 'nohup %s %s > /dev/null &'%(self.callback['cmd'], yaml_file)
                timeout = self.callback['cmd']
            else:
                cmd = 'nohup %s %s > /dev/null &'%(self.callback, yaml_file)
                timeout = 0
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            running_processes[cmd] = {'process':p, 'starttime': datetime.datetime.now(), 'timeout':timeout}
        return self.output

    def recycle(self, running_process):
        if not running_process: return []
        remove_cmd = []
        for cmd, process in running_process.items():
            if process['process'].poll() is None: 
                remove_cmd.append(cmd)
                self.output.append((cmd, process['process'].stdout.read()))
            if not process['timeout']: continue
            start = process['starttime']
            now = datetime.datetime.now()
            if (now - start).seconds > process['timeout']:
                os.kill(process['process'].pid, signal.SIGKILL)
                os.waitpid(-1, os.WNOHANG)
                remove_cmd.append(cmd)
                self.output.append((cmd, None))
        if not remove_cmd: return running_process
        for cmd in remove_cmd:
            running_process.remove(cmd)
        return running_process


class Crawlable(YAMLObject, Base):
    yaml_tag = u'!Crawlable'
    def __init__(self, yaml_file, label):
        self.yaml_file = yaml_file
        self.label = label
    
    def __repr__(self):
        return "%s(yaml_files=%r, label=%r)" % (self.__class__.__name__, self.yaml_files, self.label)
    
    def run(self, label=None, yaml_file=None):
        self.iterate_callables()
        data = None
        if not yaml_file: yaml_file = self.yaml_file
        if not lable: lable = self.label
        yaml_file_path = os.path.join(os.getcwd(), yaml_file)
        try:
            fp = file(yaml_file_path, 'r')
            stream = fp.read()
            fp.close()
        except IOError, err:
            if hasattr(self, 'debug') and self.debug:
                raise CrawlerError("!Crawlable: can't read YAML file.\n%r"%Traceback())
            return self.output
        try:
            data = load(stream, Loader=Loader)
        except YAMLError, exc:
            if hasattr(self, 'debug') and self.debug:
                if hasattr(exc, 'problem_mark'):
                    mark = exc.problem_mark
                raise StorageError("!Crawlable: position: (%s:%s)\nYAML file: %r" % (mark.line+1, mark.column+1, yaml_file))
            return self.output
        if not label or not label in data: return self.output
        data = data[label]
        if 'last_updated_at' not in data or 'duration' not in data['duration']:
            self.output = True
            return self.output
        if (time.mktime(time.gmtime()) - float(data['last_updated_at'])) > float(data['duration']):
            self.output = True
        return self.output
    
class CrawlerError(Exception):

    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)