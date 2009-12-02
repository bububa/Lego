#!/usr/bin/env python
# encoding: utf-8
"""
Storages.py

Created by Syd on 2009-11-08.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""
import os
import datetime
try:
    import cPickle as pickle
except:
    import pickle
from hashlib import md5
import time
from datetime import datetime
from yaml import YAMLObject
from yaml import YAMLError
from yaml import load, dump
try:
    from yaml import CLoader as Loader
    from yaml import CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
from bububa.Lego.Base import Base
try:
    from bububa.Lego.MongoDB import Page
except:
    pass
from bububa.SuperMario.utils import Traceback

class File(YAMLObject, Base):
    yaml_tag = u'!File'
    def __init__(self, method, filename, data, debug=None):
        self.method = method
        self.filename = filename
        self.data = data
        self.debug = debug
    
    def __repr__(self):
        return "%s(method=%r, filename=%r)" % (self.__class__.__name__, self.filename)
    
    def run(self):
        self.iterate_callables()
        method = getattr(self, self.method)
        if not method: 
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!File: invalid method.\n%r"%self.method)
            self.output = None
        elif hasattr(self, 'data'): self.output = method(self.filename, data)
        else: self.output = method(self.filename)
        return self.output
        
    def read_plain(self, filename):
        try:
            fp = file(os.path.join(os.getcwd(), filename), 'r')
            content = fp.read()
            fp.close()
            return content
        except IOError, err:
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!Document: failed to read file.\n%r"%Traceback())
            return ''
    
    def readlines(self, filename):
        try:
            fp = file(os.path.join(os.getcwd(), filename), 'r')
            lines = fp.readlines()
            fp.close()
            return lines
        except IOError, err:
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!Document: failed to read file.\n%r"%Traceback())
            return ''
    
    def read_pickle(self, filename):
        try:
            return pickle.load(os.path.join(os.getcwd(), filename))
        except:
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!Document: failed to load pickle file.\n%r"%Traceback())
            return ''
    
    def write_plain(self, filename, data):
        try:
            fp = file(os.path.join(os.getcwd(), filename), 'w')
            fp.write(data)
            fp.close()
            return data
        except IOError, err:
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!Document: failed to write file.\n%r"%Traceback())
            return None
    
    def write_pickle(self, filename, data):
        try:
            pickle.dump(data, os.path.join(os.getcwd(), filename))
            return data
        except:
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!Document: failed to write pickle.\n%r"%Traceback())
            return None


class Document(YAMLObject, Base):
    yaml_tag = u'!Document'
    def __init__(self, label, page, method, callback=None, debug=None):
        self.label = label
        self.page = page
        self.method = method
        self.callback = callback
        self.debug = debug
    
    def __repr__(self):
        return "%s(page=%r)" % (self.__class__.__name__, self.page)
    
    def run(self, page=None):
        self.iterate_callables(exceptions='callback')
        if not page: page = self.page
        method = getattr(self, self.method)
        if not method: self.output = None
        self.output = method(page)
        try:
            self.callback.run()
        except:
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!Document: failed during callback.\n%r"%Traceback())
        return self.output
    
    def batch_write(self, pages):
        if not pages or not isinstance(pages, (list, set)): return None
        for page in pages:
            self.write(page)
        return pages
        
    def write(self, page):
        for k, v in page.items():
            if isinstance(page[k], str): page[k] = page[k].decode('utf-8')
        if isinstance(self.label, str): label = self.label.decode('utf-8')
        else: label = self.label
        url_hash = md5(page['effective_url']).hexdigest().decode('utf-8')
        for k, v in page['wrapper'].items():
            if isinstance(v, unicode): page['wrapper'][k] = v.encode('utf-8')
        wrapper = pickle.dumps(page['wrapper']).decode('utf-8')
        pageObj = Page.get_from_id(url_hash)
        if not pageObj:
            pageObj = Page()
            pageObj['_id'] = url_hash
            pageObj.label = label
            pageObj.url = page['url']
            pageObj.effective_url = page['effective_url']
            pageObj.url_hash = url_hash
            pageObj.page = page['body']
            pageObj.wrapper = wrapper
        elif md5(wrapper).hexdigest() != md5(pageObj.wrapper).hexdigest():
            pageObj.last_updated_at = datetime.utcnow()
            pageObj.label = label
            pageObj.url = page['url']
            pageObj.body = page['body']
            pageObj.wrapper = wrapper
            pageObj.updated_times += 1
            days = (pageObj.last_updated_at - pageObj.inserted_at).days + 1
            pageObj.update_freq = 1.0 * pageObj.updated_times / days
            pageObj.rank = int(30.0 * pageObj.update_freq)
        else:
            return page
        pageObj.save()
        return page
    
    def read(self, url):
        url_hash = md5(url).hexdigest().decode('utf-8')
        pageObj = Page.get_from_id(url_hash)
        if not pageObj: 
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!Document: don't have page.\nurl: %r"%url)
            return None
        wrapper = pickle.loads(pageObj.wrapper)
        return {'url':pageObj.url, 'effective_url':pageObj.effective_url, 'body':pageObj.body, 'wrapper':wrapper}


class YAMLStorage(YAMLObject, Base):
    yaml_tag = u'!YAMLStorage'
    def __init__(self, yaml_file, method, data=None, label=None, callback=None, debug=None):
        self.yaml_file = yaml_file
        self.data = data
        self.label = label
        self.method = method
        self.callback = callback
        self.debug = debug

    def __repr__(self):
        return "%s(yaml_file=%r, method=%r)" % (self.__class__.__name__, self.yaml_file, self.method)
    
    
    def run(self):
        self.iterate_callables(exceptions='callback')
        method = getattr(self, self.method)
        if not method: self.output = None
        if hasattr(self, 'data'): 
            if hasattr(self, 'label') and self.label: self.output = method(self.data, self.label)
            else: self.output = method(self.data)
        elif hasattr(self, 'label') and self.label: self.output = method(label=self.label)
        else: self.output = method()
        try:
            self.callback.run()
        except:
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!YAMLStorage: failed during callback.\n%r"%Traceback())
        return self.output
    
    def read(self, data=None, label=None):
        data = None
        yaml_file_path = os.path.join(os.getcwd(), self.yaml_file)
        try:
            fp = file(yaml_file_path, 'r')
            stream = fp.read()
            fp.close()
        except IOError, err:
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!YAMLStorage: can't read YAML file.\n%r"%Traceback())
            return None
        try:
            data = load(stream, Loader=Loader)
        except YAMLError, exc:
            if hasattr(self, 'debug') and self.debug:
                if hasattr(exc, 'problem_mark'):
                    mark = exc.problem_mark
                raise StorageError("!YAMLStorage: position: (%s:%s)\nYAML file: %r" % (mark.line+1, mark.column+1, yaml_file))
            return None
        if label and label in data: return data[label]
        return data
    
    def write(self, data=None, label=None):
        if not isinstance(data, dict) or not data: 
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!YAMLStorage: invalide input data.")
            return None
        update_data = self.data
        old_data = self.read()
        if not old_data: old_data = {}
        if label and label not in data: old_data[label] = {}
        for k, v in update_data.iteritems():
            old_data[label][k] = v
        try:
            stream = dump(old_data, Dumper=Dumper)
        except:
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!YAMLStorage: failed to load YAML file.\n%r"%Traceback())
        yaml_file_path = os.path.join(os.getcwd(), self.yaml_file)
        try:
            fp = file(yaml_file_path, 'w')
            fp.write(stream)
            fp.close()
        except IOError, err:
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!YAMLStorage: failed to write YAML file.\n%r"%Traceback())
            return None
        return old_data
    
    def update_config(self, data=None, label=None):
        old_data = self.read(label=label)
        if not old_data: 
            if hasattr(self, 'debug') and self.debug:
                raise StorageError("!YAMLStorage: empty config file.")
            return None
        if not data:
            if old_data.has_key('max_depth') and old_data['max_depth'] > 3: old_data['max_depth']-=1
            if old_data.has_key('end_no') and old_data.has_key('start_no') and old_data.has_key('step'):
                page = (old_data['end_no'] - old_data['start_no']) / old_data['step']
                if page > 5: old_data['end_no'] = old_data['start_no'] + old_data['step'] * (page-1)
            if old_data.has_key('duration'): old_data['duration'] += 10*60
        else:
            if old_data.has_key('max_depth'): old_data['max_depth']+=1
            if old_data.has_key('end_no') and old_data.has_key('start_no') and old_data.has_key('step'):
                page = (old_data['end_no'] - old_data['start_no']) / old_data['step']
                old_data['end_no'] = old_data['start_no'] + old_data['step'] * (page+1)
                old_data['duration'] -= 10*60
            if old_data.has_key('duration') and old_data['duration'] < 0: old_data['duration'] = 0
        old_data['last_updated_at'] = time.mktime(time.gmtime())
        return self.write(old_data, label)


class StorageError(Exception):

    def __init__(self, value):
        self.parameter = value

    def __str__(self):
        return repr(self.parameter)