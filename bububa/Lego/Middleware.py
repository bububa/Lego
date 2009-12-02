#!/usr/bin/env python
# encoding: utf-8
"""
Middleware.py

Created by Syd on 2009-11-04.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import re
import hashlib
from dateutil.parser import parse as dateParse
from yaml import YAMLObject
from bububa.Lego.Base import Base
from bububa.SuperMario.Parser import striptags

class Input(YAMLObject, Base):
    yaml_tag = u'!Input'
    def __init__(self, obj, method, member, *args, **kwargs):
        self.obj = obj
        self.args = args
        self.member = member
        self.kwargs = kwargs
        self.method = method
    
    def __repr__(self):
        return "%s(obj=%r)" % (self.__class__.__name__, self.obj)
    
    def run(self):
        args = kwargs = member = None
        if hasattr(self, 'args'): args = self.args
        if hasattr(self, 'kwargs'): kwargs = self.kwargs
        if hasattr(self, 'member'): member = self.parse_input(self.member)
        if hasattr(self, 'method'):
            method = getattr(self.obj, method)
            if not (args or kwargs):
                self.output = method()
            elif args:
                self.output = method(args)
            elif kwargs:
                self.output = method(kwargs)
            return self.output
        if not (args or kwargs):
            self.output = self.obj.get('output')
        elif args:
            self.output = [self.obj.get(arg) for arg in args]
            if isinstance(member, (int, long)): self.output = self.output[member]
        elif kwargs:
            self.output = dict([(k, self.obj.get(arg)) for k in kwargs])
        return self.output


class Decrease(YAMLObject, Base):
    yaml_tag = u'!Decrease'
    def __init__(self, number):
        self.number = number

    def __repr__(self):
        return "%s(number=%r)" % (self.__class__.__name__, self.number)

    def run(self):
        number = self.parse_input(self.number)
        self.output = number - 1
        return self.output
    

class Increase(YAMLObject, Base):
    yaml_tag = u'!Increase'
    def __init__(self, number):
        self.number = number

    def __repr__(self):
        return "%s(number=%r)" % (self.__class__.__name__, self.number)

    def run(self):
        number = self.parse_input(self.number)
        self.output = number + 1
        return self.output
    

class Array(YAMLObject, Base):
    yaml_tag = u'!Array'
    def __init__(self, arr, method, *args):
        self.arr = arr
        self.args = args
        self.method = method
    
    def __repr__(self):
        return "%s(array=%r)" % (self.__class__.__name__, self.arr)
    
    def run(self):
        if not hasattr(self, 'method'): 
            self.output = None
            return None
        method = getattr(self, self.method)
        if not method:
            self.output = None
            return None
        arr = self.parse_input(self.arr)
        if hasattr(self, 'args'): args = self.parse_input(self.args)
        else: args = None
        self.output = method(arr, args)
        return self.output
    
    def member(self, arr, args):
        try:
            return arr[int(args[0])]
        except Exception, err:
            return None
    
    def slice(self, arr, args):
        try:
            return arr[int(args[0]):int(args[1])]
        except:
            return None
    
    def sort(self, arr, args):
        pass
    
    def len(self, arr, args):
        return len(arr)
    
    def merge(self, arr, args):
        try:
            return reduce(list.__add__, arr)
        except:
            return None


class String(YAMLObject, Base):
    yaml_tag = u'!String'
    def __init__(self, base_str, args):
        self.args = args
        self.base_str = base_str
    
    def __repr__(self):
        return "%s(base_str=%r)" % (self.__class__.__name__, self.base_str)
    
    def run(self):
        self.iterate_callables()
        try:
            args = self.parse_input(self.args)
            self.output = self.base_str%args
        except:
            self.output = self.base_str
        return self.output


class ConvertCNToNumber(YAMLObject, Base):
    yaml_tag = u'!ConvertToNumber'
    def __init__(self, string):
        self.string = string
    
    def __repr__(self):
        return "%s(string=%r)" % (self.__class__.__name__, self.string)
    
    def run(self):
        base = ("零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十", "百", "千", "万", "亿")
        self.flag = self.wan = self.yi = 1
        self.output = self.getResult(self.string)
        return self.output
    
    def getCharNo(self, char):
        try:
            return base.index(char)
        except ValueError, err:
            return None
    
    def formatInputString(self, string):
        if string.startswith('十'): return '一%s'%string
        return string
    
    def getResult(self, string):
        string = self.formatInputString(string)
        result = 0
        for s in string:
            temp = self.coep(s)
            if temp == None: return None
            result += temp
        return result
    
    def coep(self, s):
        no = self.getCharNo(s)
        if no == None: return None
        elif no == 0: return 0
        elif no >= 1 and no <= 9: 
            self.flag = 1
            return no * self.flag * self.wan * self.yi
        elif no >=10 and no <= 12:
            self.flag = 10 ** (no - 9)
            return 0
        elif no == 13:
            self.wan = 10000
            return 0
        elif no == 14:
            self.yi = 100000
            self.wan = 10000
            return 0
        else:
            return 0


class ConvertToDatetime(YAMLObject, Base):
    yaml_tag = u'!ConvertToDatetime'
    def __init__(self, string):
        self.string = string
    
    def __repr__(self):
        return "%s(string=%r)" % (self.__class__.__name__, self.string)
    
    def run(self):
        string = self.parse_input(self.string)
        try:
            self.output = dateParse(self.formatInputString(string))
        except:
            self.output = None
        return self.output
    
    def formatInputString(self, string):
        if not string: return ''
        return re.sub('年|月|日', '-', string)


class StripTags(YAMLObject, Base):
    yaml_tag = u'!StripTags'
    def __init__(self, string):
        self.string = string
    
    def __repr__(self):
        return "%s(string=%r)" % (self.__class__.__name__, self.string)
    
    def run(self):
        string = self.string
        for tag in ('head', 'style', 'script', 'object', 'embed', 'applet', 'noframes', 'noscript', 'noembed'):
            pattern = re.compile('<%s*?>.*?</%s>' % (tag, tag), re.S|re.I|re.U)
            string = re.sub(pattern, ' ', string)
        for i in ('onload', 'onmouseover', 'onclick'):
            pattern = re.compile('%s="\s*[^"]*"'%action, re.S|re.I|re.U)
            string = re.sub(pattern,'',string)
        self.output = ''.join(BeautifulSoup(string, convertEntities=BeautifulSoup.HTML_ENTITIES).findAll(text=True))
        return self.output


class MarkDown(YAMLObject, Base):
    yaml_tag = u'!MarkDown'
    def __init__(self, string):
        self.string = string
    
    def __repr__(self):
        return "%s(string=%r)" % (self.__class__.__name__, self.string)
    
    def run(self):
        self.iterate_callables()
        self.output = striptags(self.string)
        return self.output


class Hash(YAMLObject, Base):
    yaml_tag = u'!Hash'
    def __init__(self, string, method):
        self.string = string
        self.method = method
    
    def __repr__(self):
        return "%s(string=%r)" % (self.__class__.__name__, self.string)
    
    def run(self):
        self.iterate_callables()
        if method=='md5':
            self.output = hashlib.md5(string).hexdigest()
        elif method=='sha1':
            self.output = hashlib.sha1(string).hexdigest()
        elif method=='sha224':
            self.output = hashlib.sha224(string).hexdigest()
        elif method=='sha256':
            self.output = hashlib.sha256(string).hexdigest()
        elif method=='sha512':
            self.output = hashlib.sha512(string).hexdigest()
        return self.output