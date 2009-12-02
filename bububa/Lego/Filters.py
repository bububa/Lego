#!/usr/bin/env python
# encoding: utf-8
"""
Filters.py

Created by Syd on 2009-11-04.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import re
from yaml import YAMLObject
from bububa.Lego.Base import Base


class Regx(Base, YAMLObject):
    yaml_tag = u'!Regx'
    def __init__(self, string=None, pattern=None, multiple=False):
        self.string = str(string)
        self.pattern = str(pattern)
        self.multiple = multiple
    
    def __repr__(self):
        return "%s(string=%r, pattern=%r, multiple=%r)" % (self.__class__.__name__, self.string, self.pattern, self.multiple)
        
    def run(self):
        self.iterate_callables()
        if not (isinstance(self.pattern, (str, unicode)) and isinstance(self.string, (str, unicode))): return None
        pattern = re.compile(self.pattern)
        res = pattern.findall(self.string)
        if self.multiple: 
            self.output = res
            return res
        if not res: return None
        self.output = res[0]
        return res[0]
        
    #def output(self):
        #frame = sys._getframe()
        #caller = frame.f_back
        #print 'caller function is : ', caller.f_locals['self'].yaml_file
        
