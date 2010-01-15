#!/usr/bin/env python
# encoding: utf-8
"""
Lego.py

Created by Syd on 2009-11-04.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import sys
import os
import logging
from yaml import YAMLObject
from yaml import YAMLError
from yaml import load, dump
try:
    from yaml import CLoader as Loader
    from yaml import CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

from bububa.Lego.Base import Base
from bububa.Lego.Middleware import *
from bububa.Lego.Filters import *
from bububa.Lego.Controls import *
from bububa.Lego.Crawler import *
from bububa.Lego.Storages import *
from bububa.Lego.Collective import *
from bububa.Lego.Inserter import *



class Lego(YAMLObject, Base):
    yaml_tag = u'!Lego'
    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
    
    def __repr__(self):
        return "%s(yaml_file=%r)" % (self.__class__.__name__, self.yaml_file)
        
    def run(self, yaml_file=None):
        if not yaml_file: yaml_file = self.yaml_file
        if hasattr(self, 'logger'):
            self.setup_logger(self.logger['filename'])
        data = self.read_yaml_file(yaml_file)
        if not data.has_key('run'):
            if hasattr(self, 'log'): self.log.error('No "run" defined in YAML file: %r'%yaml_file)
            raise LegoError('No "run" defined in YAML file: %r'%yaml_file)
        data['run'].run()
        self.output = data['run'].output
        return self.output
    
    def read_yaml_file(self, yaml_file):
        yaml_file_path = os.path.join(os.getcwd(), yaml_file)
        try:
            fp = file(yaml_file_path, 'r')
            stream = fp.read()
            fp.close()
        except IOError, err:
            if hasattr(self, 'log'): self.log.error("Can't open YAML file: %r"%yaml_file)
            raise LegoError("Can't open YAML file: %r"%yaml_file)
        try:
            data = load(stream, Loader=Loader)
        except YAMLError, exc:
            if hasattr(exc, 'problem_mark'):
                mark = exc.problem_mark
                if hasattr(self, 'log'): self.log.exception("YAMLError position: (%s:%s)\nYAML file: %r" % (mark.line+1, mark.column+1, yaml_file))
                raise LegoError("YAMLError position: (%s:%s)\nYAML file: %r" % (mark.line+1, mark.column+1, yaml_file))
        if not data:
            if hasattr(self, 'log'): self.log.error("Empty YAML file: %r"%yaml_file)
            raise LegoError("Empty YAML file: %r"%yaml_file)
        return data
    

class Init(YAMLObject, Base):
    yaml_tag = u'!Init'
    def __init__(self, obj, inputs, params=None):
        self.obj = obj
        self.inputs = inputs
        self.params = params

    def __repr__(self):
        return "%s(obj=%r)" % (self.__class__.__name__, self.obj)

    def run(self, inputs=None):
        if not inputs: inputs = self.inputs
        ins = self.obj.__class__()
        if hasattr(self, 'params') and self.params:
            ins.default_params(self.params)
        self.output = ins.run(inputs)
        return self.output
    

class LegoError(Exception):
    
    def __init__(self, value):
        self.parameter = value
        
    def __str__(self):
        return repr(self.parameter)

def main():
    lego = Lego('../yaml/BaseCrawlerDemo.yaml')
    lego.run()

if __name__ == '__main__':
    main()