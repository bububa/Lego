#!/usr/bin/env python
# encoding: utf-8
"""
Base.py

Created by Syd on 2009-11-05.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import logging

class Base:
    
    def __init__(self):
        self.output=None
    
    def get(self, attr):
        if hasattr(self, attr):
            return self.__dict__[attr]
        return None
    
    def setup_logger(self, fn, level=logging.DEBUG):
        self.log = logging.getLogger(self.__class__.__name__)
        hdlr = logging.FileHandler(fn)
        FORMAT='%(asctime)s\t%(levelname)s\t%(message)s'
        formatter = logging.Formatter(FORMAT)
        logging.basicConfig(format=FORMAT) # log sur console
        hdlr.setFormatter(formatter)
        self.log.addHandler(hdlr)
        self.log.setLevel(logging.DEBUG)
        
    def run(self):
        pass
        
    def regist(self, params):
        self.default_params(params)
        self.register = params
        
    def default_params(self, params):
        for member in self.__dict__:
            if self.__dict__[member]: continue
            if 'crawler' in params and self.__class__.__name__ in params['crawler'] and member in params['crawler'][self.__class__.__name__]:
                self.__dict__[member] = params['crawler'][self.__class__.__name__][member]
            elif member in params:
                self.__dict__[member] = params[member]
         
    def iterate_callables(self, exceptions=[]):
        if 'output' not in self.__dict__: self.__dict__['output'] = None
        for member in self.__dict__:
            if isinstance(exceptions, (set, tuple, list)) and member in exceptions or isinstance(exceptions, (str, unicode)) and member==exceptions: continue
            try:
                #print self.__class__.__name__, member, self.__dict__['register']
                if 'register' in self.__dict__: self.__dict__[member].regist(self.__dict__['register'])
                self.__dict__[member].run()
                self.__dict__[member] = self.__dict__[member].output
            except Exception, err:
                continue
    
    def parse_input(self, request):
        if isinstance(request, dict):
            inputs = dict([(k, self.iterate_inputs(v)) for k, v in request.items()])
        elif isinstance(request, (list, set)):
            inputs = [self.iterate_inputs(i) for i in request]
        else:
            inputs = self.iterate_inputs(request)
        return inputs

    def iterate_inputs(self, i):
        try:
            i.run()
            return i.output
        except Exception, err:
            return i

    def furthure_parser(self, inputs):
        if not inputs: return {}
        return dict([(k, furthure['parser'].run(inputs)) for k, furthure in self.furthure.items()])