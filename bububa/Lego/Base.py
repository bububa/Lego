#!/usr/bin/env python
# encoding: utf-8
"""
Base.py

Created by Syd on 2009-11-05.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

class Base:
    
    def __init__(self):
        self.output=None
    
    def get(self, attr):
        if hasattr(self, attr):
            return self.__dict__[attr]
        return None
    
    def iterate_callables(self, exceptions=[]):
        if 'output' not in self.__dict__: self.__dict__['output'] = None
        for member in self.__dict__:
            if isinstance(exceptions, (set, tuple, list)) and member in exceptions or isinstance(exceptions, (str, unicode)) and member==exceptions: continue
            try:
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

