#!/usr/bin/env python
# encoding: utf-8
"""
Controls.py

Created by Syd on 2009-11-06.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import re
import datetime, time
from yaml import YAMLObject
from bububa.Lego.Base import Base
from bububa.Lego.Helpers import ThreadPool

class Subprocess(YAMLObject, Base):
    yaml_tag = u'!Subprocess'
    def __init__(self, processes, concurrent):
        self.processes = processes
        self.concurrent = concurrent
    
    def __repr__(self):
        return "%s(processes=%r)" %(self.__class__.__name__, self.processes)
    
    def run(self):
        self.output = None
        processes = self.parse_input(self.processes)
        if not isinstance(processes, (list, set)): return None
        self.output = []
        if hasattr(self, 'concurrent'): concurrent = self.concurrent
        else: concurrent = 10
        total_processes = len(processes)
        processes = self.processes
        running_processes = {}
        for process in processes:
            while len(running_process) >= concurrent:
                running_process = self.recycle(running_process)
            if isinstance(process, dict) and process.has_key('cmd') and process.has_key('timeout'):
                cmd = 'nohup %s > /dev/null &'%process['cmd']
                timeout = process['cmd']
            else:
                cmd = 'nohup %s > /dev/null &'%process
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


class Queue(YAMLObject, Base):
    yaml_tag = u'!Queue'
    def __init__(self, workers):
        self.workers = workers
    
    def __repr__(self):
        return "%s(workers=%r)" % (self.__class__.__name__, self.workers)
    
    def run(self):
        self.output = None
        if not isinstance(self.workers, (list, set)): return None
        self.output = []
        max_chunk = 10
        total_workers = len(self.workers)
        for i in xrange(0, total_workers, max_chunk):
            workers = self.workers[i:i + max_chunk]
            self.run_workers(workers)
        return self.output
    
    def callback(self, response):
        self.output.append(response)
    
    def run_workers(self, workers):
        threadPool = ThreadPool(len(workers))
        for worker in workers:
            threadPool.run(worker.run, callback=self.callback)
        threadPool.killAllWorkers()


class Sequence(YAMLObject, Base):
    yaml_tag = u'!Sequence'
    def __init__(self, steps):
        self.steps = steps
    
    def __repr__(self):
        return "%s(steps=%r)" % (self.__class__.__name__, self.steps)
    
    def run(self):
        self.output = None
        if not isinstance(self.steps, (list, set)): return None
        self.output = []
        for step in self.steps:
            step.run()
            self.output.append(step.output)
        return self.output


class Step(YAMLObject, Base):
    yaml_tag = u'!Step'
    def __init__(self, obj, method, inputs, class_members):
        self.obj = obj
        self.inputs = inputs
        self.method = method
        self.class_members = class_members
    
    def __repr__(self):
        return "%s(obj=%r)" % (self.__class__.__name__, self.obj)
    
    def run(self):
        if hasattr(self, 'method'): method = getattr(self.obj, method)
        else: method = getattr(self.obj, 'run')
        if hasattr(self, 'cycle'):
            self.cycle = self.parse_input(self.cycle)
        if not hasattr(self, 'cycle') or not self.cycle:
            self.output = method()
            return self.output
        if hasattr(self, 'class_members') and isinstance(self.class_members, dict):
            class_members = self.parse_input(self.class_members)
            for k, v in class_members.iteritems():
                self.obj.__dict__[k] = v
        if not hasattr(self, 'inputs'): 
            self.output = method()
        else:
            inputs = self.parse_input(self.inputs)
            self.output = method(inputs)
        return self.output
    

class Loop(YAMLObject, Base):
    yaml_tag = u'!Loop'
    def __init__(self, obj, times, sleep, break_condition):
        self.obj = obj
        self.times = times
        self.sleep = sleep
        self.break_condition = break_condition
    
    def __repr__(self):
        return "%s(obj=%r)" % (self.__class__.__name__, self.obj)
    
    def run(self):
        self.iterate_callables(exceptions=('obj',))
        self.cycle = 0
        self.output = []
        self.current_output = None
        while True:
            if hasattr(self, 'times') and self.times and self.cycle >= self.times: break
            if hasattr(self, 'break_condition') and self.break_condition: break
            try:
                self.obj.__dict__['cycle'] = self.cycle
                self.current_output = self.obj.run()
            except Exception, err:
                #print err
                pass
            self.output.append(self.current_output)
            self.cycle += 1
            if hasattr(self, 'sleep') and isinstance(self.sleep, (int, float, long)):
                time.sleep(self.sleep)
        return self.output


class Mapper(YAMLObject, Base):
    yaml_tag = u'!Mapper'
    def __init__(self, items, key, callback):
        self.items = items
        self.key = key
        self.callback = callback
    
    def __repr__(self):
        return "%s(items=%r, callback=%r)" % (self.__class__.__name__, self.items, self.callback)
    
    def run(self):
        items = self.parse_input(self.items)
        key = self.parse_input(self.key)
        self.output = []
        for v in items:
            if hasattr(self, 'key'):
                self.callback.__dict__[key] = v
                self.output.append(self.callback.run())
            elif isinstance(v, dict):
                for k, i in v.iteritems():
                    self.callback.__dict__[k] = i
                self.output.append(self.callback.run())
        return self.output

    
class If(YAMLObject, Base):
    yaml_tag = u'!If'
    def __init__(self, condition, true_callback, false_callback):
        self.condition = condition
        self.true_callback = true_callback
        self.false_callback = false_callback
    
    def __repr__(self):
        return "%s(condition=%r, true_callback=%r)" % (self.__class__.__name__, self.condition, self.true_callback)
    
    def run(self):
        condition = self.parse_input(self.condition)
        if condition:
            self.output = self.true_callback.run()
        elif hasattr(self, 'false_callback'):
            self.output = self.false_callback.run()
        else:
            self.output = None
        return self.output