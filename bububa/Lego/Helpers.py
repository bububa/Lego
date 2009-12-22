#!/usr/bin/env python
# encoding: utf-8
"""
Helpers.py

Created by Syd on 2009-11-07.
Copyright (c) 2009 __ThePeppersStudio__. All rights reserved.
"""

import sys
import os
import signal
import time
import datetime
import threading
import Queue
import MySQLdb
import DBUtils.PersistentDB
from DBUtils.SteadyDB import connect
from dateutil.parser import parse as dateParse
from bububa.SuperMario.utils import Traceback

class ThreadPool:

    def __init__(self,maxWorkers = 10):
        self.tasks = Queue.Queue()
        self.workers = 0
        self.working = 0
        self.maxWorkers = maxWorkers
        self.allKilled = threading.Event()
        self.countLock = threading.RLock()
        
        self.allKilled.set()
        

    def run(self, target, callback=None, *args, **kargs):
        """ starts task.
            target = callable to run with *args and **kargs arguments.
            callback = callable executed when target ends
                       callback sould accept one parameter where target's
                       return value is passed.
                       If callback is None it's ignored.
        """
        self.countLock.acquire()
        if not self.workers:
            self.addWorker()
        self.countLock.release()
        self.tasks.put((target,callback,args,kargs))
        
        
    def setMaxWorkers(self,num):
        """ Sets the maximum workers to create.
            num = max workers
                  If number passed is lower than active workers 
                  it will kill workers to match that number. 
        """
        self.countLock.acquire()
        self.maxWorkers = num
        if self.workers > self.maxWorkers:
            self.killWorker(self.workers - self.maxWorkers)
        self.countLock.release()


    def addWorker(self,num = 1):
        """ Add workers.
            num = number of workers to create/add.
        """
        for x in xrange(num):
            self.countLock.acquire()
            self.workers += 1
            self.allKilled.clear()
            self.countLock.release()        
            t = threading.Thread(target = self.__workerThread)
            t.setDaemon(True)
            t.start()


    def killWorker(self,num = 1):
        """ Kill workers.
            num = number of workers to kill.
        """
        self.countLock.acquire()
        if num > self.workers:
            num = self.workers
        self.countLock.release()
        for x in xrange(num):
            self.tasks.put("exit")            
            

    def killAllWorkers(self, wait=None):
        """ Kill all active workers.
            wait = seconds to wait until last worker ends
                   if None it waits forever.
        """
        
        self.countLock.acquire()
        self.killWorker(self.workers)
        self.countLock.release()
        self.allKilled.wait(wait)


    def __workerThread(self):
        while True:
            try:
                task = self.tasks.get(timeout=2)
            except:
                break
            # exit is "special" tasks to kill thread
            if task == "exit":
                break
            
            self.countLock.acquire()
            self.working += 1
            if (self.working >= self.workers) and (self.workers < self.maxWorkers): # create thread on demand
                self.addWorker()
            self.countLock.release()
    
            fun,cb,args,kargs = task
            try:
                ret = fun(*args,**kargs)
                if cb:
                    cb(ret)
            except Exception, err:
                print Traceback()
            self.countLock.acquire()
            self.working -= 1
            self.countLock.release()
                
        self.countLock.acquire()
        self.workers -= 1
        if not self.workers:
            self.allKilled.set()
        self.countLock.release()


# TODO: Maybe Interval should be a multiply of Precision.
class Job:
    Interval = 0
    Elapsed = 0
    JobFunction = None
    Force = False

class JobController(threading.Thread):

    def __init__(self,precision=1.0):
        threading.Thread.__init__(self)
        self.__StopIt = False
        self.__Jobs = {}
        self.__JobCounter = 0
        self.__Precision = precision
        self.__JobsLock = threading.Lock()

    def run(self):

        while(1):
            jobfuncs = []

            if self.__StopIt == True:
                return

            self.__JobsLock.acquire()
            try:
                for key,val in self.__Jobs.items():
                    val.Elapsed += self.__Precision
                    if val.Elapsed >= val.Interval or val.Force:
                        val.Elapsed = 0
                        val.Force = False

                        # copy to another list for
                        # not acquirirng JobsLock()
                        # while calling the JobsFunction 
                        jobfuncs.append(val.JobFunction)
            finally:
                self.__JobsLock.release()

            # now invoke the job functions
            for jobfunc in jobfuncs:
                try:
                    jobfunc()
                # no unhandled exceptions allowed
                except Exception,e:
                    print "JOBERROR:"+str(e)
            time.sleep(self.__Precision)


    def JcStart(self):
        self.start()

    def JcStop(self):
        self.__StopIt = True

    def __AssertBounds(self,val,min,max):
        if (val < min) or (val > max):
            raise AssertionError, "value not in bounds" \
                      "["+str(val)+"]["+str(min)+"]["+str(max)+"]"

    def JcAddJob(self,interval,jobfunction):

        self.__AssertBounds(interval,self.__Precision,float(sys.maxint))

        # create a job object
        ajob = Job()
        ajob.Interval = interval
        ajob.Elapsed = 0
        ajob.JobFunction = jobfunction
        ajob.Force = False
        # append it to jobs dict
        self.__JobCounter += 1

        self.__JobsLock.acquire()
        try:
            self.__Jobs[self.__JobCounter] = ajob
        finally:
            self.__JobsLock.release()

        return self.__JobCounter

    def JcRemoveJob(self,jobid):
        self.__JobsLock.acquire()
        try:
            del self.__Jobs[jobid]
        finally:
            self.__JobsLock.release()

    def JcForceJob(self,jobid):
        self.__JobsLock.acquire()
        try:            
            self.__Jobs[jobid].Force = True
        finally:
            self.__JobsLock.release()

    def JcChangeJob(self,jobid,interval,jobfunction):

        self.__AssertBounds(interval,self.__Precision,sys.maxint)

        self.__JobsLock.acquire()
        try:  
            self.__Jobs[jobid].Interval = interval
            self.__Jobs[jobid].JobFunction = jobfunction
        finally:
            self.__JobsLock.release()

# EXAMPLE
#A simple example is like this:
#jc = JobController(60.0) # precision is 60 secs, so main JobController will be invoked
                         # per 60 secs.
#jc.JcAddJob(60*30,CheckConnectionJob)
#jc.JcAddJob(60*5,CheckStatisticsJob)
#jc.start()
#jc.JcAddJob(60*5,CheckPingJob)

def timeout_command(command, timeout):
    """call shell-command and either return its output or kill it
    if it doesn't normally exit within timeout seconds and return None"""

    cmd = command.split(" ")
    start = datetime.datetime.now()
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    while process.poll() is None:
        if not timeout: continue
        time.sleep(0.1)
        now = datetime.datetime.now()
        if (now - start).seconds > timeout:
            os.kill(process.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
            return None

    return process.stdout.read()


class DB:
    def __init__(self, host, port, user, passwd, db, table):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.table = table

    def re_connect(self, is_persist=None):
        if is_persist:
            persist=DBUtils.PersistentDB.PersistentDB(MySQLdb, 100, host=self.host,user=self.user,passwd=self.passwd,db=self.db)
            conn = persist.connection()
            return conn
        else:
            conn = connect(MySQLdb, 1000, host=self.host,user=self.user,passwd=self.passwd,db=self.db)
            return conn
    
    @staticmethod
    def pre_query(conn, dict_cursor=False):
        if not conn: return None
        if dict_cursor:
            cursor = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
        else:
            cursor = conn.cursor()
        cursor.execute("SET NAMES utf8")
        cursor.execute("SET CHARACTER SET utf8")
        cursor.execute("SET COLLATION_CONNECTION='utf8_general_ci'")
        return cursor

    @staticmethod
    def result_iter(cursor, arraysize=5000):
        while True:
            results = cursor.fetchmany(arraysize)
            if not results: break
            for result in results:
                yield result

    def iterget(self, query, data=None, conn=None, is_persist=None):
        if not isinstance(query, (str, unicode)): return None
        if not conn: conn = self.re_connect(is_persist)
        c = DB.pre_query(conn, True)
        if data: c.execute(query, data)
        else: c.execute(query)
        return DB.result_iter(c)
        return None
    
    
    def get(self, query, data=None, conn=None, is_persist=None):
        if not isinstance(query, (str, unicode)): return None
        if not conn: conn = self.re_connect(is_persist)
        c = DB.pre_query(conn, True)
        if data: c.execute(query, data)
        else: c.execute(query)
        return c.fetchall()
    
    def insert(self, data, conn=None, is_persist=None):
        if not isinstance(data, dict): return None
        keys = []
        values = []
        for k, v in data.iteritems():
            if isinstance(v, unicode): v = v.encode('utf-8').decode('latin-1')
            elif isinstance(v, str): v = v.decode('latin-1')
            elif type(v) == type(datetime.datetime.utcnow()): v = strftime("%Y-%m-%d %X", datetime.timetuple(v))
            elif type(v) == type(time.localtime()): v = strftime("%Y-%m-%d %X", v)
            keys.append(k)
            values.append(v)
        if not conn: conn = self.re_connect(is_persist)
        c = DB.pre_query(conn, True)
        c.execute('INSERT INTO %s (%s) VALUES (%s)'%(self.table, ','.join(keys), ','.join(['%s'] * len(keys))), tuple(values))
        conn.commit()
        c.close()
        conn.close()
        return c.lastrowid
    
    def remove(self, where, conn=None, is_persist=None):
        if not conn: conn = self.re_connect(is_persist)
        c = DB.pre_query(conn, True)
        c.execute('DELETE FROM %s WHERE %s'%(self.table, where))
        conn.commit()
        c.close()
        conn.close()
        return None
    
    def update(self, data, where, conn=None, is_persist=None):
        if not isinstance(data, dict): return None
        keys = []
        values = []
        for k, v in data.iteritems():
            if isinstance(v, unicode): v = v.encode('utf-8').decode('latin-1')
            elif isinstance(v, str): v = v.decode('latin-1')
            elif type(v) == type(datetime.datetime.utcnow()): v = strftime("%Y-%m-%d %X", datetime.timetuple(v))
            elif type(v) == type(time.localtime()): v = strftime("%Y-%m-%d %X", v)
            keys.append(k + '=%s')
            values.append(v)
        if not conn: conn = self.re_connect(is_persist)
        try:
            c = DB.pre_query(conn, True)
            c.execute('SELECT * FROM %s WHERE %s'%(self.table, where))
            last_id = c.fetchone()
            c.execute('UPDATE %s SET %s WHERE %s'%(self.table, ','.join(keys), where), tuple(values))
            conn.commit()
            c.close()
            conn.close()
            try:
                return last_id['id']
            except:
                return last_id
        except Exception, err:
            #print Traceback()
            pass
        return None


class Converter:
    def __init__(self):
        pass
    
    @staticmethod
    def toDatetime(string):
        try:
            return dateParse(self.formatInputString(string))
        except:
            return None

    def formatInputString(self, string):
        if not string: return ''
        return re.sub('年|月|日', '-', string)
    
    @staticmethod
    def toNumber(string):
        base = ("零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十", "百", "千", "万", "亿")
        self.flag = self.wan = self.yi = 1
        return self.getResult(string)

    def getCharNo(self, char):
        try:
            return base.index(char)
        except ValueError, err:
            return None

    def formatNumberInputString(self, string):
        if string.startswith('十'): return '一%s'%string
        return string

    def getResult(self, string):
        string = self.formatNumberInputString(string)
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