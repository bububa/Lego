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
from eventlet import db_pool
from eventlet.db_pool import ConnectTimeout
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


class ConnectionPool(db_pool.TpooledConnectionPool):
    """A pool which gives out saranwrapped MySQLdb connections from a pool
    """
    def __init__(self, *args, **kwargs):
        super(ConnectionPool, self).__init__(MySQLdb, *args, **kwargs)

    def get(self):
        conn = super(ConnectionPool, self).get()
        # annotate the connection object with the details on the
        # connection; this is used elsewhere to check that you haven't
        # suddenly changed databases in midstream while making a
        # series of queries on a connection.
        arg_names = ['host','user','passwd','db','port','unix_socket','conv','connect_timeout',
         'compress', 'named_pipe', 'init_command', 'read_default_file', 'read_default_group',
         'cursorclass', 'use_unicode', 'charset', 'sql_mode', 'client_flag', 'ssl',
         'local_infile']
        # you could have constructed this connectionpool with a mix of
        # keyword and non-keyword arguments, but we want to annotate
        # the connection object with a dict so it's easy to check
        # against so here we are converting the list of non-keyword
        # arguments (in self._args) into a dict of keyword arguments,
        # and merging that with the actual keyword arguments
        # (self._kwargs).  The arg_names variable lists the
        # constructor arguments for MySQLdb Connection objects.
        converted_kwargs = dict([ (arg_names[i], arg) for i, arg in enumerate(self._args) ])
        converted_kwargs.update(self._kwargs)
        conn.connection_parameters = converted_kwargs
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


class DatabaseConnector(db_pool.DatabaseConnector):
    def __init__(self, credentials, *args, **kwargs):
        super(DatabaseConnector, self).__init__(MySQLdb, credentials, conn_pool=db_pool.ConnectionPool, *args, **kwargs)

    def get(self, host, dbname, port=3306):
        key = (host, dbname, port)
        if key not in self._databases:
            new_kwargs = self._kwargs.copy()
            new_kwargs['db'] = dbname
            new_kwargs['host'] = host
            new_kwargs['port'] = port
            new_kwargs.update(self.credentials_for(host))
            dbpool = ConnectionPool(*self._args, **new_kwargs)
            self._databases[key] = dbpool
        return self._databases[key]


class Inserter:
    def __init__(self, host, port, user, passwd, db, table):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.table = table
    
    def re_connect(self):
        for rtry in xrange(0, 3):
            try:
                conn = self.pool_db.get()
                break
            except (AttributeError, ConnectTimeout):
                if rtry >1 : return
                self.connector = DatabaseConnector({'%s:%d'%(self.host, self.port): {'host':self.host,'port':self.port, 'user':self.user, 'passwd':self.passwd}})
                self.pool_db = self.connector.get('%s:%d'%(self.host, self.port), self.db)
        return conn
    
    def insert(self, data):
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
        conn = self.re_connect()
        try:
            c = ConnectionPool.pre_query(conn, True)
            c.execute('INSERT INTO %s (%s) VALUES (%s)'%(self.table, ','.join(keys), ','.join(['%s'] * len(keys))), tuple(values))
            conn.commit()
            c.close()
            return c.lastrowid
        finally:
            try:
                self.pool_db.put(conn)
            except:
                pass
        return None
    
    def update(self, data, where):
        if not isinstance(data, dict): return None
        keys = []
        values = []
        for k, v in data.iteritems():
            if isinstance(v, unicode): v = v.encode('utf-8').decode('latin-1')
            elif isinstance(v, str): v = v.decode('latin-1')
            elif type(v) == type(datetime.utcnow()): v = strftime("%Y-%m-%d %X", datetime.timetuple(v))
            elif type(v) == type(time.localtime()): v = strftime("%Y-%m-%d %X", v)
            keys.append(k + '=%s')
            values.append(v)
        conn = self.re_connect()
        try:
            c = ConnectionPool.pre_query(conn, True)
            c.execute('SELECT id FROM %s WHERE %s'%(self.table, where))
            last_id = c.fetchone()
            c.execute('UPDATE %s () SET %s WHERE %s'%(self.table, ','.join(keys), where), tuple(values))
            conn.commit()
            c.close()
            return last_id['id']
        finally:
            try:
                self.pool_db.put(conn)
            except:
                pass
        return None


class DB:
    def __init__(self, host, port, user, passwd, db, table):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.table = table

    def re_connect(self):
        for rtry in xrange(0, 3):
            try:
                conn = self.pool_db.get()
                break
            except (AttributeError, ConnectTimeout):
                if rtry >1 : return
                self.connector = DatabaseConnector({'%s:%d'%(self.host, self.port): {'host':self.host,'port':self.port, 'user':self.user, 'passwd':self.passwd}})
                self.pool_db = self.connector.get('%s:%d'%(self.host, self.port), self.db)
        return conn

    def iterget(self, query, data=None):
        if not isinstance(query, (str, unicode)): return None
        conn = self.re_connect()
        try:
            c = ConnectionPool.pre_query(conn, True)
            if data: c.execute(query, data)
            else: c.execute(query)
            return ConnectionPool.result_iter(c)
        except:
            pass
        finally:
            try:
                self.pool_db.put(conn)
            except:
                pass
        return None
    
    
    def get(self, query, data=None):
        if not isinstance(query, (str, unicode)): return None
        conn = self.re_connect()
        try:
            c = ConnectionPool.pre_query(conn, True)
            if data: c.execute(query, data)
            else: c.execute(query)
            return c.fetchall()
        except:
            pass
        finally:
            try:
                self.pool_db.put(conn)
            except:
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