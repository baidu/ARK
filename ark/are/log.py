# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
*日志模块* 对不同级别的日志提供不同的接口，如info，debug，error，fatal，warining
"""

import fcntl
import inspect
import json
import logging
import logging.config
import logging.handlers
import os
import sys
import re
import threading
import time
import types
import traceback
from logging import Handler
from logging import LogRecord

import config
from ark.are.exception import EMissingParam

LOG_ROOT = "LOG_DIR"
LOG_CONF_DIR = "LOG_CONF_DIR"
LOG_NAME_ARK = "ARK"
LOG_NAME_GUARDIAN = "GUARDIAN"


class NullLogRecord(LogRecord):
    """
    日志输出过程中如果出现本身异常会打印到错误输出，这里将其覆盖掉，不再输出
    """

    def __init__(self):
        """
        初始化
        """
        pass

    def __getattr__(self, attr):
        """
        覆盖默认属性
        """
        return None


class LockException(Exception):
    """
    获取文件锁失败异常
    """
    LOCK_FAILED = 1


class LimitTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """
    扩展了滚动日志handler(BaseRotatingHandler)，支持同时对日志大小和时间进行限制:
    1.在日志按时间滚动的基础上，单个日志的大小不超过指定的大小;
    2.如果日志大小未超限，则日志的滚动方式与TimedRotatingFileHandler相同;
    3.如果日志大小超限，在单个时间周期内日志滚动方式与RotatingFileHandler相同;
    4.如果同时满足了大小超限和时间滚动两个条件，则总是先处理大小超限;
    5.支持多进程打印，多进程打印使用文件锁同步，参考了库`ConcurrentLogHandler <https://pypi.python.org/pypi/ConcurrentLogHandler/0.9.1>`

    * 示例：

        * 日志app.log大小超限，则将其重命名为app.log.1，新日志输出到app.log
        * 如果app.log继续超过大小限制，则将app.log.1重命名为app.log.2，将app.log重命名为app.log.1，新日志输出到app.log
        * 如果时间滚动的条件已经满足，比如按小时滚动，则将已经产生日志均添加时间后缀，即依次：
            * 将app.log重命名为app.log.2017080801
            * 将app.log.1重命名为app.log.2017080801.1
            * 将app.log.2重命名为app.log.2017080801.2
        即小标号的日志总是最新的，而且同一周期内的日志名字中的时间戳字段一致
    另外，去掉对delay参数的处理，参考Issue 18940
    """

    def __init__(self, filename, when='h', interval=1, maxBytes=0,
                 backupCount=0,
                 encoding=None, utc=False):
        """
        初始化handler

        :param filename:文件名
        :param when: 时间滚动的单位，支持s（秒）、m（分钟）、h（小时）、d（天），大小写不敏感，不支持星期（W）和午夜（MIDNIGHT）
        :param interval: 时间滚动间隔，默认1，这个值与when相乘后为实际滚动间隔
        :param maxBytes: 大小上限，单位为字节（bytes），如果日志将要超过该值则进行滚动，设置成0时则不进行大小限制（默认值）
        :param backupCount: 日志保存数量，按时间滚动过的日志数量不超过该值，设置成0时则不进行日志删除（默认值），最大支持9999999
        :param encoding: 文件编码
        :param utc: 是否使用utc时区时间
        """
        logging.handlers.BaseRotatingHandler.__init__(self, filename, 'a',
                                                      encoding)
        self.when = when.upper()
        self.backupCount = backupCount
        self.maxBytes = maxBytes
        self.utc = utc
        self.limit = False
        self.stream_lock = None
        self.count = 0
        if self.when == 'S':
            self.interval = 1  # one second
            self.suffix = "%Y%m%d%H%M%S"
            self.extMatch = r"^\d{4}\d{2}\d{2}\d{2}\d{2}\d{2}(\.\d+)?$"
        elif self.when == 'M':
            self.interval = 60  # one minute
            self.suffix = "%Y%m%d%H%M"
            self.extMatch = r"^\d{4}\d{2}\d{2}\d{2}\d{2}(\.\d+)?$"
        elif self.when == 'H':
            self.interval = 60 * 60  # one hour
            self.suffix = "%Y%m%d%H"
            self.extMatch = r"^\d{4}\d{2}\d{2}\d{2}(\.\d+)?$"
        elif self.when == 'D':
            self.interval = 60 * 60 * 24  # one day
            self.suffix = "%Y%m%d"
            self.extMatch = r"^\d{4}\d{2}\d{2}(\.\d+)?$"
        else:
            raise ValueError(
                "Invalid rollover interval specified: %s" % self.when)
        self.extMatch = re.compile(self.extMatch)
        self.interval = self.interval * interval
        # 查找上次打印日志的时间戳
        if os.path.exists(filename):
            t = os.stat(filename)[8]  # ST_MTIME
        else:
            t = int(time.time())
        self.rolloverAt = self.computeRollover(t)
        # 查找按大小滚动的次数
        self.count = self.getMaxFileSuffix()
        # 滚动冲突标志位
        self._rotateFailed = False
        # 打开文件锁
        self._open_lockfile()

    def _open_lockfile(self):
        """
        打开文件锁，文件锁的命令方式取basename的非log部分，如app.log则锁名为app.lock

        :return: None
        """
        dir_name, base_name = os.path.split(self.baseFilename)
        if base_name.endswith(".log"):
            lock_file = base_name[:-4]
        else:
            lock_file = base_name
        lock_dir = dir_name + "/../data"
        if not os.path.exists(lock_dir):
            os.mkdir(lock_dir)
        lock_name = os.path.join(lock_dir, lock_file) + ".lock"
        self.stream_lock = open(lock_name, "w")

    def _close(self):
        """
        关闭文件流

        :return: None
        """
        if self.stream:
            try:
                if not self.stream.closed:
                    self.stream.flush()
                    self.stream.close()
            finally:
                self.stream = None

    def _lock(self, fd, flags):
        """
        加文件锁，使用fcntl，只支持liunx环境

        :param int fd: 文件描述符
        :param flags: 创建锁，e.g. fcntl.LOCK_EX排他锁
        :return: None
        :raises: IOError: 加锁失败
        """
        try:
            fcntl.flock(fd.fileno(), flags)
        except IOError as exc_value:
            print "LOGWARN: log _lock failed"
            raise LockException(*exc_value)

    def _unlock(self, fd):
        """
        释放文件锁

        :param int fd: 文件描述符
        :return: 无返回值
        """
        fcntl.flock(fd.fileno(), fcntl.LOCK_UN)

    def acquire(self):
        """
        获取文件锁和线程锁，如果滚动失败则关闭文件

        :return: None
        :raises: NullLogRecord 日志输出异常
        """
        # 获得线程锁
        Handler.acquire(self)
        # 处理文件锁，如果stream锁已经close则什么也不做
        if self.stream_lock:
            if self.stream_lock.closed:
                try:
                    self._open_lockfile()
                except Exception:
                    self.handleError(NullLogRecord())
                    self.stream_lock = None
                    return
            self._lock(self.stream_lock, fcntl.LOCK_EX)

    def release(self):
        """
        释放文件和handler线程锁，如果获得文件锁失败，则关闭文件避免冲突。

        :return : None
        """
        try:
            if self._rotateFailed:
                self._close()
        except Exception:
            self.handleError(NullLogRecord())
        finally:
            try:
                if self.stream_lock and not self.stream_lock.closed:
                    self._unlock(self.stream_lock)
            except Exception:
                self.handleError(NullLogRecord())
            finally:
                Handler.release(self)

    def close(self):
        """
        关闭文件流，并释放锁

        :return : None
        """
        try:
            self._close()
            if not self.stream_lock.closed:
                self.stream_lock.close()
        finally:
            self.stream_lock = None
            Handler.close(self)

    def _degrade(self, degrade):
        """
        设置文件冲突标志位

        :param degrade: 冲突标志位，True为冲突，False为正常
        :return: 无
        """
        self._rotateFailed = degrade

    def computeRollover(self, currentTime):
        """
        计算滚动时间

        :return: 无返回值
        """
        return currentTime + self.interval

    def shouldRollover(self, record):
        """
        判断日志是否应该滚动，这里在第一次判断应该触发滚动时会重新关闭打开文件，避免重复滚动其它进程已经滚动的文件

        :return: 是否应该滚动日志
        :rtype: bool False代表不滚动，True代表需要滚动
        """
        if self.stream is None:
            # 没有打开文件，下次再判断
            return False
        if self._shouldRollover():
            self._close()
            self.stream = self._open()
            return self._shouldRollover()
        return False

    def _shouldRollover(self):
        """
        判断日志是否应该滚动，判断条件是先判断文件大小是否超限，否则判断时间戳是否应该滚动

        :return: 是否应该滚动日志
        :rtype: bool False代表不滚动，True代表需要滚动
        """
        # 先判断文件大小，如果大小超过了限制，则触发日志滚动，并设置标志位
        if self.maxBytes > 0:
            self.stream.seek(0, 2)
            if self.stream.tell() >= self.maxBytes:
                self.limit = True
                return True
        # 重置大小超限标志位
        self.limit = False
        # 如果大小没有超限再判断时间，两者不会同时触发
        t = int(time.time())
        if t >= self.rolloverAt:
            return True
        # 不需要滚动
        self._degrade(False)
        return False

    def getMaxFileSuffix(self):
        """
        找出日志大小滚动的次数

        :return: 日志滚动的次数
        :rtype: int
        """
        dirName, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dirName)
        plen = len(baseName) + 1
        maxRe = re.compile(r"^\d{1,7}$")
        maxNum = 0
        for fileName in fileNames:
            if fileName.startswith(baseName) and maxRe.match(fileName[plen:]):
                num = int(fileName[plen:])
                if num > maxNum:
                    maxNum = num
        return maxNum

    def getFilesToDelete(self):
        """
        找出待清理的日志，用于按时间戳切分的情况

        :return: 待清理的文件列表
        :rtype: dict
        """
        dir_name, baseName = os.path.split(self.baseFilename)
        fileNames = os.listdir(dir_name)
        result = []
        prefix = baseName + "."
        plen = len(prefix)
        for fileName in fileNames:
            if fileName[:plen] == prefix:
                suffix = fileName[plen:]
                if self.extMatch.match(suffix):
                    result.append(os.path.join(dir_name, fileName))
        result.sort()
        if len(result) < self.backupCount:
            result = []
        else:
            result = result[:len(result) - self.backupCount]
        return result

    def doRollover(self):
        """
        滚动日志，按大小滚动或者按时间滚动，并且进行日志清理

        :return: 无返回值
        :raises: IOError, OSError 拆分文件异常
        """
        self._close()
        try:
            if self.limit:
                self.limit = False
                # 大小触发的滚动，只可能早于时间触发，所以处理目标是baseFilename
                for i in range(self.count, 0, -1):
                    # 移动之前已经由于大小滚动过的日志
                    lsfn = "%s.%d" % (self.baseFilename, i)
                    ldfn = "%s.%d" % (self.baseFilename, i + 1)
                    if os.path.exists(lsfn) and not os.path.exists(ldfn):
                        os.rename(lsfn, ldfn)
                ldfn = self.baseFilename + ".1"
                if not os.path.exists(ldfn) and os.path.exists(
                        self.baseFilename):
                    os.rename(self.baseFilename, ldfn)
                    self.count += 1
            else:
                # 时间触发的滚动，需要处理baseFilename和大小滚动过的日志
                current_time = int(time.time())
                dstNow = time.localtime(current_time)[-1]
                t = self.rolloverAt - self.interval
                if self.utc:
                    time_tuple = time.gmtime(t)
                else:
                    time_tuple = time.localtime(t)
                    dst_then = time_tuple[-1]
                    if dstNow != dst_then:
                        if dstNow:
                            addend = 3600
                        else:
                            addend = -3600
                        time_tuple = time.localtime(t + addend)
                # 先处理base日志
                dfn = self.baseFilename + "." + time.strftime(self.suffix,
                                                              time_tuple)
                if not os.path.exists(dfn) and os.path.exists(
                        self.baseFilename):
                    os.rename(self.baseFilename, dfn)
                # 再移动有可能已经被大小触发过滚动的日志
                for j in range(self.count, 0, -1):
                    tdfn = "%s.%d" % (dfn, j)
                    tsfn = "%s.%d" % (self.baseFilename, j)
                    if not os.path.exists(tdfn) and os.path.exists(tsfn):
                        os.rename(tsfn, tdfn)
                        self.count -= 1
                # 清理日志
                if 0 < self.backupCount <= 9999999:
                    # 0是不清理标志位，超过9999999之后与时间后缀会冲突
                    for s in self.getFilesToDelete():
                        os.remove(s)
                new_rollover_at = self.computeRollover(current_time)
                while new_rollover_at <= current_time:
                    new_rollover_at = new_rollover_at + self.interval
                self.rolloverAt = new_rollover_at
            self._degrade(False)
        except (IOError, OSError):
            print "LOGWARN in split file:{}".format(self.baseFilename)
            self._degrade(True)
        finally:
            # 重新打开文件，避免冲突
            self.stream = self._open()


# 将新Handler发布到logging库，从而可以通过fileconfig调用
logging.handlers.LimitTimedRotatingFileHandler = LimitTimedRotatingFileHandler


class Logger(object):
    """
    日志具体实现类，用户直接调用接口。
    """

    _LOGGERS = dict()
    _log_gid = ""
    _log_oid = ""

    lv_map = {"INFO": logging.INFO,
              "WARNING": logging.WARNING,
              "DEBUG": logging.DEBUG,
              "ERROR": logging.ERROR,
              "CRITICAL": logging.CRITICAL,
              "FATAL": logging.FATAL}

    @staticmethod
    def _conf_log():
        """
        加载文件log配置，创建分框架分级别的日志
        """
        Logger._log_conf = config.GuardianConfig.get(LOG_CONF_DIR, "../conf/")
        Logger._log_root = config.GuardianConfig.get(LOG_ROOT, "../log/")
        Logger._log_gid = config.GuardianConfig.get(config.GUARDIAN_ID_NAME, "UNKNOWN")
        log_conf = Logger._log_conf + "log.conf"
        if not os.path.exists(Logger._log_root):
            os.makedirs(Logger._log_root)
        try:
            logging.config.fileConfig(log_conf)
        except:
            print "LOGFATAL: Parse log file config %s error, use default config: " \
                  "%s," % (log_conf, traceback.format_exc())
            # 日志配置加载失败，使用默认配置
            h_defalut = LimitTimedRotatingFileHandler(
                Logger._log_root + "/run.log", when="D",
                interval=1, maxBytes=200 * 1024 * 1024,
                backupCount=7, encoding="UTF-8")
            formatter = logging.Formatter(
                "%(levelname)s.%(name)s %(asctime)s %(funcName)s@%(filename)s:%(lineno)d "
                "%(processName)s.%(threadName)s%(message)s", "%y/%m/%d.%H:%M:%S.%f")
            h_defalut.setFormatter(formatter)
            h_defalut.setLevel(logging.INFO)
            # 初始化预定义的ARK框架的logger
            lg = logging.getLogger(LOG_NAME_ARK)
            lg.addHandler(h_defalut)
            lg.setLevel(logging.INFO)
            # 初始化用户GUARDIAN的logger
            lg = logging.getLogger(LOG_NAME_GUARDIAN)
            lg.addHandler(h_defalut)
            lg.setLevel(logging.DEBUG)

    @staticmethod
    def _level_convert(level_str):
        """
        将字符串转换成日志级别

        :param level_str: 字符串形式的日志级别
        :return: 日志级别
        :rtype: int
        """

        if level_str not in Logger.lv_map:
            return logging.INFO
        else:
            return Logger.lv_map[level_str]

    def __new__(cls, *args, **kwargs):
        """
        构造方法

        :param tuple args: 可变参数
        :param dict kwargs: 关键字参数
        """
        if len(cls._LOGGERS) == 0:
            Logger._conf_log()
        if "name" in kwargs:
            name = kwargs["name"]
        elif len(args) >= 1:
            name = args[0]
        else:
            raise EMissingParam("logger init need name.")

        if name not in cls._LOGGERS:
            lock = threading.RLock()
            if lock.acquire():
                logger = super(Logger, cls).__new__(cls, *args, **kwargs)
                cls._LOGGERS[name] = logger
                lock.release()
                return logger
        return cls._LOGGERS.get(name)

    def __init__(self, name):
        """
        初始化成员变量和logger，非框架类添加main前缀统一输出，框架类保持原名
        """
        self._log_name = name

    def tlog(self, level, text, *args):
        """
        记录文本日志信息，较高级别的日志(FATAL)会被收集到总控统一存储以备后续追查。
        实际的文本日志组装和输出是由标准库\
         `logging — Logging facility for Python <https://docs.python.org/2.7/library/logging.html>`_\ 提供的

        :param int level: 日志级别，级别包括：DEBUG < INFO < WARNING < ERROR < CRITICAL
        :param obj text: 要输出的文本信息，通过python字符串的%语法可以获得类似c语言printf的效果
        :param args: 格式化字符串中占位符对应的变量值，如果变量是对象则打印成json
        :returns: 无返回
        :rtype: None
        """
        logger = logging.getLogger(self._log_name)
        texts, json_args = self._format_request(logger.getEffectiveLevel(), text, args)
        if len(json_args) > 0:
            logger.log(level, texts, *json_args)
        else:
            logger.log(level, texts)

    def warning(self, text, *args):
        """
        打印warning日志

        :param obj text: 要输出的文本信息，通过python字符串的%语法可以获得类似c语言printf的效果
        :param args: 格式化字符串中占位符对应的变量值，如果变量是对象则打印成json
        :returns: 无返回
        :rtype: None
        """
        logger = logging.getLogger(self._log_name)
        texts, json_args = self._format_request(logger.getEffectiveLevel(), text, args)
        if len(json_args) > 0:
            logger.log(logging.WARNING, texts, *json_args)
        else:
            logger.log(logging.WARNING, texts)

    def fatal(self, text, *args):
        """
        打印fatal日志

        :param obj text: 要输出的文本信息，通过python字符串的%语法可以获得类似c语言printf的效果
        :param args: 格式化字符串中占位符对应的变量值，如果变量是对象则打印成json
        :returns: 无返回
        :rtype: None
        """
        logger = logging.getLogger(self._log_name)
        texts, json_args = self._format_request(logger.getEffectiveLevel(), text, args)
        if sys.exc_info()[0] is None:
            ei = None
        else:
            ei = True
        if len(json_args) > 0:
            logger.log(logging.FATAL, texts, *json_args, exc_info=ei)
        else:
            logger.log(logging.FATAL, texts, exc_info=ei)

    def info(self, text, *args):
        """
        打印info日志

        :param obj text: 要输出的文本信息，通过python字符串的%语法可以获得类似c语言printf的效果
        :param args: 格式化字符串中占位符对应的变量值，如果变量是对象则打印成json
        :returns: 无返回
        :rtype: None
        """
        logger = logging.getLogger(self._log_name)
        texts, json_args = self._format_request(logger.getEffectiveLevel(), text, args)
        if len(json_args) > 0:
            logger.log(logging.INFO, texts, *json_args)
        else:
            logger.log(logging.INFO, texts)

    def error(self, text, *args):
        """
        打印error日志

        :param obj text: 要输出的文本信息，通过python字符串的%语法可以获得类似c语言printf的效果
        :param args: 格式化字符串中占位符对应的变量值，如果变量是对象则打印成json
        :returns: 无返回
        :rtype: None
        """
        logger = logging.getLogger(self._log_name)
        texts, json_args = self._format_request(logger.getEffectiveLevel(), text, args)
        if len(json_args) > 0:
            logger.log(logging.ERROR, texts, *json_args)
        else:
            logger.log(logging.ERROR, texts)

    def debug(self, text, *args):
        """
        打印debug日志

        :param obj text: 要输出的文本信息，通过python字符串的%语法可以获得类似c语言printf的效果
        :param args: 格式化字符串中占位符对应的变量值，如果变量是对象则打印成json
        :returns: 无返回
        :rtype: None
        """
        logger = logging.getLogger(self._log_name)
        texts, json_args = self._format_request(logger.getEffectiveLevel(), text, args)
        if len(json_args) > 0:
            logger.log(logging.DEBUG, texts, *json_args)
        else:
            logger.log(logging.DEBUG, texts)

    @staticmethod
    def setoid(operation_id):
        """
        设置opeartion-id，仅应在主线程中调用，且只有主线程才会输出opeartion-id
        :param operation_id: 应设置的操作id
        :return: 无返回
        :rtype: None
        """
        Logger._log_oid = operation_id

    @staticmethod
    def clearoid():
        """
        清理掉已经设置的opeartion-id，仅应在主线程中调用
        :return: 无返回
        :rtype: None
        """
        Logger._log_oid = ""

    @staticmethod
    def _deal_json(args):
        """
        对传入不定参数进行整理，如果某个参数是对象类型，则将其转化为json格式，如果对象成员不可json化则显示其__dict__
        :param args: 任意数量和类型的变量
        :return: 整理过的参数列表
        :rtype: list
        """
        result = []
        for arg in args:
            json_str = str(arg)
            # 判断传入的参数的类型是否为一个对象，即来自class实例化，而非内置其它类型
            if str(type(arg)).startswith("<class ") and hasattr(arg, "__dict__"):
                try:
                    json_str = json.dumps(arg.__dict__)
                except TypeError:
                    # 对象包括不能json序列化的成员，这里直接使用str
                    json_str = json.dumps(str(arg.__dict__))
            result.append(json_str)
        return result

    def _format_request(self, level, text, args):
        """
        统一处理各种级别的用户日志打印请求，添加自定义的处理，目前有:

        :param level: 设置的打印日志级别
        :param text: 要输出的文本信息，通过python字符串的%语法可以获得类似c语言printf的效果
        :param args: 格式化字符串中占位符对应的变量值，如果变量是对象则打印成json
        :return texts: 整理过的输出信息
        :return json_args: 整理过的列表列表，其中的对象为json形式
        """
        if threading.current_thread().name == "MainThread" and Logger._log_oid != "":
            texts = "@%s.%s %s# %s" % (Logger._log_gid, Logger._log_oid, Logger._get_call_info(level), text)
        else:
            texts = "@%s %s# %s" % (Logger._log_gid, Logger._get_call_info(level), text)
        json_args = Logger._deal_json(args)
        return texts, json_args

    @staticmethod
    def _formatvalue(value):
        """
        简化各种复杂类型的输出，如类、对象、方法等
        """
        if inspect.isclass(value):
            return '=C{%s}' % value.__name__
        if inspect.ismethod(value):
            return '=M<%s.%s()>' % (value.im_class.__name__, value.__name__)
        if inspect.isfunction(value):
            return '=F<%s()>' % value.__name__
        sc = str(value.__class__)
        if sc.startswith("<class '"):
            return '=I<%s>' % value.__class__.__name__
        return '=' + repr(value)

    @staticmethod
    def _get_call_info(level):
        """
        输出调用日志打印的函数的函数名及形参、实参。仅当配置的日志打印级别为DEBUG时的情况下才会输出这些详细信息
        """
        frames = inspect.stack()
        fr = frames[4][0]
        args_pretty = inspect.formatargvalues(*(inspect.getargvalues(fr)), formatvalue=Logger._formatvalue)
        filename, lineno, funcname, _, _ = inspect.getframeinfo(fr, -1)
        filename = filename.split('/')[-1]
        if level != logging.DEBUG and funcname != "<module>":
            return '%s:%d/%s%s' % (filename, lineno, funcname, args_pretty)
        else:
            return '%s:%d/%s' % (filename, lineno, funcname)


def info(message):
    """
    打印info级别日志
    """
    logger = Logger(LOG_NAME_GUARDIAN)
    logger.info(message)


def debug(message):
    """
    打印debug级别日志
    """
    logger = Logger(LOG_NAME_GUARDIAN)
    logger.debug(message)


def error(message):
    """
    打印error级别日志
    """
    logger = Logger(LOG_NAME_GUARDIAN)
    logger.error(message)


def fatal(message):
    """
    打印fatal级别日志
    """
    logger = Logger(LOG_NAME_GUARDIAN)
    logger.fatal(message)


def fatal_raise(e, message):
    """
    打印fatal级别日志
    """
    logger = Logger(LOG_NAME_GUARDIAN)
    logger.fatal(message)
    raise e


def warning(message):
    """
    打印warning级别日志
    """
    logger = Logger(LOG_NAME_GUARDIAN)
    logger.warning(message)


def i(message):
    """
    打印ARK框架的info级别日志
    """
    logger = Logger(LOG_NAME_ARK)
    logger.info(message)


def d(message):
    """
    打印ARK框架的debug级别日志
    """
    logger = Logger(LOG_NAME_ARK)
    logger.debug(message)


def e(message):
    """
    打印ARK框架的error级别日志
    """
    logger = Logger(LOG_NAME_ARK)
    logger.error(message)


def f(message):
    """
    打印ARK框架的fatal级别日志
    """
    logger = Logger(LOG_NAME_ARK)
    logger.fatal(message)


def w(message):
    """
    打印ARK框架的warning级别日志
    """
    logger = Logger(LOG_NAME_ARK)
    logger.warning(message)


def r(e, message):
    """
    打印ARK框架的fatal级别日志，并raise异常
    """
    logger = Logger(LOG_NAME_ARK)
    logger.fatal(message)
    raise e
