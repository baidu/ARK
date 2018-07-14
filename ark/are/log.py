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
import re
import threading
import time
from logging import Handler
from logging import LogRecord

import config
from are.common import Singleton

LOG_ROOT = "LOG_DIR"
LOG_CONF_DIR = "LOG_CONF_DIR"
LOG_MATRIX_ID = "MATRIX_CONTAINER_ID"
LOG_OPERATION_ID = "OPERATION_ID"
LOG_FRAMEWORKS = ("are", "framework", "opal", "extend",)


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


class LimitTimedRotatingFileHandler(logging.handlers.BaseRotatingHandler):
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


class LoggerContext(Singleton):
    """
    logger context，生成log handler
    """

    def __init__(self):
        """
        初始化LOGContext，从ark环境变量和are.conf配置文件提取日志参数，如果用户没有显示配置则使用默认值。
        """
        env = config.GuardianConfig.get_all()
        if LOG_CONF_DIR in env:
            log_conf = env[LOG_CONF_DIR]
        else:
            log_conf = "../conf/"
        self._log_conf = log_conf
        if LOG_ROOT in env:
            log_root = env[LOG_ROOT]
        else:
            log_root = "../log/"
        self._log_root = log_root
        if LOG_MATRIX_ID in env:
            matrix_container_id = env["MATRIX_CONTAINER_ID"]
            parts = matrix_container_id.split(".")
            if len(parts) >= 2:
                log_gid = parts[1]
            else:
                log_gid = "Wrong"
        else:
            log_gid = "None"
        self.log_gid = log_gid
        if LOG_OPERATION_ID in env:
            log_oid = env[LOG_OPERATION_ID]
        else:
            log_oid = "None"
        self.log_oid = log_oid
        self._init_log()

    def _init_log(self):
        """
        加载文件log配置，创建分框架分级别的日志
        """
        log_conf = self._log_conf + "log.conf"
        if not os.path.exists(self._log_root):
            os.makedirs(self._log_root)
        try:
            logging.config.fileConfig(log_conf)
        except Exception as e:
            print "LOGFATAL: Parse log file config %s error: %s," \
                  " use default config" % (log_conf, e)
            # 日志配置加载失败，使用默认配置
            h_defalut = LimitTimedRotatingFileHandler(
                self._log_root + "/run.log", when="D",
                interval=1, maxBytes=200 * 1024 * 1024,
                backupCount=7, encoding="UTF-8")
            formatter = logging.Formatter(
                "%(levelname)s %(asctime)s %(thread)d %(message)s")
            h_defalut.setFormatter(formatter)
            h_defalut.setLevel(logging.INFO)
            # 初始化预定义的顶层framework logger，基于类名的logger都是它们的子类，会被默认设置
            for framework_name in LOG_FRAMEWORKS:
                logger = logging.getLogger(framework_name)
                logger.addHandler(h_defalut)
                logger.setLevel(logging.DEBUG)
            # 初始化用户main logger
            logger = logging.getLogger("main")
            logger.addHandler(h_defalut)
            logger.setLevel(logging.DEBUG)

    def level_convert(self, level_str):
        """
        将字符串转换成日志级别

        :param level_str: 字符串形式的日志级别
        :return: 日志级别
        :rtype: int
        """
        if level_str == "INFO":
            level = logging.INFO
        elif level_str == "WARNING":
            level = logging.WARNING
        elif level_str == "DEBUG":
            level = logging.DEBUG
        elif level_str == "ERROR":
            level = logging.ERROR
        elif level_str == "CRITICAL":
            level = logging.CRITICAL
        elif level_str == "FATAL":
            level = logging.FATAL
        else:
            level = logging.INFO
        return level


class LoggerFactory(Singleton):
    """
    Log 工厂类，用于创建Logger。
    """
    _LOGGERS = dict()
    _CONTEXT = LoggerContext()

    @staticmethod
    def get_logger(cls):
        """
        获取对应cls的Logger，如果不存在则新建。

        :param cls:  Logger作用类
        :return: 用于日志打印的Logger
        """
        if cls not in LoggerFactory._LOGGERS:
            LoggerFactory._init_logger(cls)
        return LoggerFactory._LOGGERS.get(cls)

    @staticmethod
    def _init_logger(cls):
        """
        针对不同类产生不同Logger，线程安全。

        :param cls: Logger作用类
        :return: 无返回值
        """
        lock = threading.RLock()
        if lock.acquire():
            logger = Logger(LoggerFactory._CONTEXT, cls)
            LoggerFactory._LOGGERS[cls] = logger
            lock.release()


class Logger(object):
    """
    日志具体实现类，用户直接调用接口。
    """

    def __init__(self, context, cls):
        """
        初始化成员变量和logger，非框架类添加main前缀统一输出，框架类保持原名
        """
        self._context = context
        self._cls = cls
        # 取出类名部分，如<class '__main__.abc'>转化为__main__.abc
        cls_name = str(cls)[8:-2]
        logger_name = "main" + "." + cls_name
        parts = cls_name.split(".")
        if len(parts) >= 1:
            prefix = parts[0]
            if prefix in LOG_FRAMEWORKS:
                logger_name = cls_name
        # 这里持有logger名称而不是logger对象，避免arkOjbect在pickle时失败
        self._log_name = logger_name

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
        texts, json_args = self._format_request(text, args)
        logger = logging.getLogger(self._log_name)
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
        texts, json_args = self._format_request(text, args)
        logger = logging.getLogger(self._log_name)
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
        texts, json_args = self._format_request(text, args)
        logger = logging.getLogger(self._log_name)
        if len(json_args) > 0:
            logger.log(logging.FATAL, texts, *json_args)
        else:
            logger.log(logging.FATAL, texts)

    def info(self, text, *args):
        """
        打印info日志

        :param obj text: 要输出的文本信息，通过python字符串的%语法可以获得类似c语言printf的效果
        :param args: 格式化字符串中占位符对应的变量值，如果变量是对象则打印成json
        :returns: 无返回
        :rtype: None
        """
        texts, json_args = self._format_request(text, args)
        logger = logging.getLogger(self._log_name)
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
        texts, json_args = self._format_request(text, args)
        logger = logging.getLogger(self._log_name)
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
        texts, json_args = self._format_request(text, args)
        logger = logging.getLogger(self._log_name)
        if len(json_args) > 0:
            logger.log(logging.DEBUG, texts, *json_args)
        else:
            logger.log(logging.DEBUG, texts)

    def tsetoid(self, level, text, oid='None', *args):
        """
        本接口用于获得executor返回的operation_id，用户不需要调用

        :param str oid: executor返回的operation_id，会更新到整个log的context后续打印
        :param int level: 日志级别，支持字符串形式和logging本身级别
        :param obj text: 要输出的文本信息，通过python字符串的%语法可以获得类似c语言printf的效果
        :param args: 格式化字符串中占位符对应的变量值，如果变量是对象则打印成json
        :returns: 无返回
        :rtype: None
        """
        self._context.log_oid = oid
        if isinstance(level, basestring):
            log_level = self._context.level_convert(level)
        else:
            log_level = level
        texts, json_args = self._format_request(text, args)
        logger = logging.getLogger(self._log_name)
        if len(json_args) > 0:
            logger.log(log_level, texts, *json_args)
        else:
            logger.log(log_level, texts)

    def _deal_json(self, args):
        """
        对传入不定参数进行整理，如果某个参数是对象类型，则将其转化为json格式，如果对象成员不可json化则显示其__dict__
        :param args: 任意数量和类型的变量
        :return: 整理过的参数列表
        :rtype: list
        """
        result = []
        for arg in args:
            json_str = str(arg)
            if self._is_dict_class(arg):
                try:
                    json_str = json.dumps(arg.__dict__)
                except TypeError:
                    # 对象包括不能json序列化的成员，这里直接使用str
                    json_str = json.dumps(str(arg.__dict__))
            result.append(json_str)
        return result

    def _is_dict_class(self, obj):
        """
        判断传入的参数的类型是否为一个对象，即来自class实例化，而非内置其它类型

        :param object: 任意类型的变量
        :returns: 是否为对象
        :rtype: bool
        """
        type_str = str(type(obj))
        if type_str.startswith("<class "):
            if hasattr(obj, "__dict__"):
                return True
        return False

    def _format_request(self, text, args):
        """
         统一处理各种级别的用户日志打印请求，添加自定义的处理，目前有:
         1.添加类名[c:]
         2.添加文件名[f:]和行号信息[l:]
         3.将对象格式化为json，这里要求使用标准的logger("%s", obj)形式，obj是对象则转成json
         4.添加guardian_id，日志中为[g:]节省空间
         5.添加operation_id，日志中为[o:]节省空间，注意这个值会随命令执行不断改变，初始值为None

         :param text: 要输出的文本信息，通过python字符串的%语法可以获得类似c语言printf的效果
         :param args: 格式化字符串中占位符对应的变量值，如果变量是对象则打印成json
         :return texts: 整理过的输出信息
         :return json_args: 整理过的列表列表，其中的对象为json形式
         """
        call_stack = inspect.stack()
        if self._cls:
            cls_name = str(self._cls)[8:-2]
            call_file = call_stack[2][1]
            # 提取文件名
            call_line = call_stack[2][2]
        else:
            print "none-------------------"
            cls_name = None
            call_stack = inspect.stack()
            call_file = call_stack[3][1]
            call_line = call_stack[3][2]
        guardian_id = self._context.log_gid
        paths = os.path.split(call_file)
        if len(paths) == 2:
            file_name = paths[1]
        else:
            file_name = call_file
        texts = "[c:{}] [f:{}] [l:{}] [g:{}] [o:{}]: {}". \
            format(cls_name, file_name, call_line, guardian_id,
                   self._context.log_oid, text)
        json_args = self._deal_json(args)
        return texts, json_args


def info(message):
    """
    打印info级别日志
    """
    logger = LoggerFactory.get_logger("info")
    logger.info(message)


def debug(message):
    """
    打印debug级别日志
    """
    logger = LoggerFactory.get_logger("debug")
    logger.debug(message)


def error(message):
    """
    打印error级别日志
    """
    logger = LoggerFactory.get_logger("error")
    logger.error(message)


def fatal(message):
    """
    打印fatal级别日志
    """
    logger = LoggerFactory.get_logger("fatal")
    logger.fatal(message)


def warning(message):
    """
    打印warning级别日志
    """
    logger = LoggerFactory.get_logger("warning")
    logger.warning(message)

