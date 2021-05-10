# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**common** 框架基础公用模块，该模块包含一些提供基本功能的类，方便框架中的其他类继承，目前封装了如下工具：

* ``Singleton`` 单例基类，在本框架中，如果要求仅有一个实例的类必须继承自该类，
继承该基类的派生类对应的实例在整个框架中有且仅一份。如，`LoggerContext`，`LoggerFactory`，`GuardianContext`，`BaseClient`，`DriverFactory`
* ``SingleDict`` 单例字典基类
* ``OpObject`` 运维对象基类，对多种运维对象(如，机器、实例、服务和应用等)公共属性的统一抽象
* ``StringUtil`` 字符串操作工具类，统一封装字符串相关的处理接口
"""

import ark.are.exception as exception
import re
import os
import threading
import traceback
import sys
import warnings
import unittest


class Singleton(object):
    """
    单例基类，封装了类的构造方法
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        构造方法

        :param tuple args: 可变参数
        :param dict kwargs: 关键字参数
        """
        if not cls._instance:
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
        return cls._instance


class SingleDict(dict):
    """
    单例字典，封装类的构造方法和值的存取，为后续扩展或用户使用提供便利
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        构造方法

        :param tuple args: 可变参数
        :param dict kwargs: 关键字参数
        """

        if cls._instance is None:
            cls._instance = super(SingleDict, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __getitem__(self, key):
        """
        单例字典魔术方法，返回键key对应的值

        :param str key: 键参数
        :return: 键对应的值
        :rtype: str
        :raises: KeyError 键对应的值不存在
        """

        try:
            return dict.__getitem__(self, key)
        except KeyError:
            raise exception.EMissingParam("key [{}] not exist".format(key))


class OpObject(object):
    """
    运维对象抽象基类，统一抽象出多种运维对象(如，机器、实例、服务和应用等)的公共属性。
    这些属性包括类型type、名字name和其他相关信息refer。
    """

    def __init__(self, type, name, refer=None):
        """
        初始化方法

        :param str type: 运维对象的类型
        :param str name: 运维对象的名字
        :param str refer: 运维对象的uid
        """
        self.__type = type
        self.__name = name
        self.__refer = refer

    @property
    def type(self):
        """
        返回运维对象类型

        :return: 运维对象类型
        :rtype: str
        """
        return self.__type

    @property
    def name(self):
        """
        返回运维对象名字

        :return: 运维对象名字
        :rtype: str
        """
        return self.__name

    @property
    def refer(self):
        """
        返回运维对象其他相关信息

        :return: 运维对象uid
        :rtype: str
        """
        return self.__refer


class StringUtil(object):
    """
    字符串操作工具类，封装了字符串命名方式（驼峰和下划线）的转换
    """

    @staticmethod
    def camel_to_underline(camel_format):
        """
        驼峰命名格式转下划线命名格式

        :param str camel_format: 驼峰格式的字符串
        :return: 下划线分隔的字符串
        :rtype: str
        """

        # 匹配正则，匹配小写字母或者数字和大写字母的分界位置
        p = re.compile(r'([a-z]|\d)([A-Z])')
        # 这里第二个参数使用了正则分组的后向引用
        underline_format = re.sub(p, r'\1_\2', camel_format).lower()
        return underline_format

    @staticmethod
    def underline_to_camel(underline_format):
        """
        下划线命名格式转驼峰命名格式

        :param str underline_format: 下划线格式的字符串
        :return: 驼峰格式的字符串
        :rtype: str
        """
        # 这里re.sub()函数第二个替换参数用到了一个匿名回调函数，回调函数的参数x为一个匹配对象，返回值为一个处理后的字符串
        camel_format = re.sub(r'(_\w)', lambda x: x.group(1)[1].upper(),
                              underline_format)
        return camel_format


class ParametrizedTestCase(unittest.TestCase):
    """ TestCase classes that want to be parametrized should
        inherit from this class.
    """

    def __init__(self, methodName='runTest', param=None):
        super(ParametrizedTestCase, self).__init__(methodName)
        self.param = param

    @staticmethod
    def parametrize(testcase_klass, param=None):
        """ Create a suite containing all tests taken from the given
            subclass, passing them the parameter 'param'.
        """
        testloader = unittest.TestLoader()
        testnames = testloader.getTestCaseNames(testcase_klass)
        suite = unittest.TestSuite()
        for name in testnames:
            suite.addTest(testcase_klass(name, param=param))
        return suite

# 下列代码用于避免在多线程场景下fork时子进程hung的问题
def _monkeypatch_os_fork_functions():
    """
    Replace os.fork* with wrappers that use ForkSafeLock to acquire
    all locks before forking and release them afterwards.
    """
    builtin_function = type(''.join)
    if hasattr(os, 'fork') and isinstance(os.fork, builtin_function):
        global _orig_os_fork
        _orig_os_fork = os.fork
        os.fork = _os_fork_wrapper
    if hasattr(os, 'forkpty') and isinstance(os.forkpty, builtin_function):
        global _orig_os_forkpty
        _orig_os_forkpty = os.forkpty
        os.forkpty = _os_forkpty_wrapper


_fork_lock = threading.Lock()
_prepare_call_list = []
_prepare_call_exceptions = []
_parent_call_list = []
_child_call_list = []


def _atfork(prepare=None, parent=None, child=None):
    """A Python work-a-like of pthread_atfork.

    Any time a fork() is called from Python, all 'prepare' callables will
    be called in the order they were registered using this function.
    After the fork (successful or not), all 'parent' callables will be called in
    the parent process.  If the fork succeeded, all 'child' callables will be
    called in the child process.
    No exceptions may be raised from any of the registered callables.  If so
    they will be printed to sys.stderr after the fork call once it is safe
    to do so.
    """
    assert not prepare or callable(prepare)
    assert not parent or callable(parent)
    assert not child or callable(child)
    _fork_lock.acquire()
    try:
        if prepare:
            _prepare_call_list.append(prepare)
        if parent:
            _parent_call_list.append(parent)
        if child:
            _child_call_list.append(child)
    finally:
        _fork_lock.release()


def _call_atfork_list(call_list):
    """
    Given a list of callables in call_list, call them all in order and save
    and return a list of sys.exc_info() tuples for each exception raised.
    """
    exception_list = []
    for func in call_list:
        try:
            func()
        except Exception as e:
            exception_list.append(sys.exc_info())
    return exception_list


def _prepare_to_fork_acquire():
    """Acquire our lock and call all prepare callables."""
    _fork_lock.acquire()
    _prepare_call_exceptions.extend(_call_atfork_list(_prepare_call_list))


def _parent_after_fork_release():
    """
    Call all parent after fork callables, release the lock and print
    all prepare and parent callback exceptions.
    """
    prepare_exceptions = list(_prepare_call_exceptions)
    del _prepare_call_exceptions[:]
    exceptions = _call_atfork_list(_parent_call_list)
    _fork_lock.release()
    _print_exception_list(prepare_exceptions, 'before fork')
    _print_exception_list(exceptions, 'after fork from parent')


def _child_after_fork_release():
    """
    Call all child after fork callables, release lock and print all
    all child callback exceptions.
    """
    del _prepare_call_exceptions[:]
    exceptions = _call_atfork_list(_child_call_list)
    _fork_lock.release()
    _print_exception_list(exceptions, 'after fork from child')


def _print_exception_list(exceptions, message, output_file=None):
    """
    Given a list of sys.exc_info tuples, print them all using the traceback
    module preceeded by a message and separated by a blank line.
    """
    output_file = output_file or sys.stderr
    message = 'Exception %s:\n' % message
    for exc_type, exc_value, exc_traceback in exceptions:
        output_file.write(message)
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                                  file=output_file)
        output_file.write('\n')


def _os_fork_wrapper():
    """Wraps os.fork() to run atfork handlers."""
    pid = None
    _prepare_to_fork_acquire()
    try:
        pid = _orig_os_fork()
    finally:
        if pid == 0:
            _child_after_fork_release()
        else:
            # We call this regardless of fork success in order for
            # the program to be in a sane state afterwards.
            _parent_after_fork_release()
    return pid


def _os_forkpty_wrapper():
    """Wraps os.forkpty() to run atfork handlers."""
    pid = None
    _prepare_to_fork_acquire()
    try:
        pid, fd = _orig_os_forkpty()
    finally:
        if pid == 0:
            _child_after_fork_release()
        else:
            _parent_after_fork_release()
    return pid, fd


def patch_logging():
    """
    打logging补丁

    :return:
    """
    _monkeypatch_os_fork_functions()
    logging = sys.modules.get('logging')
    if logging and getattr(logging, 'fixed_for_atfork', None):
        return
    if logging:
        warnings.warn('logging module already imported before patch')
    import logging
    if logging.getLogger().handlers:
        raise exception.ETypeMismatch('logging handlers registered before')

    logging._acquireLock()
    try:
        def fork_safe_createLock(self):
            """
            线程安全的锁
            :param self:
            :return:
            """
            self._orig_createLock()
            _atfork(self.lock.acquire, self.lock.release, self.lock.release)

        logging.Handler._orig_createLock = logging.Handler.createLock
        logging.Handler.createLock = fork_safe_createLock
        _atfork(logging._acquireLock, logging._releaseLock, logging._releaseLock)
        logging.fixed_for_atfork = True
    finally:
        logging._releaseLock()
