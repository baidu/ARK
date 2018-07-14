# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**lock** 框架提供的分布式锁模块，使用分布式强一致系统实现（当前版本使用Zookeeper)，用于竞态条件控制，如并发度控制，主从等

"""
from are import config
from are import client
from kazoo.client import KazooClient


class Lock(object):
    """
    分布式锁模块
    """
    def __init__(self, name):
        """
        初始化方法

        :param str name: 分布式锁名字
        :return: None
        :rtype: None
        :raises kazoo.interfaces.IHandler.timeout_exception: 连接超时异常
        """
        self._lock_name = name
        self._guardian_id = config.GuardianConfig.get(client.GUARDIAN_ID_NAME)
        self._lock_node_path = "{}/lock".format(self._guardian_id)
        self._lock_node = self._lock_node_path + '/' + self._lock_name
        self._lock_handle = None

        hosts = config.GuardianConfig.get(client.STATE_SERVICE_HOSTS)
        self._zkc = KazooClient(hosts=hosts)
        self._zkc.start()

    def create(self):
        """
        创建分布式锁

        :return: 分布式锁句柄
        :rtype: Kazoo lock
        """
        if not self._lock_handle:
            self._lock_handle = self._zkc.Lock(self._lock_node)
        
        return self._lock_handle

    def delete(self):
        """
        删除分布式锁

        :return: None
        :rtype: None
        :raises kazoo.exceptions.NoNodeError: 锁不存在
        :raises kazoo.exceptions.NotEmptyError: 锁被占用
        :raises kazoo.exceptions.ZookeeperError: Zookeeper连接异常
        """
        if not self._lock_handle:
            self._zkc.delete(self._lock_node)
            self._lock_handle = None

    def obtain(self):
        """
        获得锁，调用该接口后会一直阻塞，直到获得锁

        :return: None
        :rtype: None
        """
        self._lock_handle.acquire()

    def obtain_wait(self, timeout):
        """
        获得锁，调用该接口后，如果在timeout秒内得锁便成功返回，否则抛出异常

        :param int timeout: 争锁超时时间
        :return: 无返回
        :rtype: None
        :raises kazoo.exceptions.LockTimeout: 得锁超时
        """
        self._lock_handle.acquire(timeout=timeout)

    def release(self):
        """
        释放锁

        :return: 无返回
        :rtype: None
        """
        self._lock_handle.release()

    def retain(self):
        """
        锁重入，暂未实现
        """
        pass

