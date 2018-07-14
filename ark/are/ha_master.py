# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**ha_master** 模块用于智能运维机器人的主从切换，以保证服务的高可用

* ``HAMaster`` 封装基于zookeeper的主从高可用操作，为了保证智能运维机器人的高可用，本框架采用一主多备的方式，当主机器人因为异常挂掉，从机器人能及时感知到异常并切换成主机器人，取出存在zookeeper中的运行数据，在保证异常安全的条件下，继续提供服务

"""
import time
import traceback

from are import config
from are import log
from are.client import ZkClient

from kazoo import client


GUARDIAN_ID_NAME = "GUARDIAN_ID"
INSTANCE_ID_NAME = "INSTANCE_ID"


class HAMaster(object):
    """
    主从选举客户端类, 封装基于zookeeper的主从选举和相应状态变更
    """
    def __init__(self, start_scheduler_func, stop_scheduler_func):
        """
        初始化方法
        
        :param func start_scheduler_func: 实例成为主时回调函数
        :param func stop_scheduler_func: 实例成为从时回调函数
        """
        self.path = "/{}/alive_clients".format(
            config.GuardianConfig.get(GUARDIAN_ID_NAME))
        self._start_scheduler_func = start_scheduler_func
        self._stop_scheduler_func = stop_scheduler_func
        self.zk = ZkClient()
        self.zk.client.add_listener(self.state_listener)
        self.is_leader = False

    def create_instance(self):
        """
        创建临时有序节点

        :return: None
        :raises: kazoo.exceptions.NodeExistsError 节点存在
        :raises: kazoo.exceptions.ZookeeperError 连接异常
        """
        node_path = self.path + "/{}#".\
            format(config.GuardianConfig.get(INSTANCE_ID_NAME))
        self.zk.client.create(path=node_path, value="",
                              ephemeral=True, sequence=True, makepath=True)

    def choose_master(self):
        """
        guardian选主: master节点下, 所有ephemeral+sequence类型的节点中, 编号最小的获得领导权.

        :return: None
        """
        instance_list = self.zk.client.get_children(
            path=self.path, watch=self.event_watcher)
        instance = sorted(instance_list)[0].split("#")[0]
        # 本实例获得领导权
        if str(instance) == str(config.GuardianConfig.get(INSTANCE_ID_NAME)):
            if not self.is_leader:
                self._start_scheduler_func()
                self.is_leader = True
                log.info("I am new master, scheduler")
            else:
                log.info("I am new master, and is old master, "
                         "no longer rescheduler")
        # 本实例没有获得领导权
        else:
            if self.is_leader:
                self._stop_scheduler_func()
                self.is_leader = False
                log.info("I am slave, stop scheduler")
            else:
                log.info("I am slave, and is old slave, "
                         "no longer stop scheduler")
        log.info("choose master finished")

    def state_listener(self, state):
        """
        监听会话状态

        :param state:
        :return: None
        :raises: Exception通用异常
        """
        if state == client.KazooState.LOST:
            log.info("guardian instance state lost")
            while True:
                try:
                    self.create_instance()
                    self.zk.client.get_children(
                        path=self.path, watch=self.event_watcher)
                    log.info("guardian instance state recreate finished")
                    break
                except Exception as e:
                    traceback.print_exc("create instance err:{}".format(e))
                time.sleep(1)
        elif state == client.KazooState.SUSPENDED:
            log.info("guardian instance state suspended")
        elif state == client.KazooState.CONNECTED:
            log.info("guardian instance state connected")
        else:
            log.info("guardian instance state unrecognized, state:{}".format(state))

    def event_watcher(self, event):
        """
        监听事件

        :param ZnodeStat instance event: 节点状态事件
        :return: None
        :raises: None
        """
        if event.state == "CONNECTED" \
                or event.type == "CREATED" \
                or event.type == "DELETED" \
                or event.type == "CHANGED" \
                or event.type == "CHILD":
            log.info("event change, state:{}".format(event.state))
            self.choose_master()
        else:
            log.info("event unrecognized")
