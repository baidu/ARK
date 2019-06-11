# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**ha** 模块用于智能运维机器人的主从切换，以保证服务的高可用

* ``HAMaster`` 封装基于persistence模块的主从高可用操作，
以保证Guardian在极端情况下的可用性，即主备切换功能支持

"""
import time

from ark.are import config
from ark.are import log
from ark.are import persistence


class HAMaster(object):
    """
    主从选举客户端类, 封装基于persistence的主从选举和相应状态变更
    """
    def __init__(self, start_scheduler_func, stop_scheduler_func):
        """
        初始化方法
        
        :param func start_scheduler_func: 实例成为主时回调函数
        :param func stop_scheduler_func: 实例成为从时回调函数
        """
        self.path = config.GuardianConfig.get_persistent_path("alive_clients")
        self._start_scheduler_func = start_scheduler_func
        self._stop_scheduler_func = stop_scheduler_func
        self._pd = persistence.PersistenceDriver()
        self._pd.add_listener(self.state_listener)
        self.is_leader = False

    @classmethod
    def init_environment(cls):
        """
        初始化Guardian运行环境

        :return: 无返回
        :rtype: None
        :raises EFailedRequest: 状态服务请求异常
        """

        guardian_base = config.GuardianConfig.get_persistent_path()
        guardian_client_path = config.GuardianConfig.get_persistent_path("alive_clients")
        context_path = config.GuardianConfig.get_persistent_path("context")
        operations_path = config.GuardianConfig.get_persistent_path("operations")
        pd = persistence.PersistenceDriver()

        if not pd.exists(guardian_base):
            pd.create_node(path=guardian_base)
            log.d("persistent node %s created!" % guardian_base)
        if not pd.exists(context_path):
            pd.create_node(path=context_path)
            log.d("persistent node %s created!" % context_path)
        if not pd.exists(guardian_client_path):
            pd.create_node(path=guardian_client_path)
            log.d("persistent node %s created!" % guardian_client_path)
        if not pd.exists(operations_path):
            pd.create_node(path=operations_path)
            log.d("persistent node %s created!" % operations_path)

    def create_instance(self):
        """
        创建临时有序节点
        :return: None
        """
        node_path = self.path + "/{}#".\
            format(config.GuardianConfig.get(config.INSTANCE_ID_NAME))
        self._pd.create_node(path=node_path, value="",
                             ephemeral=True, sequence=True, makepath=True)

    def choose_master(self):
        """
        guardian选主: master节点下, 所有ephemeral+sequence类型的节点中, 编号最小的获得领导权.

        :return: None
        """
        instance_list = self._pd.get_children(
            path=self.path, watcher=self.event_watcher)
        instance = sorted(instance_list)[0].split("#")[0]
        # 本实例获得领导权
        if str(instance) == str(config.GuardianConfig.get(config.INSTANCE_ID_NAME)):
            if not self.is_leader:
                self._start_scheduler_func()
                self.is_leader = True
                log.i("I am new master, scheduler")
            else:
                log.i("I am new master, and is old master, "
                         "no longer reschedule")
        # 本实例没有获得领导权
        else:
            if self.is_leader:
                self._stop_scheduler_func()
                self.is_leader = False
                log.i("I am slave, stop scheduler")
            else:
                log.i("I am slave, and is old slave, "
                      "no longer stop scheduler")
        log.i("choose master finished")

    def state_listener(self, state):
        """
        监听会话状态

        :param state: 本次触发的状态
        :return: None
        """
        if state == persistence.PersistenceEvent.PersistState.LOST:
            log.i("guardian instance state lost")
            while True:
                try:
                    self.create_instance()
                    self._pd.get_children(
                        path=self.path, watcher=self.event_watcher)
                    log.i("guardian instance state recreate finished")
                    break
                except:
                    log.f("create instance err")
                time.sleep(1)
        elif state == persistence.PersistenceEvent.PersistState.SUSPENDED:
            log.i("guardian instance state suspended")
        elif state == persistence.PersistenceEvent.PersistState.CONNECTED:
            log.i("guardian instance state connected")
        else:
            log.i("guardian instance state unrecognized, state:{}".format(state))

    def event_watcher(self, event):
        """
        监听事件

        :param PersistenceEvent event: 节点状态事件
        :return: None

        """
        if event.state == persistence.PersistenceEvent.PersistState.CONNECTED \
                or event.type == persistence.PersistenceEvent.EventType.CREATED \
                or event.type == persistence.PersistenceEvent.EventType.DELETED \
                or event.type == persistence.PersistenceEvent.EventType.CHANGED \
                or event.type == persistence.PersistenceEvent.EventType.CHILD:
            log.i("event change, state:{}".format(event.state))
            self.choose_master()
        else:
            log.i("event unrecognized")
