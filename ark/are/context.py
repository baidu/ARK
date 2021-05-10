# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**context** Guardian运行上下文信息描述类，框架提供高可用方案，Guardian主备部署，在运行关键点通过context记录运行
上下文信息并持久化数据，当实例发生迁移时，可恢复当前运行状态，保证执行可用性。

.. Note:: 当前状态服务由zookeeper实现
"""
import pickle
import time
import copy

import ark.are.config as config
import ark.are.persistence as persistence
import ark.are.exception as exception
import ark.are.log as log
from ark.are.common import Singleton


class GuardianContext(Singleton):
    """
    Guardian运行状态数据类，为单例类，避免生成多个context对象
    """
    _context = None

    @classmethod
    def get_context(cls):
        """
        获取context

        .. Note:: 此方法为从本地内存中读取context, 不涉及外部调用，应在context加载后使用

        :return: GuardianContext对象
        :rtype: GuardianContext
        :raises EInvalidOperation: 非法操作
        """
        if not cls._context:
            raise exception.EInvalidOperation(
                "guardian context is not init yet")
        return cls._context

    @classmethod
    def load_context(cls):
        """
        加载context，首次加载时，新建GuardianContext对象，否则从状态服务反序列化对象

        :return: context对象
        :rtype: GuardianContext
        """
        context_path = config.GuardianConfig.get_persistent_path("context")
        data = persistence.PersistenceDriver().get_data(context_path)
        log.i("load context success")
        guardian_context = pickle.loads(data) if data else GuardianContext()
        # load状态机信息
        operations_path = config.GuardianConfig.get_persistent_path("operations")
        # operations子节点名称均为operation_id
        operation_ids = persistence.PersistenceDriver().get_children(operations_path)
        for operation_id in operation_ids:
            operation_path = operations_path + "/" + operation_id
            try:
                operation_data = persistence.PersistenceDriver().get_data(operation_path)
                log.i("load operation[{}] success".format(operation_id))
                operation = pickle.loads(operation_data)
                guardian_context.operations[operation_id] = operation
            except Exception as e:
                log.f("load operation {} failed".format(operation_id))

        cls._context = guardian_context
        return guardian_context

    def __init__(self):
        """
        初始化。GuardianContext保存的数据除消息队列与所有operation信息外，
        还包含extend字段，供用户扩展。持久化数据时会检查lock字段，判断此guardian实例
        是否有保存信息的权利。

        .. Note:: 为方便使用json序列化并进行记录，属性统一设置为public，
                 但不建议直接对属性进行更新，应调用对应的函数修改（函数中都包含了context
                 持久化的操作），否则在更新属性后应手动调用 ``save_context`` 方法对数据
                 持久化。

        """
        self.__guardian_id = config.GuardianConfig.get(config.GUARDIAN_ID_NAME)
        self.message_list = []
        self.operations = {}
        self.extend = {}
        self.lock = False

    def save_operation(self, operation):
        """
        持久化状态机信息
        :param operation:
        :return:
        """
        if not self.lock:
            log.e("current guardian instance no privilege to save operation")
            raise exception.EInvalidOperation(
                "current guardian instance no privilege to save operation")
        operation_path = config.GuardianConfig.get_persistent_path("operations") + "/" + operation.operation_id
        if not persistence.PersistenceDriver().exists(operation_path):
            persistence.PersistenceDriver().create_node(path=operation_path)
        persistence.PersistenceDriver().save_data(operation_path, pickle.dumps(operation))
        log.d("save operation_id:{} success".format(operation.operation_id))

    def save_context(self):
        """
        运行数据持久化，当当前Guardian为主（lock属性为True）时，可持久化数据，否则失败

        :return: 无返回
        :rtype: None
        :raises EInvalidOperation: 非法操作
        """
        if not self.lock:
            log.e("current guardian instance no privilege to save context")
            raise exception.EInvalidOperation(
                "current guardian instance no privilege to save context")
        context_path = config.GuardianConfig.get_persistent_path("context")
        context_to_persist = self
        operations_tmp = self.operations
        context_to_persist.operations = {}
        try:
            persistence.PersistenceDriver().save_data(context_path, pickle.dumps(context_to_persist))
        except Exception as e:
            self.operations = operations_tmp
            log.r(e, "save context fail")

        self.operations = operations_tmp
        log.d("save context success")

    def update_lock(self, is_lock):
        """
        更新锁标识

        :param bool is_lock: 是否获得锁
        :return: 无返回
        :rtype: None
        """
        self.lock = is_lock
        log.d("update context lock, {}".format(self.lock))

    def create_operation(self, operation_id, operation):
        """
        新增一个操作，一般在感知完成时调用此方法。一个外部事件的整个处理流程，称为一个操作

        :param str operation_id: 操作id，作为操作的唯一标识
        :param Operation operation: 操作对象
        :return: 无返回
        :rtype: None
        """
        self.operations[operation_id] = operation
        self.save_operation(operation)
        log.d("create new operation success, operation_id:{}".
              format(operation_id))

    def delete_operation(self, operation_id):
        """
        删除一个操作，一般在一个事件操作结束时调用

        :param str operation_id: 操作id
        :return: 无返回
        :rtype: None
        """
        del self.operations[operation_id]
        operation_path = config.GuardianConfig.get_persistent_path("operations") + "/" + operation_id
        persistence.PersistenceDriver().delete_node(operation_path)
        log.d("delete operation from context success, operation_id:{}".
              format(operation_id))

    def get_operation(self, operation_id):
        """
        获取操作，会返回一个操作对象

        :param str operation_id: 操作id
        :return: 操作对象
        :rtype: Operation
        """
        return self.operations[operation_id]

    def update_operation(self, operation_id, operation):
        """
        更新一个操作

        :param str operation_id: 操作id
        :param Operation operation: 操作对象
        :return: 无返回
        :rtype: None
        """
        self.operations[operation_id] = operation
        self.save_operation(operation)
        log.d("update operation success, operation_id:{}".format(operation_id))

    def get_extend(self):
        """
        获取扩展

        :return: 扩展字段
        :rtype: dict
        """
        return self.extend

    def update_extend(self, params):
        """
        更新扩展，增量更新

        :param dict params: 更新的参数
        :return: 无返回
        :rtype: None
        """
        self.extend.update(params)
        self.save_context()
        log.d("update extend success, current extend:{}".format(self.extend))

    def del_extend(self, key):
        """
        从扩展中删除指定的key

        :param str key: 需要删除的字段
        :return: 无返回
        :rtype: None
        """
        del self.extend[key]
        self.save_context()
        log.d("delete extend success, current extend:{}".format(self.extend))

    @staticmethod
    def new_period(func):
        """
        消息发送到消息泵的后续操作装饰器，在本次操作中添加此period的记录

        :param function func: 被装饰的函数名
        :return: 被装饰后的新函数
        :rtype: function
        """
        def wrapper(send_obj, message):
            """
            :param send_obj:
            :param message:
            :return:
            """
            # 避免循环import和局部import
            if message.__class__.__name__ != "OperationMessage":
                raise exception.ETypeMismatch("only OperationMessage can be send()")
            if not message.operation_id:
                raise exception.EMissingParam("OperationMessage need operation_id")
            if message.name == "SENSED_MESSAGE" or \
               message.name == "DECIDED_MESSAGE" or \
               message.name == "COMPLETE_MESSAGE":
                guardian_context = GuardianContext.get_context()
                try:
                    operation = guardian_context.get_operation(
                        message.operation_id)
                except KeyError:
                    # 此处使用深拷贝，防止后续处理中造成环形引用
                    operation = Operation(
                        message.operation_id, copy.deepcopy(message.params))
                    guardian_context.create_operation(
                        message.operation_id, operation)
                operation.append_period(message.name)
            ret = func(send_obj, message)
            return ret

        return wrapper

    @staticmethod
    def complete_operation(func):
        """
        整个操作执行完后续处理

        :param function func: 被装饰的函数
        :return: 新函数
        :rtype: function
        """
        def wrapper(send_obj, message):
            """

            :param send_obj:
            :param message:
            :return:
            """
            msg_name = message.name
            if hasattr(message, "operation_id"):
                msg_oid = message.operation_id
            try:
                ret = func(send_obj, message)
                return ret
            finally:
                if msg_name == "COMPLETE_MESSAGE":
                    guardian_context = GuardianContext.get_context()
                    operation = guardian_context.get_operation(msg_oid)
                    operation.end_operation()
                    guardian_context.delete_operation(msg_oid)

        return wrapper

    @staticmethod
    def new_action(func):
        """
        发送消息至消息泵后续处理方法

        :param function func: 被修饰函数
        :return: 新函数
        :rtype: function
        """

        def wrapper(send_obj, message):
            """

            :param send_obj:
            :param message:
            :return:
            """
            # 避免循环import和局部import
            if message.__class__.__name__ != "OperationMessage":
                raise exception.ETypeMismatch("only OperationMessage can be send()")
            if not message.operation_id:
                raise exception.EMissingParam("OperationMessage need operation_id")
            if message.name == "STATE_COMPLETE_MESSAGE" or \
                    message.name == "PERSIST_SESSION_MESSAGE":
                guardian_context = GuardianContext.get_context()
                operation = guardian_context.get_operation(
                    message.operation_id)
                finished_node = message.params["finished_node"]
                current_node = message.params["current_node"]
                timestamp = message.params["timestamp"]
                session = message.params["session"]
                if current_node:
                    operation.add_action(current_node)
                if finished_node:
                    operation.update_action(
                        finished_node, "FINISHED", timestamp)
                if session:
                    operation.update_session(session)

                # 状态机节点持久化
                guardian_context.update_operation(operation.operation_id, operation)
            else:
                pass
            ret = func(send_obj, message)
            return ret
        return wrapper

    def is_operation_id_in_message_list(self, operation_id):
        for message in self.message_list:
            if message.name != "IDLE_MESSAGE" and message.operation_id == operation_id:
                return True
        return False
        

class Operation(object):
    """
    操作类，操作类描述了一个外部事件从感知到执行完成的所有状态信息
    """
    def __init__(self, operation_id, operation_params,
                 session=None):
        """
        初始化方法，``Operation`` 对象包含操作id、当前操作状态、操作参数、
        操作各阶段信息、执行过程信息、运行session信息
        * periods: 表示一个事件的各个执行时期，主要包含感知、决策、执行等各个阶段性信息
        * actions: 表示一个事件在执行期，各执行过程信息

        :param str operation_id: 操作id
        :param dict operation_params: 操作参数
        :param dict session: 运行session信息，默认为None。状态机相关operation的session用来保存状态机session
        """
        self.operation_id = operation_id
        self.status = "CREATE"
        self.operation_params = operation_params
        self.periods = Periods()
        self.actions = Actions()
        self.session = session

    def append_period(self, name):
        """
        添加一个执行节点记录

        :param str name: 执行阶段名
        :return: 无返回
        :rtype: None
        """
        self.periods.append_period(name)
        self.record_action()

    def add_action(self, name):
        """
        添加一个执行状态记录

        :param str name: 执行状态名
        :return: 无返回
        :rtype: None
        """
        last_action = self.actions.last_action()
        if last_action and last_action.name == name:
            return
        self.actions.append_action(name)
        self.record_action()

    def update_action(self, name, status, end_time):
        """
        更新一个执行状态

        :param str name: 状态名
        :param str status: 状态
        :param int end_time: 结束时间
        :return: 无返回
        :rtype: None
        """
        self.actions.update_action(name, status, end_time)
        self.record_action()

    def end_operation(self):
        """
        结束一个操作

        :return: 无返回
        :rtype: None
        """
        self.status = "FINISH"
        self.record_action()

    def record_action(self):
        """
        记录状态

        .. Note:: 为避免es服务异常导致主进程退出，此方法捕获所有异常，并打印日志

        :return: 无返回
        :rtype: None
        """
        # try:
        #     ESClient("ark", "operation").put_data(self.operation_id, json.dumps(
        #         self, default=lambda obj: obj.__dict__))
        # except Exception as e:
        #     log.f("record operation err")
        # else:
        #     log.i("record action success, operation_id:{}".format(
        #         self.operation_id))

    def update_session(self, session):
        """
        状态及处理完一个节点之后，更新session
        """
        self.session = session


class Periods(object):
    """
    操作阶段集合类
    """

    def __init__(self):
        """
        初始化方法

        """
        self.periods = []

    def append_period(self, name):
        """
        添加一个执行阶段

        :param str name: 执行阶段名
        :return: 无返回
        :rtype: None
        """
        self.periods.append(Period(name))


class Actions(object):
    """
    执行进度集合类
    """

    def __init__(self):
        """
        初始化方法

        """
        self.actions = []

    def append_action(self, name):
        """
        添加一个执行进度

        :param str name: 执行进度名
        :return: 无返回
        :rtype: None
        """
        self.actions.append(Action(name))

    def get_action(self, name):
        """
        获取一个执行进度

        :param str name: 执行进度名
        :return: 执行进度
        :rtype: Action
        :raises EMissingParam: 缺少参数异常
        """
        match_action = filter(lambda x: x.name == name, self.actions)
        try:
            action = match_action[0]
        except IndexError:
            raise exception.EMissingParam("action:{} not found".format(name))
        return action

    def update_action(self, name, status, end_time):
        """
        更新执行进度

        :param str name: 进度名
        :param str status: 进度状态
        :param int end_time: 结束时间
        :return: 无返回
        :rtype: None
        """
        action = self.get_action(name)
        action.status = status
        action.endTime = end_time

    def last_action(self):
        """
        获取最后一个执行进度

        :return: 执行进度
        :rtype: Action
        """
        if len(self.actions) == 0:
            return None
        else:
            return self.actions[-1]


class Period(object):
    """
    操作阶段类
    """

    def __init__(self, name):
        """
        初始化方法

        :param str name: 操作阶段名
        """
        self.name = name
        self.timestamp = int(time.time())


class Action(object):
    """
    执行进度类
    """

    def __init__(self, name, status=None, start_time=None, end_time=None):
        """
        初始化方法

        :param str name: 进度名
        :param str status: 进度状态
        :param int start_time: 开始时间
        :param int end_time: 结束时间
        """
        self.name = name
        self.status = status or "CREATE"
        self.startTime = start_time or int(time.time())
        self.endTime = end_time or 2147483647


class FlushFlag(object):
    """
    刷新标记，提供一系列用于上下文刷新控制的函数支持
    """
    def get_flush(self):
        """
        获取标记判断是否需要进行flush
        :return: flag
        :rtype: bool
        """
        self._flush_flag = self._flush_flag if hasattr(self, "_flush_flag") else False
        return self._flush_flag

    def set_flush(self, flag):
        """
        设置标记判断是否需要进行flush
        :param bool flag: 是否需要进行刷新
        :return: 无返回
        :rtype: None
        """
        self._flush_flag = flag

    def flush(self):
        """
        设置标记以进行flush
        :return: 无返回
        :rtype: None
        """
        self._flush_flag = True

    def reset_flush(self):
        """
        标记完成flush，同时返回之前的标记
        :return: flag
        :rtype: bool
        """
        flag = self.get_flush()
        self._flush_flag = False
        return flag
