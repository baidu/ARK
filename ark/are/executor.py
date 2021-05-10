# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
``BaseExecutor`` 的各种派生实现，具体参见 :mod: `framework`
"""
import Queue
import copy
import multiprocessing
import time
import uuid

import ark.are.graph as graph
import ark.are.context as context
import ark.are.framework as framework
import ark.are.log as log
import ark.are.exception as exception


class BaseExecFuncSet(object):
    """
    通过继承 ``BaseExecFuncSet`` ，使用者可以将一系列的操作名与函数直接相关联。
    所有的函数拥有相同的入参：params。相同的返回值：ret。

    .. Note:: 与操作关联的函数必须为public属性
    """

    def list_all(self):
        """
        列出所有包含的函数名

        :return: 函数名列表
        :rtype: list(str)
        """
        func_name_list = []
        all_elem = dir(self)
        for elem_name in all_elem:
            if elem_name[0:1] != "_" \
                    and elem_name != "list_all" \
                    and elem_name != "exec_func" \
                    and type(getattr(self, elem_name)) == \
                            type(getattr(self, "list_all")):
                func_name_list.append(elem_name)
        return func_name_list

    def exec_func(self, func_name, params):
        """
        执行指定的函数

        :param str func_name: 要执行的函数名
        :param dict params: 执行入参，通常由决策模块生成
        :return: 执行结果
        :rtype: dict
        """
        return getattr(self, func_name)(params)


def run_process(cls_instance, operation):
    """
    运行一个子进程

    :param MultiProcessExecutor cls_instance: 运行实例
    :param Operation operation: operation操作对象
    :return: 执行结果
    :rtype: dict
    """
    return cls_instance.message_handler(operation)


class MultiProcessExecutor(framework.BaseExecutor):
    """
    并发执行器基类。 ``MultiProcessExecutor`` 继承自 ``BaseExecutor`` 并提供了多进程
    执行的能力。 ``MultiProcessExecutor`` 会根据指定的最大进程数开启进程池，并对待执行
    消息进行异步处理。 ``MultiProcessExecutor`` 关注空闲消息，并在空闲消息到达后进行任务
    启动和结果获取。
    """
    def __init__(self, process_count=1):
        """
        初始化方法

        :param int process_count: 进程数
        :return: 无返回
        :rtype: None
        :raises ETypeMismatch: 参数类型不匹配
        """
        if not isinstance(process_count, int) or process_count < 1 \
                or process_count > 1000:
            raise exception.ETypeMismatch(
                "param process_count must be 1-1000 integer")
        self._manager = multiprocessing.Manager()
        self._result_queue = self._manager.Queue()
        self._concerned_message_list = ["IDLE_MESSAGE", "DECIDED_MESSAGE"]
        self._process_count = process_count
        self._process_pool = None

    def __getstate__(self):
        self_dict = self.__dict__.copy()
        del self_dict['_process_pool']
        del self_dict['_manager']
        return self_dict

    def __setstate__(self, state):
        self.__dict__.update(state)

    def active(self):
        self._process_pool = multiprocessing.Pool(processes=self._process_count)

    def inactive(self):
        self._process_pool.terminate()

    def _persist_operation(self, message):
        """
        如果message有更新，则更新Operation，并将最新的Operation返回

        :param Message message: 消息对象
        :return: 持久化过的operation
        :rtype: Operation
        """
        guardian_context = context.GuardianContext.get_context()
        operation = guardian_context.get_operation(message.operation_id)
        if not message.params:
            return operation
        # 判断如果不同则更新operation并保存
        if cmp(message.params, operation.operation_params) != 0:
            operation.operation_params.update(message.params)
            guardian_context.save_operation(operation)
        return operation

    def on_execute_message(self, message):
        """
        执行器处理逻辑，该函数在空闲消息到来时，进行任务分发和结果获取操作

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        :raises Exception: 进程池启动异常
        :raises EUnknownEvent: 未知消息异常
        """
        if message.name == "DECIDED_MESSAGE":
            operation = self._persist_operation(message)
            self.on_pre_action(operation)
            self._process_pool.apply_async(run_process, (self, operation, ))

        elif message.name == "IDLE_MESSAGE":
            try:
                ret = self._result_queue.get(block=False)
            except Queue.Empty:
                pass
            else:
                return ret
        elif message.name in self._concerned_message_list:
            self.on_extend_message(message)
        else:
            raise exception.EUnknownEvent(
                "message type [{}] is not concerned".format(message.name))

    def on_extend_message(self, message):
        """
        扩展消息的处理逻辑

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        pass

    def on_pre_action(self, operation):
        """
        操作前的前置动作，在开启子进程执行操作之前调用

        :param Operation operation: operation操作对象
        :return: 无返回
        """
        pass

    def message_handler(self, operation):
        """
        消息处理逻辑，该函数调用具体的消息执行函数，并获取结果放入结果队列中

        .. Note:: 由于操作异步执行，因此各子进程执行结果统一放入多进程安全的结果队列，
                 由主进程统一进程结果的后续处理

        :param Operation operation: operation操作对象
        :return: 无返回
        :rtype: None
        """
        try:
            log.Logger.setoid(operation.operation_id)
            log.i("operation execute")
            log.d("operation execute, param:%s" % str(operation.operation_params))
            ret = self.execute(operation)
        except Exception as e:
            log.f("execute_message fail")
            ret = "err:{}".format(e)

        message = framework.OperationMessage(
            "COMPLETE_MESSAGE", operation.operation_id, ret)
        self._result_queue.put(message)
        log.Logger.clearoid()
        return

    def execute(self, operation):
        """
        事件具体执行逻辑

        :param Operation operation: operation操作对象
        :return: 执行结果
        :rtype: dict
        :raises ENotImplement: 未实现
        """
        raise exception.ENotImplement("function is not implement")


class CallbackExecutor(MultiProcessExecutor):
    """
    回调型执行器，该执行器一般与 ``BaseExecFuncSet`` 子类绑定，根据待执行消息中
    _exec_key字段值，回调func_set中定义的操作方法，并返回执行结果
    """
    _exec_key = ".inner_executor_key"

    def __init__(self, func_set, process_count=1):
        """
        初始化方法

        :param BaseExecFuncSet func_set: 回调方法对象
        :param int process_count: 进程数
        """
        super(CallbackExecutor, self).__init__(process_count)
        self._func_set = func_set

    def execute(self, operation):
        """
        执行消息的具体逻辑

        :param Operation operation: operation操作对象
        :return: 执行结果
        :rtype: dict
        """
        return self._func_set.exec_func(
            operation.operation_params[self._exec_key], operation.operation_params)


class StateMachineExecutor(MultiProcessExecutor, graph.PersistedStateMachineHelper):
    """
    状态机执行器。该执行器会在待执行消息到来时，生成（Guardian实例迁移时会先加载当前运行状态）状态机对象，找一个空闲的处理子进程执行状态机。执行逻辑为：
    * 选择当前运行状态（新建状态机当前状态为状态列表中第一个状态），并处理
    * 处理完成后生成一个阶段性运行完成的message，将当前运行的状态机信息传递给决策器，决策器记录到context
    * 根据返回的状态，进行下一个状态处理，直到返回结束标志，本次轮转结束
    * 生成一个执行完成的message，由决策器进行数据记录和清理动作

    .. Note:: ``StateMachineExecutor`` 状态机执行器额外关注控制消息，并在控制消息到来时
            传递给运行中的状态机，用户可用状态机session.control_message获得该控制消息
            的参数。
    """

    def __init__(self, nodes, process_count=1):
        """
        初始化方法

        :param list(Node) nodes: 节点列表
        :param int process_count: 最大进程数
        """
        super(StateMachineExecutor, self).__init__(process_count)
        self._nodes = nodes
        self._concerned_message_list.append("CONTROL_MESSAGE")

        self._graphs = {}
        self._control_message = self._manager.dict()

    def execute(self, operation):
        """
        执行消息

        :param Operation operation: operation操作对象
        :return: 无返回
        :rtype: None
        """
        state_machine = self._create_state_machine(operation, self._nodes)
        # 状态机启动
        state_machine.start()
        try:
            del self._control_message[state_machine.session.id]
        except Exception as e:
            str(e)
        log.i(
            "state machine run finished, operationId:%s" % format(state_machine.session.id))

    def persist(self, session, reason, finished_name, next_name):
        """
        提供必要的持久化实现

        .. Note:: session中的控制消息应在处理完成之后被清理，否则会造成重复触发

        :param object session: 状态机的session
        :param str reason: 持久化的原因
        :param str finished_name: 已经完成的节点名
        :param str next_name: 下一个将处理的节点名
        :return: 无返回
        :rtype: None
        """
        message_name = None
        if reason == graph.PersistedStateMachineHelper.Reason.CONTROL:
            message_name = "PERSIST_SESSION_MESSAGE"
        elif reason == graph.PersistedStateMachineHelper.Reason.STARTED:
            message_name = "STATE_COMPLETE_MESSAGE"
        elif reason == graph.PersistedStateMachineHelper.Reason.NODE_CHANGED:
            message_name = "STATE_COMPLETE_MESSAGE"
        if session is not None and message_name is not None:
            params = {
                "session": session,
                "finished_node": finished_name,
                "current_node": next_name,
                "timestamp": int(time.time())
            }
            notice = framework.OperationMessage(
                message_name,
                str(session.id), params)
            try:
                self._result_queue.put(notice)
            except IOError:
                log.f("result_queue.put fail, retry")
                self._result_queue.put(notice)
        else:
            log.e("operation persist but session is None or reason unknown")

    def get_control_message(self, session):
        """
        获取当前是否有控制消息需要处理。如果没有，则应返回None, None

        :param object session: 状态机的session
        :return: 控制消息ID，控制消息
        :rtype: str, object
        """
        try:
            if session.id not in self._control_message or self._control_message[session.id] is None:
                return None, None
            control = self._control_message[session.id]
            return control['control_id'], control['message']
        except Exception as e:
            log.f("get control message fail")
            return None, None

    def _create_state_machine(self, operation, nodes):
        """
        创建状态机。新建时创建，实例迁移时重新加载

        :param Operation operation: operation操作对象
        :return: 状态机实例
        :rtype: StateMachine
        """

        state_machine_session = operation.session
        if not state_machine_session:
            session = graph.PersistedStateMachineSession(id=operation.operation_id,
                                                         params=operation.operation_params)
            state_machine = graph.PersistedStateMachine(session)
            for node in nodes:
                state_machine.add_node(node)
            state_machine.prepare()
            ret = state_machine.dump()
            session.current_node = ret["current_node"]
            session.nodes_process = ret["nodes_process"]
            session.status = ret["status"]
            self._control_message[session.id] = None
        else:
            session = state_machine_session
            state_machine = graph.PersistedStateMachine(session)
            state_machine.load(session, session.nodes_process,
                               session.current_node.name, session.status)
            # 状态机的current_node必须为持久化至Session中的current_node
            for node in nodes:
                if node.name == session.current_node.name:
                    state_machine.add_node(session.current_node)
                else:
                    state_machine.add_node(node)
            # reload控制消息
            if session.control_message:
                self.on_extend_message(session.control_message)
            if session.id in self._control_message and self._control_message[session.id]:
                self._control_message[session.id]['control_id'] = session.last_control_id

        self._graphs[session.id] = state_machine
        state_machine.set_helper(self)
        return state_machine

    @context.GuardianContext.new_action
    def send(self, message):
        """
        发送消息至消息泵，更新状态机中的每个状态变化为action
        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        return super(StateMachineExecutor, self).send(message)

    def on_extend_message(self, message):
        """
        扩展消息的处理逻辑

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        # 执行器从消息泵获取控制消息
        control = {
            'message': message,
            'control_id': uuid.uuid1()
        }
        self._control_message[message.operation_id] = copy.deepcopy(control)
