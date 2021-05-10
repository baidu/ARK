# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**framework** 框架核心运行模块，封装了感知、决策、执行的执行模型和基于消息驱动的事件处理流程，
并提供了各种功能组件的基类。包含以下主要模块：

* ``Message`` 消息类，消息泵分发和处理的主体，外部事件在各阶段生成对应阶段的消息对象，供不同的监听器执行。
* ``Listener`` 消息监听器。一个消息监听器关注某些类型的消息，并在消息到达后执行对应的操作，感知、决策、执行器都为其派生类
* ``MessagePump`` 消息泵，实现了框架核心运行机制，监听器绑定消息泵后，当各监听器关注的消息到达时，由消息泵进行消息分发。
"""
import time
import copy
import multiprocessing

import ark.are.context as context
import ark.are.exception as exception
import ark.are.log as log
import ark.are.ha as ha
from ark.are.report import ArkServer


class Message(object):
    """
    消息基类，Guardian各消息处理器通过关注一系列感兴趣的消息，进行消息处理

    """

    def __init__(self, name):
        """
        初始化方法

        :param str name: 消息名
        """
        self.__name = name

    @property
    def name(self):
        """
        消息名，区分不同类型的消息

        :return: 消息名
        :rtype: str
        """
        return self.__name


class IDLEMessage(Message):
    """
    空闲消息。在消息泵中没有任何消息要处理时，添加空闲消息，使关注空闲消息的处理器运行。如：
    感知模块通过关注空闲消息进行外部事件感知等。
    """
    __ALL = (
        "IDLE_MESSAGE",
    )

    def __init__(self):
        """
        初始化方法

        :return: 无返回
        :rtype: None
        """
        name = "IDLE_MESSAGE"
        super(IDLEMessage, self).__init__(name)


class OperationMessage(Message):
    """
    操作各阶段的描述消息。包含消息名、操作id、操作参数属性。
    """
    __ALL = (
        "SENSED_MESSAGE",
        "DECIDED_MESSAGE",
        "COMPLETE_MESSAGE",
        "STAGE_COMPLETE_MESSAGE",
    )

    def __init__(self, name, operation_id, params):
        """
        初始化方法

        :param str name: 消息名
        :param str operation_id: 操作id
        :param dict params: 操作参数

        """
        super(OperationMessage, self).__init__(name)
        self.__operation_id = operation_id
        self.params = params

    @property
    def operation_id(self):
        """
        操作id

        :return: 操作id
        :rtype: str
        """
        return self.__operation_id


class Listener(object):
    """
    消息处理器基类，包含对消息的注册、处理等功能，是``GuardianFramework``消息驱动模型中
    消息处理的主体部分。感知器、决策器、执行器都是该类的子类。

    消息处理器不断从消息队列中获取自己关注的消息，并调用消息处理函数进行处理。

    .. Note:: 消息处理器仅处理自身所关注的类型的消息，消息由消息泵进行分发
    """
    _message_pump = None
    _concerned_message_list = []

    def register(self, message_name_list):
        """
        增加对一组消息的关注

        .. Note:: 该方法只能在消息处理器绑定消息泵后调用。多次调用，会对消息名字列表合并。

        :param list(str) message_name_list: 消息名字列表
        :return: 无返回
        :rtype: None
        """
        self._concerned_message_list = list(set(self._concerned_message_list).
                                            union(set(message_name_list)))
        log.i("register message success, concerned message list:{}".format(
            self._concerned_message_list))

    def deregister(self, message_name_list):
        """
        去除对某一组消息的关注

        :param list(str) message_name_list: 消息名字列表
        :return: 无返回
        :rtype: None
        """
        self._concerned_message_list = list(set(self._concerned_message_list).
                                            difference(set(message_name_list)))
        log.i("deregister message success, concerned message list:{}".format(
            self._concerned_message_list))

    def active(self):
        """
        消息处理器生效，通常在Guardian获得执行权、消息处理器开始工作时触发。
        默认不进行任何操作，子类可继承并实现具体的生效逻辑。

        :return: 无返回
        :rtype: None
        """
        pass

    def inactive(self):
        """
        消息处理器失效，通常在Guardian丧失执行权、消息处理器不再进行工作时触发。
        默认不进程任何操作，子类可继承并实现具体的逻辑。

        :return: 无返回
        :rtype: None
        """
        pass

    def list(self):
        """
        返回所有关注消息的名字列表

        :return: 关注消息的名字列表
        :rtype: list(str)
        """
        return self._concerned_message_list

    @context.GuardianContext.complete_operation
    def on_message_wrapper(self, message):
        """
        消息处理入口方法，消息处理器获得关注的消息后，调用此方法进行消息处理。装饰器用来完成一次操作的记录及清理操作。此函数通常不应被继承

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        return self.on_message(message)

    def on_message(self, message):
        """
        消息处理入口方法，消息处理器获得关注的消息后，调用此方法进行消息处理。

        .. Note:: 该方法必须被实现，不能直接调用

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        :raises ENotImplement: 虚接口，不能直接调用
        """
        raise exception.ENotImplement("function is not implement")

    def bind_pump(self, message_pump):
        """
        将消息处理器与一个消息泵绑定，是消息处理器从消息泵中获得关注的消息并处理的必要条件。

        :param MessagePump message_pump: 消息泵对象
        :return: 无返回
        :rtype: None
        """
        self._message_pump = message_pump

    @context.GuardianContext.new_period
    def send(self, message):
        """
        发送一个消息到消息泵

        .. Note:: 此方法必须在主进程中调用，严禁在子进程中调用send方法，否则可能导致执行记录被覆盖、发送的消息未处理等严重问题。
        .. Note:: 该函数涉及到的与消息持久化相关的操作在装饰器中实现，为避免非预期的问题，尽量避免对此方法进行重写操作，如需重写，
        需明确可能的行为，并显式添加装饰器

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        if multiprocessing.current_process().name != "MainProcess":
            raise exception.ENotImplement("send() only be used in \'MainProcess\'")
        # 此处使用深拷贝，防止后续处理中造成环形引用
        msg_cp = copy.deepcopy(message)
        self._message_pump.put(msg_cp)
        log.d("send message to message pump success, message:{}".format(
            message.name))


class MessagePump(object):
    """
    消息泵，控制消息的生成处理流转。

    消息泵会定期取出消息，并分发给关注此类型消息的处理器，以驱动消息的处理。

    """
    _message_queue = []
    _listener_list = []
    _listener_table = {}
    _stop_tag = True
    _short_circuit_mode = False

    def mode(self, short_circuit=False):
        """
        设置运行模式。短路模式下，会尽可能跳过主线程不必要的处理流程，如会跳过DecisionMaker，会跳过主线程的自动持久化

        :param bool short_circuit: 短路模式开关
        :return: 无返回
        :rtype: None
        """
        self._short_circuit_mode = short_circuit
        return self

    def _short_circuit_msg(self, message):
        """
        短路模式下，改写消息实现短路处理效果，跳过DecisionMaker，直接生成决策消息进行执行处理

        :param OperationMessage message: 消息
        :return: 处理后的message
        :rtype: Message
        """
        if not self._short_circuit_mode:
            return message
        if message.name == "SENSED_MESSAGE":
            params_cp = copy.deepcopy(message.params)
            return OperationMessage(
                "DECIDED_MESSAGE", message.operation_id, params_cp)
        elif message.name == "COMPLETE_MESSAGE":
            return message
        else:
            return message

    def add_listener(self, listener):
        """
        添加一个消息处理器

        :param Listener listener: 消息处理器
        :return: 无返回
        :rtype: None
        """
        self._listener_list.append(listener)
        self._listener_table[listener] = listener.list()

    def del_listener(self, listener):
        """
        删除一个消息处理器

        :param Listener listener: 消息处理器
        :return: 无返回
        :rtype: None
        :raises ValueError: 消息处理器不存在
        """
        try:
            self._listener_list.remove(listener)
            del self._listener_table[listener]
        except ValueError as e:
            log.r(e, "del listener failed")

    def validate_listeners(self):
        """
        验证消息处理器是否符合要求。通常，我们认为一个Guardian包含的消息处理器数量应该为：
        至少一个感知器、一个决策器、一个执行器。

        :return: 无返回
        :rtype: None
        :raises ETypeMismatch: 消息处理器验证失败
        """
        sensor_count, decision_count, executor_count = 0, 0, 0
        for listener in self._listener_list:
            if isinstance(listener, BaseSensor):
                sensor_count += 1
            elif isinstance(listener, BaseDecisionMaker):
                decision_count += 1
            elif isinstance(listener, BaseExecutor):
                executor_count += 1
            else:
                raise exception.ETypeMismatch("listener type is not match")
        if sensor_count < 1 or decision_count != 1 or executor_count != 1:
            raise exception.ETypeMismatch(
                "listener must be: one decision, "
                "one executor, at least one sensor")

    def list_listener_concern(self, listener):
        """
        列出一个消息处理器关注的消息类型列表

        :param Listener listener: 要查询的消息处理器
        :return: 所有关注的消息类型列表
        :rtype: list(str)
        """
        return self._listener_table[listener]

    def put(self, message):
        """
        发送一个消息至消息泵

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        self._message_queue.append(message)

    def on_persistence(self):
        """
        数据持久化操作

        .. Note:: 该方法会在消息泵中消息发生变化时调用，如有特殊的持久化逻辑，需重写该方法

        :return:
        """
        pass

    def run_loop(self, idle_sleep):
        """
        消息泵驱动逻辑。从消息泵中取消息并分发给关注此消息的处理器执行。
        为避免执行异常退出，消息泵捕获处理器执行的所有异常。当消息泵中无消息时，
        添加空闲消息，以驱动需要持续运行的消息处理器执行。

        :param float idle_sleep: 如果无事件要处理则强制sleep对应时间
        :return: 无返回
        :rtype: None
        """
        while not self._stop_tag:
            if not self._message_queue:
                self.put(IDLEMessage())
                is_idle = True
            else:
                is_idle = False
            message = self._message_queue[0]
            if not is_idle:
                # 将消息短路处理，跳过DecisionMaker，直接返回决策消息进行执行处理。如果返回None，则不需要处理，直接跳过该消息。
                message = self._short_circuit_msg(message)
                if message is None:
                    self._message_queue.pop(0)
                    continue

            for listener in self._listener_list:
                if message.name not in self.list_listener_concern(listener):
                    continue
                else:
                    pass
                try:
                    if not is_idle:
                        log.Logger.setoid(message.operation_id)
                    listener.on_message_wrapper(message)
                except Exception as e:
                    log.f("error occurred on listener:{}".format(listener.__class__))
                finally:
                    log.Logger.clearoid()

            self._message_queue.pop(0)
            if not is_idle:
                if not self._short_circuit_mode:
                    self.on_persistence()
            else:
                if not self._message_queue:
                    time.sleep(idle_sleep)


class GuardianFramework(MessagePump):
    """
    ``GuardianFramework`` 提供了面向感知、决策、执行的处理框架。

    * 感知：数据采集单元。针对状态信息或者监控数据，汇总成后续处理感兴趣的事件。感知仅关心 **实时数据** ，可存在多个感知模块。
    * 决策：带历史的数据分析单元，根据感知事件及策略，产出一系列的处理动作事件。 决策关心 **历史的事件与处理结果** ，仅能存在一个决策模块。
    * 执行：根据决策生成的处理事件执行相应的处理动作。执行关心 **具体操作的运行结果** ，执行器支持并行操作。

    感知、决策、执行三部分相互之间通过消息机制进行通讯，即输入和输出均为消息（感知部分可对接外部事件源）
    其运行逻辑如下图所示：

    .. image:: ../../../image/pump.png
    """
    _context = None
    _is_leader = False
    _run_tag = True
    __TIME_INTERVAL = 3
    __IDLE_INTERVAL = 0.0001

    def start(self, pmode):
        """
        Guardian启动函数，当Guardian获得领导权后，消息泵开始工作。

        :param str pmode: 所要使用的持久化类型，可选zookeeper或者local
        :return: 无返回
        :rtype: None
        """
        ArkServer().start()
        ha.HAMaster.init_environment()
        leader_election = ha.HAMaster(self.obtain_leader, self.release_leader)
        leader_election.create_instance()
        leader_election.choose_master()
        while self._run_tag:
            if self._is_leader:
                self.run_loop(self.__IDLE_INTERVAL)
            else:
                time.sleep(self.__TIME_INTERVAL)
        else:
            pass

    def obtain_leader(self):
        """
        获取领导权。Guardian获得领导权后，加载运行上下文信息，并开始运行。

        :return: 无返回
        :rtype: None
        """
        self._is_leader = True
        self._context = context.GuardianContext.load_context()
        MessagePump._message_queue = self._context.message_list
        self._recover_executing_message()
        self._context.update_lock(True)
        for listener in self._listener_list:
            listener.bind_pump(self)
            listener.active()
        self._stop_tag = False
    
    def _recover_executing_message(self):
        for operation in self._context.operations.itervalues():
            if operation.status != "FINISH":
                operation_id = operation.operation_id
                ret = self._context.is_operation_id_in_message_list(operation_id)
                if not ret:
                    name = "DECIDED_MESSAGE"
                    params_cp = operation.operation_params
                    message = OperationMessage(name, operation_id, copy.deepcopy(params_cp))
                    log.i("recover_message operation_id:{}".format(
                        operation_id))
                    self._context.message_list.append(message)

    def release_leader(self):
        """
        释放领导权

        :return: 无返回
        :rtype: None
        """
        for listener in self._listener_list:
            listener.inactive()
        self._is_leader = False
        self._stop_tag = True
        self._context.update_lock(False)

    def on_persistence(self):
        """
        数据持久化操作

        :return: 无返回
        :rtype: None
        """
        self._context.save_context()
        log.d("context persistent success")


class BaseSensor(Listener):
    """
    感知基类
    """

    def on_message(self, message):
        """
        消息处理函数

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        self.on_sensor_message(message)

    def on_sensor_message(self, message):
        """
        感知消息处理函数

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        :raises ENotImplement: 未实现
        """
        raise exception.ENotImplement("function is not implement")


class BaseDecisionMaker(Listener):
    """
    决策基类
    """
    def on_message(self, message):
        """
        消息处理函数。

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        self.on_decision_message(message)

    def on_decision_message(self, message):
        """
        消息处理

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        :raises ENotImplement: 未实现
        """
        raise exception.ENotImplement("function is not implement")


class BaseExecutor(Listener):
    """
    执行基类
    """

    def on_message(self, message):
        """
        触发执行并返回结果，``on_execute_message`` 可能为同步或异步操作，
        若为异步操作，则应返回None

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        :raises ETypeMismatch: 返回值类型不匹配
        :raises EMissingParam: 返回值缺字段
        """
        message = self.on_execute_message(message)
        if not message:
            return
        elif not isinstance(message, Message):
            raise exception.ETypeMismatch()
        else:
            log.i("execute message return:{}".format(message))
            self.send(message)

    def on_execute_message(self, message):
        """
        消息处理

        :param Message message: 消息对象
        :return: 执行结果
        :rtype: Message
        :raises ENotImplement: 未实现
        """
        raise exception.ENotImplement("function is not implement")
