# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
``BaseDecision`` 基类的具体实现，具体参见 :mod:`framework`
"""
import copy

from ark.are import framework
from ark.are import exception
from ark.are import log
from ark.are import context


class DecisionMaker(framework.BaseDecisionMaker):
    """
    决策类，该类关注感知到的事件与完成事件，并进行决策操作。
    """
    def __init__(self):
        """
        初始化方法

        """
        self._concerned_message_list = ["SENSED_MESSAGE", "COMPLETE_MESSAGE"]

    def on_decision_message(self, message):
        """
        决策处理逻辑

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        :raises EUnknownEvent: 位置事件异常
        """
        log.i("on decision message:{}".format(message.name))
        if message.name == "SENSED_MESSAGE":
            decided_message = self.decision_logic(message)
            self.send(decided_message)
        elif message.name == "COMPLETE_MESSAGE":
            pass
        elif message.name in self._concerned_message_list:
            self.on_extend_message(message)
        else:
            raise exception.EUnknownEvent(
                "message type [{}] is not concerned".format(message.name))

    def on_extend_message(self, message):
        """
        扩展消息处理函数。默认的决策只关注感知到的消息和执行完成的消息，如关注其他消息，可在此函数中实现处理逻辑

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        pass

    def decision_logic(self, message):
        """
        决策逻辑，生成待执行事件

        :param Message message: 消息对象
        :return: 待发送消息
        :rtype: Message
        :raises ENotImplement: 未实现
        """
        raise exception.ENotImplement("function is not implement")


class KeyMappingDecisionMaker(DecisionMaker):
    """
    key-mapping映射决策器。 该决策器根据感知到的消息中的from_key字段值在mapping中对应的
    方法名，生成待执行消息，待执行操作在待执行消息参数中，用_to_key字段表示。
    """
    _to_key = ".inner_executor_key"

    def __init__(self, mapping, from_key):
        """
        初始化方法

        :param dict mapping: 策略-操作映射表
        :param str from_key: 参考外部事件中的键
        """
        super(KeyMappingDecisionMaker, self).__init__()
        self._mapping = mapping
        self._from_key = from_key

    def decision_logic(self, message):
        """
        具体决策逻辑

        :param Message message: 消息对象
        :return: 决策完成的消息
        :rtype: Message
        :raises ETypeMismatch: 事件参数不匹配异常
        """
        log.i("begin decision logic, message:{}".format(message.name))
        operation_id = message.operation_id
        params = message.params
        if self._from_key in params \
                and params[self._from_key] in self._mapping:
            params_cp = copy.deepcopy(params)
            params_cp[self._to_key] = self._mapping[params_cp[self._from_key]]
            decided_message = framework.OperationMessage(
                "DECIDED_MESSAGE", operation_id, params_cp)
            return decided_message
        else:
            raise exception.ETypeMismatch(
                "{} not in params or params[{}] not in "
                "mapping".format(self._from_key, self._from_key))


class StateMachineDecisionMaker(DecisionMaker):
    """
    状态机决策类，该类会额外关注单个状态完成的消息，并进行记录.
    """
    def decision_logic(self, message):
        """
        状态机决策逻辑，该函数直接根据Message生成决策完成的消息对象

        :param Message message: 感知完成的消息对象
        :return: 决策完成的消息对象
        :rtype: Message
        """
        decided_message = framework.OperationMessage(
            "DECIDED_MESSAGE", message.operation_id, message.params)
        return decided_message
