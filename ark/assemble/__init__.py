# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**assemble** 提供各种具体实现的Guardian（各种感知器、决策器、执行器的组装），是需要
用户关注的核心模块，可以直接引用已实现的Guardian进行体验和简单开发

* ``client`` 各种外部客户端调用的实现
* ``sensor`` 针对各种组件、系统具体实现的感知器
* ``amqpush_keymapping`` 使用activemq订阅外部事件、key-mapping映射进行决策的Guardian
* ``amqpush_statemachine`` 使用activemq订阅外部事件的状态机Guardian
* ``espull_keymapping`` 使用elasticsearch轮询方式感知外部事件、key-mapping映射进行决策的Guardian
* ``espull_statemachine`` 使用elasticsearch轮询方式感知外部事件的状态机Guardian
* ``localpull_keymapping`` 使用读取本地方式感知外部事件、key-mapping映射进行决策的Guardian
* ``localpull_statemachine`` 使用读取本地方式感知外部事件的状态机Guardian
"""
__version = '1.0.0'
__all__ = ['sensor', 'client', 'amqpush_keymapping', 'amqpush_statemachine',
           'espull_keymapping', 'espull_statemachine', 'localpull_keymapping',
           'localpull_statemachine', ]
