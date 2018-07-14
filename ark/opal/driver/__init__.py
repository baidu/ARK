# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**driver** 提供外部平台具体操作的实现

* ``driver_common`` 根据平台和操作，动态加载对象
* ``execute_driver`` 执行driver，一般为使用外部平台进行运维操作的逻辑
* ``sensor_driver`` 感知driver，一般为对接外部系统进行感知操作
"""
__version = '1.0.0'
__all = ['driver_common', 'sensor_driver', 'execute_driver', ]

