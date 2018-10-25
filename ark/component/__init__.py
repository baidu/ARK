# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
组件模块


* ``stage_executor`` 分级操作执行器
* ``amq_sensor`` 通过AMQ方式获取外部事件的感知器
* ``es_sensor`` 通过Elasticsearch方式获取外部事件的感知器
* ``local_sensor`` 通过本地文件方式获取外部事件的感知器

"""
__version = '1.0.0'
__all = ['stage_executor', 'amq_sensor', 'es_sensor', 'local_sensor']
