# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**opal** (Operation Abstraction Layer)，操作抽象层，针对操作主体对象（如服务、机器、
实例）所关心的数据和可执行操作进行封装，以此来屏蔽底层异构系统的实现（不同平台、组件），为上层
Guardian开发提供统一的接口，简化开发复杂度，提升可迁移能力和可复用性。

* ``entity`` 标准操作实体基类定义，提供常用操作实体（如机器、服务）基类，定义必需属性及常用虚接口。
* ``job`` 包含通用任务类型的常用操作抽象
* ``driver`` 包含外部平台操作的具体实现
"""
__version = '1.0.0'
__all = ['job', 'entity', 'driver', ]

