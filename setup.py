#!/usr/bin/env python
# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
Setup script.

Date:    2018/02/09
"""
import setuptools

# 当python版本低于2.7.4时，如果其他module在atexit中注册了函数，
# 那么pbr包的lazy loading将中止setuptools的执行
# 根据此方案增加以下代码解决该问题: http://bugs.python.org/issue15881#msg170215
try:
    import multiprocessing  # noqa
except ImportError:
    pass

setuptools.setup(
    setup_requires=['pbr'],
    pbr=True,
)
