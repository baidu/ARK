# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**loader** 是Guardian的运行入口，作用为引入用户提供的主模块，调用其中的 ``guardian_main`` 获取到实际的Guardian对象之后将Guardian启动
"""
import sys
import os

if not os.path.dirname(__file__):
    sys.path.append(os.path.abspath('../..'))
else:
    sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../'))
    sys.path.append(
        os.path.abspath(os.path.dirname(__file__) + '/' + '../../src/'))


def loader():
    """
    提供Guardian的测试或者本地执行的加载功能

    .. Note:: 不捕获异常，以便可以进行错误追踪

    :return: 无返回
    :rtype: None
    """
    main = __import__('main')
    guardian = main.guardian_main()
    guardian.start()


if __name__ == "__main__":
    loader()
