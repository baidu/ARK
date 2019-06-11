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
import getopt

if not os.path.dirname(__file__):
    sys.path.append(os.path.abspath('../..'))
else:
    sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../../'))
    sys.path.append(
        os.path.abspath(os.path.dirname(__file__) + '/' + '../../../src/'))
    sys.path.append(
        os.path.abspath(os.path.dirname(__file__) + '/' + '../../../bin/'))


def loader(mode):
    """
    加载执行Guardian

    .. Note:: 不捕获异常，以便可以进行错误追踪

    :param mode: 所要使用的持久化类型
    :return: 无返回
    :rtype: None
    """
    main = __import__('main')
    guardian = main.guardian_main(mode)
    guardian.start(mode)


def usage():
    print 'are/loader.py <guardian.py> [-p|--persist persist_mode] [-c|--config config_path]'
    print '              -p persist_mode: zookeeper(default), local'
    print '              -c config_path : are/../../conf/ark.conf(default)'


if __name__ == "__main__":
    pmode = "zookeeper"
    if len(sys.argv) < 2:
        usage()
        sys.exit(2)
    try:
        opts, args = getopt.getopt(sys.argv[2:], "hp:c:", ["persist=", "config="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-p", "--persist"):
            pmode = arg
        elif opt in ("-c", "--config"):
            if not os.path.exists(arg) or not os.path.isfile(arg):
                print "config({path}) not exist or not file".format(path=arg)
                sys.exit(2)
            from ark.are import config
            config.GuardianConfig.CONF_DIR = os.path.dirname(arg)
            config.GuardianConfig.CONF_FILE = os.path.basename(arg)

    loader(pmode)

