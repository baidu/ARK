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


sys.path.append(os.path.abspath('./'))
if not os.path.dirname(__file__):
    sys.path.append(os.path.abspath('../..'))
else:
    sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../../'))
    sys.path.append(
        os.path.abspath(os.path.dirname(__file__) + '/' + '../../../src/'))
    sys.path.append(
        os.path.abspath(os.path.dirname(__file__) + '/' + '../../../bin/'))


def load_usage():
    """
    打印Guardian加载功能的使用方法
    :return:
    """
    print 'ark load <guardian.py> [-p|--persist persist_mode] [-c|--config config_path]'
    print '           -p persist_mode: zookeeper(default), local'
    print '           -c config_path : ../conf/ark.conf(default)'


def load():
    """
    加载执行Guardian

    .. Note:: 不捕获异常，以便可以进行错误追踪

    :return: 无返回
    :rtype: None
    """
    pmode = "zookeeper"
    if len(sys.argv) < 3:
        load_usage()
        sys.exit(2)
    try:
        opts, args = getopt.getopt(sys.argv[3:], "hp:c:", ["persist=", "config="])
    except getopt.GetoptError:
        load_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            load_usage()
            sys.exit()
        elif opt in ("-p", "--persist"):
            pmode = arg
        elif opt in ("-c", "--config"):
            if not os.path.exists(arg) or not os.path.isfile(arg):
                print "config({path}) not exist or not file".format(path=arg)
                sys.exit(2)
            import ark.are.config as config
            config.GuardianConfig.CONF_DIR = os.path.dirname(arg)
            config.GuardianConfig.CONF_FILE = os.path.basename(arg)

    import ark.are.persistence as persistence
    if pmode == "zookeeper":
        persistence.PersistenceDriver = persistence.ZkPersistence
    else:
        persistence.PersistenceDriver = persistence.FilePersistence
    config.GuardianConfig.load_config()
    entry_point = __import__('main')
    guardian = entry_point.guardian_main(pmode)
    return guardian.start(pmode)


def mkenv_usage():
    """
    打印环境准备的使用方法
    :return:
    """
    print 'ark mkenv <path>'


def mkenv():
    """
    执行环境准备
    :return:
    """
    mkenv_usage()


def usage():
    """
    打印使用方法
    :return:
    """
    print 'ark <load|mkenv> ...'


def main():
    """
    主入口
    :return:
    """
    from ark.are.common import patch_logging
    patch_logging()
    if len(sys.argv) < 2:
        usage()
        sys.exit(2)
    if sys.argv[1] == "load":
        load()
    elif sys.argv[1] == "mkenv":
        mkenv()
    else:
        usage()
        sys.exit(2)


if __name__ == "__main__":
    main()
