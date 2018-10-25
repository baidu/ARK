# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**config** 框架基础配置模块， 封装了GuardianConfig单例配置类，该类主要用于加载智能运维机器人运行过程中会用到的各种环境变量
使用方式如下::

    获取名为"key"的配置，调用方式为 value = GuardianConfig.get("key")
"""

import json
import os


class GuardianConfig(object):
    """
    GuardianConfig单例配置类，Guardian启动时加载配置，加载顺序依次为：系统环境变量、配置文件中的配置、远程环境变量。

    .. Note:: 三者之间为依次增量覆盖的关系，即远程环境变量优先级最高，其次为配置文件，系统环境变量优先级最低。
    """
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CONF_DIR = BASE_DIR + "/../../conf"
    CONF_FILE = "ark.conf"

    __conf = {}

    @classmethod
    def load_sys_env(cls):
        """
        加载系统环境变量

        :return : None
        """
        cls.__conf.update(dict(os.environ))

    @classmethod
    def load_local_env(cls):
        """
        加载本地环境变量

        :return : None
        """
        conf_file_name = "{}/{}".format(cls.CONF_DIR, cls.CONF_FILE)
        with open(conf_file_name, 'r') as config_file:
            json_str = config_file.read()
            cls.__conf.update(json.loads(json_str))

    @classmethod
    def load_remote_env(cls):
        """
        加载远程环境变量
        当前未提供部署sdk，暂不提供远程config支持
        """
        pass

    @classmethod
    def load_config(cls):
        """
        加载配置，包括系统环境变量、本地环境变量、远程环境变量

        :return : None
        """
        cls.load_sys_env()
        cls.load_local_env()
        cls.load_remote_env()

    @classmethod
    def get(cls, key, default=None):
        """
        获取指定的配置项，如果配置项key不存在，则根据传入的default的不同：

        * 传入的default为非None，则会设置该配置项的值为default。
        * 传入的default为None，则触发异常。

        :param str key: 需要获取的配置项的key
        :param str default: 配置项的默认值

        :return: 配置项的值
        :rtype: str
        :raises KeyError: key错误异常
        """
        if not cls.has(key):
            if default is not None:
                cls.__conf[key] = default
        return cls.__conf[key]

    @classmethod
    def set(cls, mapping):
        """
        设置配置项的名值对

        :param dict mapping: 配置值kv对
        :return: 无返回
        :rtype: None
        """
        cls.__conf.update(mapping)

    @classmethod
    def get_all(cls):
        """
        获取所有配置

        :return: 配置
        :rtype: dict
        """
        return cls.__conf

    @classmethod
    def delete(cls, key):
        """
        删除指定的配置项

        :param str key: 要删除的配置项的key
        :return: 无返回
        :rtype: None
        :raises KeyError: key错误异常
        """
        del cls.__conf[key]

    @classmethod
    def has(cls, key):
        """
        判断key是否存在

        :param str key: 要判断的key
        :return: 存在（True）/ 不存在（False）
        :rtype: bool
        """
        return key in cls.__conf
