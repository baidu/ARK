# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**common** 框架基础公用模块，该模块包含一些提供基本功能的类，方便框架中的其他类继承，目前封装了如下工具：

* ``Singleton`` 单例基类，在本框架中，如果要求仅有一个实例的类必须继承自该类，
继承该基类的派生类对应的实例在整个框架中有且仅一份。如，`LoggerContext`，`LoggerFactory`，`GuardianContext`，`BaseClient`，`DriverFactory`
* ``SingleDict`` 单例字典基类
* ``OpObject`` 运维对象基类，对多种运维对象(如，机器、实例、服务和应用等)公共属性的统一抽象
* ``StringUtil`` 字符串操作工具类，统一封装字符串相关的处理接口
"""

from are import exception
import re


class Singleton(object):
    """
    单例基类，封装了类的构造方法
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        构造方法

        :param tuple args: 可变参数
        :param dict kwargs: 关键字参数
        """
        if not cls._instance:
            cls._instance = super(Singleton, cls).__new__(cls, *args, **kwargs)
        return cls._instance


class SingleDict(dict):
    """
    单例字典，封装类的构造方法和值的存取，为后续扩展或用户使用提供便利
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        构造方法

        :param tuple args: 可变参数
        :param dict kwargs: 关键字参数
        """

        if cls._instance is None:
            cls._instance = super(SingleDict, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __getitem__(self, key):
        """
        单例字典魔术方法，返回键key对应的值

        :param str key: 键参数
        :return: 键对应的值
        :rtype: str
        :raises: KeyError 键对应的值不存在
        """

        try:
            return dict.__getitem__(self, key)
        except KeyError:
            raise exception.EMissingParam("key [{}] not exist".format(key))


class OpObject(object):
    """
    运维对象抽象基类，统一抽象出多种运维对象(如，机器、实例、服务和应用等)的公共属性。
    这些属性包括类型type、名字name和其他相关信息refer。
    """

    def __init__(self, type, name, refer=None):
        """
        初始化方法

        :param str type: 运维对象的类型
        :param str name: 运维对象的名字
        :param str refer: 运维对象的uid
        """
        self.__type = type
        self.__name = name
        self.__refer = refer

    @property
    def type(self):
        """
        返回运维对象类型

        :return: 运维对象类型
        :rtype: str
        """
        return self.__type

    @property
    def name(self):
        """
        返回运维对象名字

        :return: 运维对象名字
        :rtype: str
        """
        return self.__name

    @property
    def refer(self):
        """
        返回运维对象其他相关信息

        :return: 运维对象uid
        :rtype: str
        """
        return self.__refer


class StringUtil(object):
    """
    字符串操作工具类，封装了字符串命名方式（驼峰和下划线）的转换
    """

    @staticmethod
    def camel_to_underline(camel_format):
        """
        驼峰命名格式转下划线命名格式

        :param str camel_format: 驼峰格式的字符串
        :return: 下划线分隔的字符串
        :rtype: str
        """

        # 匹配正则，匹配小写字母或者数字和大写字母的分界位置
        p = re.compile(r'([a-z]|\d)([A-Z])')
        # 这里第二个参数使用了正则分组的后向引用
        underline_format = re.sub(p, r'\1_\2', camel_format).lower()
        return underline_format

    @staticmethod
    def underline_to_camel(underline_format):
        """
        下划线命名格式转驼峰命名格式

        :param str underline_format: 下划线格式的字符串
        :return: 驼峰格式的字符串
        :rtype: str
        """
        # 这里re.sub()函数第二个替换参数用到了一个匿名回调函数，回调函数的参数x为一个匹配对象，返回值为一个处理后的字符串
        camel_format = re.sub(r'(_\w)', lambda x: x.group(1)[1].upper(),
                              underline_format)
        return camel_format 
