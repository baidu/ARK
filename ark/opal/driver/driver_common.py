# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
driver操作工厂类，提供获取driver的实现
"""
from are import log

from are.common import Singleton
from are.common import StringUtil


class DriverFactory(Singleton):
    """
    driver操作工厂类，提供获取driver的实现
    """

    def create_driver(self, platform, driver_type):
        """
        根据平台和类型创建driver

        :param str platform: 平台名
        :param str driver_type: driver类型
        :return: driver对象
        :rtype: BaseDriver
        :raises ImportError: 模块导入异常
        """
        driver_module = StringUtil.camel_to_underline(driver_type) + '_driver'
        concrete_driver_module = platform.lower() + '_' + driver_module
        full_driver_path = 'opal.driver.' + driver_module + \
                           '.' + concrete_driver_module
        # 动态导入 Driver 模块
        log.info('create driver full path:{}'.format(full_driver_path))
        try:
            module_name = __import__(full_driver_path,
                                     fromlist=[concrete_driver_module])
        except ImportError as e:
            log.error('driver: {} of the platform:{} is not '
                      'exist'.format(driver_type, platform))
            raise e
        driver_class_name = StringUtil.underline_to_camel(
            concrete_driver_module.title())
        driver_class = getattr(module_name, driver_class_name)
        driver_obj = driver_class()
        log.info('create driver class:{}'.format(driver_class_name))
        return driver_obj
