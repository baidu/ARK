# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**job** 提供了job基本操作方法，平台可继承并实现自身job操作
"""


class Job(object):
    """
    job基类
    """

    def add(self):
        """
        添加/新建一个任务
        :return: job id
        :rtype: str
        """
        pass

    def get(self, job_id):
        """
        获取job

        :param str job_id: job id
        :return: job对象
        :rtype: Job
        """
        pass

    def pause(self, job_id):
        """
        暂停job

        :param str job_id: job id
        :return: 无返回
        :rtype: None
        """
        pass

    def resume(self, job_id):
        """
        恢复job

        :param str job_id: job id
        :return: 无返回
        :rtype: None
        """
        pass

    def cancel(self, job_id):
        """
        撤销job

        :param str job_id: job id
        :return: 无返回
        :rtype: None
        """
        pass
