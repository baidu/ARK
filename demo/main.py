# -*- coding: UTF-8 -*-
# @Time    : 2018/5/24 下午3:49
# @File    : main.py
"""
guardian demo
"""
from assemble.localpull_keymapping import LocalPullKeyMappingGuardian
from are.executor import BaseExecFuncSet
from are import log


class DemoExecFuncSet(BaseExecFuncSet):
    """
    demo exec
    """
    def say_hello(self, params):
        """

        :param params:
        :return:
        """
        log.info("hello, event params:{}".format(params))
        return {}


def guardian_main():
    """

    :return:
    """
    guardian = LocalPullKeyMappingGuardian(
        "../event", {"hello": "say_hello"}, "strategy", DemoExecFuncSet())
    return guardian
