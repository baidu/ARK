# -*- coding: UTF-8 -*-
"""
测试ES sensor功能
"""
from ark.are import config
from ark.component.es_sensor import EsCallbackSensor
from ark.are.framework import IDLEMessage
import time
import Queue

config.GuardianConfig.set({"LOG_CONF_DIR": "./", "LOG_ROOT": "./log"})
config.GuardianConfig.set({"ARK_ES_HOST": "cp01-ark05.epc.baidu.com", "ARK_ES_PORT": "8920"})


class TestEsEvent(EsCallbackSensor):
    """
    测试ES sensor功能
    """
    def send(self, msg):
        """
        重载send函数，直接打印出感知到的事件
        :param msg:
        :return:
        """
        print msg.operation_id, msg.params

    def active(self):
        """
        重载active，通过本地文件做持久化和恢复
        :return:
        """
        self._collector.begin_time = 0
        self._collector.begin_isn = 0
        try:
            with open("context.txt", 'r') as context:
                for context_str in context:
                    result = context_str.split(" ")
                    self._collector.begin_time = int(result[0])
                    self._collector.begin_isn = int(result[1])
        except:
            pass
        EsCallbackSensor.active(self)

    def wait_event(self, block=False):
        """
        从事件队列中取出事件

        :param bool block: 是否阻塞取出事件
        :return: 取出的事件
        :rtype: dict
        """
        try:
            se = self._event_queue.get(block=False)
        except Queue.Empty:
            return

        try:
            context = open("context.txt", 'w')
            context.write("%d %d" % (se.ts, se.ts_seq + 1))
            context.close()
        except:
            pass

        return se.event


if __name__ == '__main__':

    sensor = TestEsEvent("arktst-%Y%m%d", "test_es", "timestamp", max_collect_time=60000,
                         header={"authorization": "token=8a4df08d5def16a2016540b5bf390025"}, es_persist_time=6)
    sensor.active()
    while True:
        message = IDLEMessage()
        sensor.on_message(message)
        time.sleep(1)
