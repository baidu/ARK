# -*- coding: UTF-8 -*-

from ark.component.cron_sensor import CronSensor
from ark.are.framework import IDLEMessage
import time


class TestCron(CronSensor):
    def send(self, msg):
        print msg.operation_id, msg.params

    def refresh(self):
        with open("cron.list", 'r') as cron_file:
            result = []
            for cron_str in cron_file:
                result.append(tuple(cron_str.split(";")))
            return result
        return []


if __name__ == '__main__':
    sensor = TestCron(30, 5)
    sensor.active()
    while True:
        message = IDLEMessage()
        sensor.on_message(message)
        time.sleep(1)
