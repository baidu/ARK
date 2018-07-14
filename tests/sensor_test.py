# -*- coding: UTF-8 -*-
# @Time    : 2018/6/21 下午1:43
# @File    : sensro_test.py
"""
sensor test
"""
import os
import sys
import mock
import unittest
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

from are import sensor
from are import framework


class TestSensor(unittest.TestCase):
    """
    test sensor
    """
    @mock.patch.object(framework.Listener, "send")
    def test_on_sensor_message(self, mock_send):
        """

        :return:
        """
        mock_send.return_value = None
        event_with_id = {"operation_id": "op1"}
        event_without_id = {}
        csen = sensor.CallbackSensor()
        message = framework.IDLEMessage()
        self.assertIsNone(csen.on_message(message))
        csen.callback_event(event_with_id)
        csen.on_message(message)
        csen.callback_event(event_without_id)
        csen.on_message(message)

    def test_pull_sensor(self):
        """

        :return:
        """
        pull_sensor = sensor.PullCallbackSensor()
        pull_sensor.active()
        time.sleep(3)
        pull_sensor.inactive()

    def test_push_sensor(self):
        """

        :return:
        """
        sen = sensor.PushCallbackSensor()


if __name__ == "__main__":
    unittest.main()
