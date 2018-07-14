# -*- coding: UTF-8 -*-
# @Time    : 2018/6/21 下午2:08
# @File    : decision_test.py
"""
decision test
"""
import mock
import unittest
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(BASE_DIR + "/../ark/")

from are import exception
from are import decision
from are import framework


class TestDecision(unittest.TestCase):
    """
    test decision
    """
    @mock.patch.object(framework.Listener, "send")
    @mock.patch.object(decision.DecisionMaker, "decision_logic")
    def test_decision_maker(self, mock_dec, mock_send):
        """

        :return:
        """
        mock_send.return_value = None
        mock_dec.return_value = framework.IDLEMessage()
        dec = decision.DecisionMaker()
        message = framework.IDLEMessage()
        mes_sen = framework.OperationMessage("SENSED_MESSAGE", "ID1", {})
        dec.on_decision_message(mes_sen)
        mes_complete = framework.OperationMessage("COMPLETE_MESSAGE", "ID1", {})
        dec.on_decision_message(mes_complete)
        dec._concerned_message_list = [
            "SENSED_MESSAGE", "COMPLETE_MESSAGE", "IDLE_MESSAGE"]
        dec.on_decision_message(message)
        mes_unconcerned = framework.OperationMessage(
            "UNKNOWN_MESSAGE", "ID1", {})
        self.assertRaises(exception.EUnknownEvent,
                          dec.on_decision_message, mes_unconcerned)

    def test_dec_logic(self):
        """

        :return:
        """
        dec = decision.DecisionMaker()
        message = framework.IDLEMessage()
        self.assertRaises(exception.ENotImplement, dec.decision_logic, message)

    def test_key_mapping_decision(self):
        """

        :return:
        """
        dec = decision.KeyMappingDecisionMaker({"key": "value"}, "strategy")
        message1 = framework.OperationMessage(
            "SENSED_MESSAGE", "id1", {"strategy": "key"})
        message2 = framework.OperationMessage(
            "SENSED_MESSAGE", "id1", {})
        self.assertIsInstance(
            dec.decision_logic(message1), framework.OperationMessage)
        self.assertRaises(exception.ETypeMismatch, dec.decision_logic, message2)

    def test_state_machine_decision(self):
        """

        :return:
        """
        dec = decision.StateMachineDecisionMaker()


if __name__ == "__main__":
    unittest.main()







