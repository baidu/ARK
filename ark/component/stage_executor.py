# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
``stage_executor`` 分级操作执行器的实现
"""
import ark.are.exception as exception
import ark.are.stage as stage
import ark.are.log as log
from ark.are.executor import StateMachineExecutor


class StageStateMachineExecutor(StateMachineExecutor):
    """
    分级操作状态机执行器
    """

    def __init__(self, job_adapter, stage_builder=None, process_count=1):
        """
        初始化方法

        :param JobAdapter job_adapter: 任务处理器，分级操作解析器与任务处理器绑定，完成分级任务的具体执行操作
        :param StageBuilder stage_builder: 分级操作构建器
        :param int process_count: 最大进程数
        """
        if not issubclass(job_adapter, stage.JobAdapter):
            raise exception.ETypeMismatch("job_adapter is not subclass of "
                                          "JobAdapter, type:{}",format(type(job_adapter)))
        if stage_builder is not None and not isinstance(stage_builder, stage.StageBuilder):
            raise exception.ETypeMismatch("stage_builder is not instance of "
                                          "StageBuilder,type:{}".format(type(stage_builder)))
        super(StageStateMachineExecutor, self).__init__(None, process_count)
        self._stage_builder = stage_builder or stage.StageBuilder()
        self._bind_job_type(job_adapter)

    def exception_handler(self, cause, params):
        """
        当状态机执行出现异常时需要向用户提供处理异常情况的途径，而不仅仅是退出执行
        该函数需要用户继承的子类去实现
        :param cause: 状态机捕获到的异常
        :param params: 用户下发的事件
        :return:
        """
        raise exception.ENotImplement("function is not implement")

    def execute(self, operation):
        """
        执行消息

        :param Operation operation: operation操作对象
        :return: 无返回
        :rtype: None
        """
        try:
            nodes = self._create_nodes(operation)
            state_machine = self._create_state_machine(operation, nodes)
            # 状态机启动
            state_machine.start()
        except IOError as e:
            log.f("state machine run IOError exception, process will exit.")
            try:
                self.exception_handler("state machine run exception:{}".format(e),
                                       operation.operation_params)
            except Exception as e:
                log.f("IOError exception handler")
            log.i("state machine run IOError exception, process exit.")
            import os
            os._exit(9)
        except Exception as e:
            log.f("state machine run exception")
            try:
                self.exception_handler("state machine run exception:{}".format(e),
                                       operation.operation_params)
            except Exception as e:
                log.f("exception handler")
        try:
            del self._control_message[state_machine.session.id]
        except Exception as e:
            log.f("fail to delete control message")
        log.i("state machine run finished, operationId:{}".format(state_machine.session.id))

    def _create_nodes(self, operation):
        """
        生成状态机节点
        :param Operation operation: operation操作对象
        :return:
        """
        stages_model = self.get_stages_model(operation.operation_params)
        stage_list = self._stage_builder.make_stages(stages_model)
        return stage_list

    def get_stages_model(self, params):
        """
        生成分级操作描述模板

        :param dict params: 分级操作参数
        :return: 分级操作描述模板
        :rtype: list
        """
        return params["stage_description"]

    def _bind_job_type(self, job_adapter):
        """
        绑定任务处理器

        :param JobAdapter job_adapter: 任务处理器
        :return: 无返回
        :rtype: None
        """
        self._stage_builder.bind(job_adapter)
