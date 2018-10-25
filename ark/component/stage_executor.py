# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
``stage_executor`` 分级操作执行器的实现
"""
from are import exception
from are.executor import StateMachineExecutor
from are import stage

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
        if not isinstance(job_adapter, stage.JobAdapter):
            raise exception.ETypeMismatch("job_adapter is not instance of "
                        "JobAdapter, type:{}",format(type(job_adapter)))
        if not isinstance(stage_builder, stage.StageBuilder):
            raise exception.ETypeMismatch("stage_builder is not instance of "
                        "StageBuilder,type:{}".format(type(stage_builder)))
        super(StageStateMachineExecutor, self).__init__(None, process_count)
        self._stage_builder = stage_builder or stage.StageBuilder()
        self._bind_job_type(job_adapter)

    def execute_message(self, message):
        """
        执行消息

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        nodes = self._create_nodes(message)
        state_machine = self._create_state_machine(message, nodes)
        # 状态机启动
        self.graph_start(state_machine)

    def on_extend_message(self, message):
        """
        扩展消息的处理逻辑

        :param Message message: 消息对象
        :return: 无返回
        :rtype: None
        """
        # 执行器从消息泵获取控制消息
        self._control_message[message.operation_id] = message

    def _create_nodes(self, message):
        """
        生成状态机节点
        :param message:
        :return:
        """
        stages_model = self.get_stages_model(message.params)
        stage_list = self._stage_builder.generate_stages(stages_model)
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
