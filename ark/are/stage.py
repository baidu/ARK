# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
分级操作通用模块，可以用来构造相应的状态机节点
"""
from are import exception
from are import graph
from are import log
from are import context


class StageBuilder(object):
    """
    分级操作基类
    """
    def __init__(self):
        """
        初始化方法
        """
        self._index = 0
        self._total_stage = 0
        self._stage_list = []
        self._adapter = None

    def make_stages(self, stages_model):
        """
        根据分级描述模板，生成最终需要执行的状态机节点列表。节点的生成逻辑需要具体实现。
        一个分级操作描述模板格式为：
        [
          {
            "stage_name": "small",
            "job_list":[
              {
                "task_id":1001,
                "idc":"bjyz",
                "service":"xxx"
              },
              {
                "task_id":1002,
                "idc":"cp01",
                "service":"xxx"
              }
            ]
          },
          {
            "stage_name": "all",
            "job_list":[
              {
                "task_id":1003,
                "idc":"nj03",
                "service":"xxx"
              }
            ]
          }
        ]

        :param list stages_model: 分级策略描述模板
        :return: 返回节点列表
        :rtype: list
        """
        self._begin()
        self._total_stage = len(stages_model)
        for index, stage_desc_dict in enumerate(stages_model):
            stage_name = stage_desc_dict["stage_name"]
            job_list = stage_desc_dict["job_list"]
            if index != self._total_stage - 1:
                next_stage_name = stages_model[index + 1]["stage_name"]
            else:
                next_stage_name = "end"

            self._append_job_stage(stage_name, next_stage_name, job_list)
            self._append_verify_stage(stage_name, next_stage_name,
                                      stage_desc_dict)

        self._end()
        return self._stage_list

    def bind(self, job_adapter):
        """
        绑定任务处理器

        :param JobAdapter job_adapter: 任务处理器
        :return: 无返回
        :rtype: None
        """
        self._adapter = job_adapter

    def _begin(self):
        """
        开始分级操作

        :return: 无返回
        :rtype: None
        :raise EUnInited: 未初始化异常
        """
        if not self._adapter:
            raise exception.EUnInited("job adatper is not binded yet")
        self._index = 0

    def _end(self):
        """
        结束分级操作

        :return: 无返回
        :rtype: None
        """
        end_node_name = "end-job-" + \
                        str(self._total_stage + 1) + "-sub-1"
        end_node = EndNode(end_node_name, True)
        self._stage_list.append(end_node)

    def _append_job_stage(self, stage_name, next_name, job_list):
        """
        生成一个阶段的stage,如发起任务和确认任务。返回一个list
        当多个任务需要同时发起时，先添加多个任务节点，再添加一个确认节点

        :param str stage_name: 状态名
        :param str next_name: 下一个要执行的状态名
        :param list job_list: 分级任务描述
        :return: 状态机的一个节点list
        """
        next_stage_name = None
        for sub_index, job_desc in enumerate(job_list):

            job_stage_name = self._get_job_stage_name(
                stage_name, self._index, sub_index)
            next_stage_name = self._get_next_stage_name(
                job_stage_name, next_name)
            job_node = JobNode(job_stage_name, next_stage_name, job_desc,
                               self._adapter, False)
            self._stage_list.append(job_node)

        job_end_node_name = next_stage_name
        job_verify_node_name = job_end_node_name.split("-sub-")[0].\
            replace("-job-", "-verify-", -1)
        job_end_node = JobEndNode(next_stage_name, job_verify_node_name, True)
        self._stage_list.append(job_end_node)

    def _append_verify_stage(self, stage_name, next_name, job_desc):
        """
        添加一个确认节点

        :param str stage_name: 状态名
        :param str next_name: 下一个要执行的
        :param dict job_desc: 任务描述信息
        :return: 无返回
        :rtype: None
        """
        verify_stage_name = self._get_verify_stage_name(stage_name, self._index)
        next_stage_name = self._get_next_stage_name(
            verify_stage_name, next_name)
        stage_verify = VerifyNode(verify_stage_name, next_stage_name, job_desc,
                                  self._adapter, True)
        self._stage_list.append(stage_verify)
        self._index += 1

    def _get_job_stage_name(self, stage_name, index, sub_index):
        """
        生成任务节点名

        :param str stage_name: 状态名
        :param int index: stage索引
        :param int sub_index: job子索引
        :return: 任务节点名
        :rtype: str
        """
        job_stage_name = stage_name + "-job-" + str(index + 1) + "-sub-" \
                         + str(sub_index + 1)
        return job_stage_name

    def _get_verify_stage_name(self, stage_name, index):
        """
        生成确认节点名

        :param str stage_name: 状态名
        :param int index: stage索引
        :return: 确认节点名
        :rtype: str
        """
        verify_stage_name = stage_name + "-verify-" + str(index + 1)
        return verify_stage_name

    def _get_next_stage_name(self, cur_stage_name, next_name):
        """
        生成下一个状态名

        :param str cur_stage_name: 当前状态名
        :param str next_name: 下一个状态名
        :return: 下一个状态全名
        :rtype: str
        :raise ECheckFailed: 状态名检查失败异常
        """
        cur_stage_name_tmp_list = cur_stage_name.split("-")
        if len(cur_stage_name_tmp_list) < 3:
            raise exception.ECheckFailed("stage name:{} check failed".
                                         format(cur_stage_name))

        stage_name = cur_stage_name_tmp_list[0]
        stage_type = cur_stage_name_tmp_list[1]
        stage_index = cur_stage_name_tmp_list[2]
        if stage_type == "job":
            sub_index = cur_stage_name_tmp_list[4]
            next_stage_name = stage_name + "-job-" + str(stage_index) + \
                              "-sub-" + str(int(sub_index) + 1)
        else:
            next_stage_name = next_name + "-job-" + \
                              str(int(stage_index) + 1) + "-sub-1"
        return next_stage_name


class JobAdapter(context.FlushFlag):
    """
    分级任务描述
    """
    def __init__(self):
        """
        初始化方法。JobAdapter的子类只能具有无参数构造函数

        :param str job_id: 任务id
        """
        self._status = "READY"
        self._job_url = None

    @property
    def status(self):
        """

        :return: status
        :rtype: str
        """
        return self._status

    @status.setter
    def status(self, status):
        """

        :param status:
        :return:
        """
        self._status = status

    @property
    def job_url(self, job_handle):
        """

        :param str job_handle: job句柄，包含job的必要信息，可根据该句柄获取job的当前状态，控制job的暂停，取消等动作
        :return: job_url
        :rtype: str
        """
        return self._job_url

    def create(self, job_desc, node_name, session):
        """
        创建任务

        :param dict job_desc: 任务参数
        :param str node_name: 当前的状态机节点名
        :param object session: 状态机的session
        :return: job_handle
        :rtype: str
        """
        raise exception.ENotImplement("function is not implement")

    def get_result(self, job_handle_list, node_name, session):
        """
        刷新任务状态

        :param list handle_list: job句柄列表，每个句柄都包含job的必要信息，可根据该句柄获取job的当前状态，控制job的暂停，取消等动作
        :param str node_name: 当前的状态机节点名
        :param object session: 状态机的session
        :return: 返回码
        :rtype: int
        """
        raise exception.ENotImplement("function is not implement")

    def control(self, job_handle_list, control_message, session):
        """
        根据控制消息进行任务控制

        :param job_handle_list stage结果
        :param dict control_message: 控制消息
        :param object session: 状态机的session
        :return: 返回码
        :rtype: int
        """
        raise exception.ENotImplement("function is not implement")


class JobNode(graph.Node):
    """
    分级操作任务节点
    """
    def __init__(self, name, next_name, job_desc, job_adapter, reentrance=False):
        """
        初始化方法

        :param str name: 节点名
        :param str next_name: 下一个节点名
        :param dict job_desc: 任务描述参数
        :param cls job_adapter: 任务处理器类
        :param bool reentrance: 是否可重入
        """
        super(JobNode, self).__init__(name, reentrance)
        self._next_name = next_name
        self._job_desc = job_desc
        self._task_controller = job_adapter()

    def process(self, session, current_node, nodes_process):
        """
        节点操作
        :param StateMachineSession session: 运行session信息
        :param str current_node: 当前节点名
        :param dict nodes_process: 节点完成情况
        :return: 下一个需要运行的节点
        :rtype: str
        """
        handle = self._task_controller.create(self._job_desc, self.name, session)
        session.set_flush(self._task_controller.reset_flush())
        if handle is None:
            next_stage_name = "ARK_NODE_END"
        else:
            if session.handle_list is None\
                    or not isinstance(session.handle_list, list):
                session.handle_list = []
            session.handle_list.append(handle)
            next_stage_name = self._next_name
        return next_stage_name


class JobEndNode(graph.Node):
    """
    任务结束节点
    """
    def __init__(self, name, next_name, reentrance=False):
        """
        初始化方法

        :param str name: 节点名
        :param str next_name: 下一个节点名
        :param bool reentrance: 是否可重入
        """
        super(JobEndNode, self).__init__(name, reentrance)
        self._next_name = next_name

    def process(self, session, current_node, nodes_process):
        """
        执行逻辑

        :param object session: session信息
        :param str current_node: 当前节点
        :param dict nodes_process: 节点完成情况
        :return: 下一个需要运行的节点
        :rtype: str
        """
        return self._next_name


class VerifyNode(graph.Node):
    """
    分级操作结果确认节点
    """

    def __init__(self, name, next_name, job_desc, job_adapter, reentrance=False):
        """
        初始化方法

        :param str name: 节点名
        :param str next_name: 下一个节点名
        :param dict job_desc: 任务描述参数
        :param cls job_adapter: 任务处理器
        :param bool reentrance: 是否可重入
        """
        super(VerifyNode, self).__init__(name, reentrance)
        self._job_desc = job_desc
        self._next_name = next_name
        self._task_controller = job_adapter()

    def process(self, session, current_node, nodes_process):
        """
        执行逻辑

        :param object session: session信息
        :param str current_node: 当前节点
        :param dict nodes_process: 节点完成情况
        :return: 下一个需要运行的节点
        :rtype: str
        """
        if session.control_message is not None:
            ret_code = self._task_controller.control(
                session.handle_list, session.control_message, session)
            session.set_flush(self._task_controller.reset_flush())

            if ret_code != 0:
                log.warning("task control failed,task name:{}".format(
                    self.name))
            else:
                session.control_message = None
        ret_code = self._task_controller.get_result(
            session.handle_list, self.name, session)
        session.set_flush(self._task_controller.reset_flush())

        if ret_code == 0:
            log.warning("task:{} finished,run next task:{}".format(
                self.name, self._next_name))
            session.handle_list = []
            next_stage_name = self._next_name
        elif ret_code < 0:
            log.warning("get task result,retcode:{}".format(ret_code))
            session.handle_list = []
            next_stage_name = "ARK_NODE_END"
        else:
            next_stage_name = self.name
        return next_stage_name


class EndNode(graph.Node):
    """
    结束节点
    """
    def process(self, session, current_node, nodes_process):
        """
        执行逻辑

        :param object session: session信息
        :param str current_node: 当前节点
        :param dict nodes_process: 节点完成情况
        :return: 下一个需要运行的节点
        :rtype: str
        """
        return "ARK_NODE_END"
