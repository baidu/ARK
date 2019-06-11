# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**graph** 模块提供了基于图论的长流程控制模型，封装了两种长流程执行的标准作业流程。

当前普遍的运维操作方法，与最早起的全手工相比，已经有了很大变化，但基本模式并没有本质的
不同，其核心仍然是：查看服务状态→执行操作→检查执行结果的循环。

过去我们通过两种主要的方式来提升运维效率：
1. 部分组件自动化，代价是控制策略简单，风险较大。典型如使用报警回调方式自动故障发现和自动故障处理执行。
2. 固化完整运维操作的控制、处理流程，代价是可扩展性较差，业务变化需要大规模的调整流程。

ARK框架从过去的运维处理中，抽象核心的模式固化成框架，解决风险和扩展性不能两全的问题：
1. 按照运维操作流程和操作模式抽象，而非提供原生的技术实现抽象
2. 实际的技术抽象隐藏在用户视野之外，避免将功能组件用于非预期的场合

基于此，我们抽象出基于图算法的处理模式：通过定义一组离散可枚举的状态，及各状态下所对应的处理动作，实现面向状态的执行处理。根据状态间切换的确定性与否，又细分为两种运行模型：
1. 状态机模型(StateMachine)：各状态处理完成后的后续状态是确定的，可直接进行后续状态的执行。
2. 流程图模型(DepedencyFlow)：状态处理完成后的后续状态是不确定的，此时需要对各状态进行检查，确认是否进入该状态的处理。

使用标准作业流程的好处：
1. 面向状态运维，状态处理具有可复用性，并且流程动态生成，可以应对复杂业务变化，具有强大的扩展能力。
2. 长流程分步执行，可在运行关键点处进行checkpoint，更好地解决了单实例故障时的可用性问题。
"""
import copy

from ark.are import exception
from ark.are import log
from ark.are import context


class BaseGraph(object):
    """
    图基类，基于图论的长流程控制基类，封装了长流程控制操作的基本接口

    一个简单的demo状态机（流程图）::

        #定义一组状态
        class AddTask(Node):
            def process(self, session, current_node, nodes_process):
                print "add task success"
                return "check_task"


        class CheckTask(Node):
            def process(self, session, current_node, nodes_process):
                print "check task success"
                return "ARK_NODE_END"

        # 生成节点对象
        node1 = AddTask(name="add_task", reentrance=False)
        node2 = CheckTask(name="check_task", reentrance=True)

        # 生成状态机对象，方便运行数据存储，这里session用{}
        state_machine = StateMachine({})
        state_machine.add_node(node1)
        state_machine.add_node(node2)
        state_machine.start()

        # 控制状态机暂停
        state_machine.pause()

        # 控制状态机恢复运行
        state_machine.resume()

        # 控制状态机结束运行
        state_machine.cancel()

    """

    class Status(object):
        """
        长流程控制状态，数值为有限值集合，用于整体流程生命周期控制。

        """
        CREATED = 0
        INITED = 1
        RUNNING = 2
        PAUSED = 3
        CANCELLED = 4
        FINISHED = 4
        FAILED = 4

    _ARK_NODE_END = "ARK_NODE_END"

    def __init__(self, session):
        """
        初始化方法

        :param object session: 状态机会话对象，通常保存运行实例的运行信息
        """
        self._session = session
        self._nodes = []
        self._nodes_process = {}
        self._current_node = None
        self._status = self.Status.CREATED

    @property
    def session(self):
        """
        session属性

        :return:
        """
        return self._session

    @property
    def status(self):
        """
        运行状态

        :return:
        """
        return self._status

    @status.setter
    def status(self, status):
        """
        修改运行状态

        :param int status: 运行状态
        :return: 无返回
        :rtype: None
        """
        self._status = status

    def add_node(self, node):
        """
        增加节点

        :param Node  node: 节点
        :return: None
        :raises ETypeMismatch: 节点类型错误
        :raises EInvalidOperation: 操作不合法
        """
        if not isinstance(node, Node):
            raise exception.ETypeMismatch("param node type must be Node")
        elif node in self._nodes:
            raise exception.EInvalidOperation(
                "node {} already added".format(node.name))
        else:
            self._nodes.append(node)
            self._nodes_process[node.name] = False

    def get_node(self, node_name):
        """
        获得节点

        :param str node_name: 节点名字
        :return: 节点对象引用
        :rtype: Node引用
        :raise EUnknownNode: 未知节点
        """
        for node in self._nodes:
            if node.name == node_name:
                return node

        raise exception.EUnknownNode("node:{} unknown".format(node_name))

    def prepare(self):
        """
        状态机创建之后初次检查, 并设置当前状态节点

        :return: None
        :raises EUnInited: 未初始化
        :raises EStatusMismatch: 状态不匹配
        """
        if self._status != self.Status.CREATED \
                and self._status != self.Status.INITED:
            raise exception.EStatusMismatch(
                "Only in created or inited status can call "
                "this method.current status:{}".format(self._status))
        if not self._nodes:
            raise exception.EUnInited("nodes list length is 0")
        self.status = self.Status.INITED
        self._current_node = self._nodes[0].name

    def pause(self):
        """
        暂停状态机

        :return: None
        :raises EStatusMismatch: 状态不匹配
        """
        if self._status == self.Status.RUNNING:
            self._status = self.Status.PAUSED
        else:
            raise exception.EStatusMismatch(
                "Only in running status can call this method. "
                "current status:{}".format(self._status))

    def resume(self):
        """
        恢复状态机

        :return: None
        :raises EStatusMismatch: 状态不匹配
        """
        if self._status == self.Status.PAUSED:
            self._status = self.Status.RUNNING
        else:
            raise exception.EStatusMismatch(
                "Only in paused status can call this method."
                "current status:{}".format(self._status))

    def cancel(self):
        """
        取消状态机

        :return: None
        """
        self._status = self.Status.CANCELLED

    def run_next(self):
        """
        接口方法， 进行一个状态的轮转，子类需要根据具体的轮转逻辑，实现该方法

        :return: None
        :raises ENotImplement: 该接口未实现
        """
        raise exception.ENotImplement()

    def start(self):
        """
        启动状态机

        :return: None
        :raises EStatusMismatch: 状态不匹配
        :raises Exception: 通用异常
        """
        self.prepare()
        if self._status == self.Status.INITED \
                or self._status == self.Status.PAUSED:
            self._status = self.Status.RUNNING
        else:
            raise exception.EStatusMismatch(
                "Only in inited or paused status can call "
                "this method.current status:{}".format(self._status))
        while self._status == self.Status.RUNNING:
            try:
                self.run_next()
            except Exception as e:
                self._status = self.Status.FAILED
                log.r(e, "start fail in run_next")

    def load(self, session, node_process, current_node, status):
        """
        重新加载实例，适用于实例迁移后运行状态的回滚操作

        :param object session: 运行session信息
        :param dict node_process: 节点运行状态
        :param str current_node: 当前运行节点
        :param str status: 当前运行状态
        :return: 无返回
        :rtype: None
        """
        self._session = session
        self._nodes_process = node_process
        self._current_node = current_node
        self._status = status

    def dump(self):
        """
        生成运行时动态信息

        :return: 运行信息
        :rtype: dict
        """
        attribute = {
            "status": self._status,
            "current_node": self._current_node,
            "nodes_process": self._nodes_process,
            "session": self._session}
        return attribute


class Node(object):
    """
    状态机节点基类
    """

    def __init__(self, name, reentrance=False):
        """
        初始化方法

        :param str name: 节点名字
        :param bool reentrance: 节点是否可重入，此属性标识一个节点的执行是否是幂等（可重复）的，默认为False
        """
        self.__name = name
        self.__reentrance = reentrance

    @property
    def name(self):
        """
        获取节点方法

        :return: 节点名字
        :rtype: str
        """
        return self.__name

    @property
    def reentrance(self):
        """
        获取节点是否可重入属性，此属性标识一个节点的执行是否是幂等（可重复）的。状态机轮转时（
        或机器人实例迁移后运行状态恢复时）根据此标识，判断是否重新进入该状态的执行。

        .. Note:: 通常情况下，我们认为读操作是可重入的，写操作是不可重入的。如发起任务的操作不可重入，
        查询任务状态的操作可重入。在进行状态划分时，"是否可重入"是划分状态的一个重要维度

        :return: True或False
        :rtype: bool
        """
        return self.__reentrance

    def check(self, session, current_node, nodes_process):
        """
        节点检查接口

        :param object session: 状态机运行信息
        :param str current_node: 当前节点
        :param dict nodes_process: 节点运行情况
        :return: 是否检查通过
        :rtype: bool
        :raises ENotImplement: 接口未实现
        """
        raise exception.ENotImplement()

    def process(self, session, current_node, nodes_process):
        """
        节点处理接口

        :param object session: 状态机运行信息
        :param str current_node: 当前节点
        :param dict nodes_process: 节点运行情况
        :return: 返回下一个节点名
        :raises ENotImplement: 接口未实现
        """
        raise exception.ENotImplement()


class State(Node):
    """
    状态机节点实现类
    """

    def check(self, session, current_node, nodes_process):
        """
        节点检查接口，状态机运行每次返回一个确定的后续状态，因此状态机节点的check结果默认为True

        :param object session: 运行session信息
        :param str current_node: 当前节点名
        :param dict nodes_process: 所有节点的运行状态
        :return: 是否检查通过
        :rtype: bool
        """
        return self.name == current_node


class StateMachine(BaseGraph):
    """
    状态机运行模式。状态机模式是一种无并发场景下的流程执行引擎，适用于管理单个运维实体的状态变迁。

    .. Note:: 用状态机模式来管理并发流程，或者是任务批次执行流程也是可以的，但是需要引入较多的限定，和更复杂的流程阶段控制。

    """

    def run_next(self):
        """
        进行一次状态轮转

        .. Note:: 状态机模型中，每个状态处理完成后需要返回一个确定的状态，可直接进行处理；若返回的状态不存在，直接抛出异常

        :return: 无返回
        :rtype: None
        :raise ECheckFailed: 检查失败
        :raise EUnknownNode: 未知节点
        """
        state = self.get_node(self._current_node)
        if not state.reentrance and self._nodes_process[state.name]:
            raise exception.ECheckFailed(
                "node:{} is finished and not reentrance".format(state.name))
        ret = state.check(self._session, self._current_node,
                          self._nodes_process)
        log.i("node {} check ret:{}".format(self._current_node, ret))
        if ret:
            self._nodes_process[state.name] = True
            current_state = state.process(self._session, self._current_node,
                                          self._nodes_process)
            log.i("node process finished, next node:{}".format(
                current_state))
            if current_state == self._ARK_NODE_END:
                self._current_node = current_state
                self._status = self.Status.FINISHED
                return
            elif current_state not in self._nodes_process:
                raise exception.EUnknownNode(
                    "return state[{}] unkown".format(current_state))
            else:
                self._current_node = current_state
        else:
            raise exception.ECheckFailed(
                "node:{} check failed".format(state.name))


class DependencyFlow(BaseGraph):
    """
    工作流运行模式
    """
    def run_next(self):
        """
        进行一次状态轮转

        .. Note:: 工作流模型中，每个状态处理完成后，下一次需要轮转的状态是不确定的（或者只提供下一个建议执行的状态），因此使用工作流模型，需要自己定义各个状态的 ``check``方法；
        状态处理完成后启动对各状态的检查，检查通过的状态，进入处理阶段。

        .. Note:: 在某个状态完成后，会从其返回的建议的下一个运行状态开始遍历（如未返回建议状态，则从状态列表中此状态的下一个开始），以提高命中效率

        :return: 无返回
        :rtype: None
        """
        node = self.get_node(self._current_node)
        index = self._nodes.index(node)
        index_list = range(index, len(self._nodes))
        index_list.extend(range(0, index))
        for i in index_list:
            node = self._nodes[i]
            if not node.reentrance and self._nodes_process[node.name]:
                continue
            else:
                ret = node.check(self._session, self._current_node,
                                 self._nodes_process)
                log.i("node {} check ret:{}".format(self._current_node, ret))
                if ret:
                    self._nodes_process[node.name] = True
                    current_node = node.process(
                        self._session, self._current_node, self._nodes_process)
                    log.i("node process finished, suggest next "
                             "node:{}".format(current_node))
                    if current_node == self._ARK_NODE_END:
                        self._status = self.Status.FINISHED
                    elif current_node not in self._nodes_process:
                        self._current_node = self._nodes[
                            (i + 1) % len(self._nodes)].name
                    else:
                        self._current_node = current_node
                    return
                else:
                    continue


class PersistedStateMachineSession(context.FlushFlag):
    """
    状态机session定义，一个状态机session，与状态机对象绑定，记录状态机运行信息（
    如当前节点，节点运行状态，控制消息等），根据状态机session可完全恢复中断的状态机运行状态

    .. Note:: session中的控制消息（control_message）应在处理完成之后被清理，否则会造成重复触发
    """

    def __init__(self, id=None, params=None, current_node=None,
                 nodes_process=None, status=None,
                 control_message=None, last_control_id=None):
        """
        初始化方法

        :param str id: 操作id
        :param dict params: 自定义参数
        :param current_node: 当前节点
        :param dict nodes_process: 节点执行信息
        :param str status: 状态机当前状态
        :param dict control_message: 控制消息
        :param list handle_list: stage处理结果
        :param str last_control_id: 控制消息id
        """
        self.id = id
        self.params = params
        self.current_node = current_node
        self.nodes_process = nodes_process
        self.status = status
        self.control_message = control_message
        self.handle_list = []
        self.last_control_id = last_control_id


class PersistedStateMachine(StateMachine):
    """
    提供具备持久化能力的状态机模式实现。具体的持久化方式由调用者实现。

    .. Note:: 持久化状态机依赖PersistedStateMachineSession管理session，其中的控制消息（control_message）应在处理完成之后被清理，否则会造成重复触发
    """

    def set_helper(self, helper):
        """
        设置用于提供相应的持久化功能的工具类实现

        :param PersistedHelper helper: 持久化工具类的实例
        :return: 无返回
        :rtype: None
        """
        self._helper = helper

    def start(self):
        """
        带有持久化功能的状态机启动执行。状态机启动后，会根据每个节点执行的返回值，执行下一个节点，直到返回
        结束或执行异常。在每个节点执行完成后，会向结果队列中发送消息，由主进程进行处理



        :return: 无返回
        :rtype: None
        """
        session = self.session
        while True:
            control_id, control_message = self._helper.get_control_message(session)
            if control_id is not None:
                # 状态机第一次处理控制消息或控制消息为最新还没被处理
                if session.last_control_id is None \
                        or session.last_control_id != control_id:
                    control_message_cp = copy.deepcopy(control_message)
                    session.last_control_id = copy.deepcopy(control_id)
                else:
                    control_message_cp = None
            else:
                control_message_cp = None

            # session中的控制消息应在处理完成之后被清理
            session.control_message = control_message_cp
            if session.control_message is not None:
                # 感知到控制消息后要强制持久化session，避免控制消息丢失
                self._helper.persist(
                    reason=PersistedStateMachineHelper.Reason.CONTROL,
                    session=session,
                    finished_name=None,
                    next_name=None
                )

            # 第一次运行的状态机，记录第一个节点信息
            if self.status == self.Status.INITED:
                todo_params = self.dump()
                todo_node_name = todo_params["current_node"]
                self._helper.persist(
                    reason=PersistedStateMachineHelper.Reason.STARTED,
                    session=session,
                    finished_name=None,
                    next_name=todo_node_name
                )
                self.status = self.Status.RUNNING

            if self.status == self.Status.RUNNING:
                try:
                    finished_node_name = self.dump()["current_node"]
                    self.run_next()
                    session.nodes_process[finished_node_name] = True

                    finished_state = self.dump()
                    todo_node_name = finished_state["current_node"]
                    session.status = finished_state["status"]

                    if self.status == self.Status.FINISHED:
                        session.current_node = None
                    else:
                        session.current_node = self. \
                            get_node(todo_node_name)

                    session.nodes_process = copy.copy(
                        finished_state["nodes_process"])

                    # 节点变更或需要强制刷新
                    if finished_node_name != todo_node_name or session.reset_flush():
                        self._helper.persist(
                            reason=PersistedStateMachineHelper.Reason.NODE_CHANGED,
                            session=session,
                            finished_name=finished_node_name,
                            next_name=todo_node_name
                        )

                except Exception as e:
                    self.status = self.Status.FAILED
                    log.r(e, "start fail in running")
            else:
                break


class PersistedStateMachineHelper(object):
    """
    状态机持久化工具类，用来提供状态机持久化所需的接口扩展。

    """
    class Reason(object):
        """
        持久化的原因

        """
        CONTROL = 0
        STARTED = 1
        NODE_CHANGED = 2

    def get_control_message(self, session):
        """
        获取当前是否有控制消息需要处理。如果没有，则应返回None, None

        :param object session: 状态机的session
        :return: 控制消息ID，控制消息
        :rtype: str, object
        """
        raise exception.ENotImplement("function is not implement")

    def persist(self, session, message_name, finished_name, next_name):
        """
        提供必要的持久化实现

        .. Note:: session中的控制消息应在处理完成之后被清理，否则会造成重复触发

        :param object session: 状态机的session
        :param str message_name: 状态机的session
        :param str finished_name: 已经完成的节点名
        :param str next_name: 下一个将处理的节点名
        :return: 无返回
        :rtype: None
        """
        raise exception.ENotImplement("function is not implement")
