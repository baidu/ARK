# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2019 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
定时感知器实现，该感知器可以在达到定时时间时自动生成事件。定时设置通过定时任务列表
"""

import bisect
import threading
import time
import json

from croniter import croniter
import ark.are.log as log
import ark.are.exception as exception
from ark.are.sensor import PullCallbackSensor


class _CronTimer(object):
    """
    定时器，该类的主要作用是提供自定义的排序函数
    """
    def __init__(self, timer_str, param_str, now=None):
        """
        初始化方法

        :param str timer_str: crontab风格的定时时间
        :param str param_str: 附属参数
        """
        self._timer_str = timer_str
        self._param_str = param_str
        self._current = 0
        self.next(now)

    def next(self, now=None):
        """
        获取下次触发时间

        :return: 下次的触发时间
        :rtype: float
        """
        if now is None:
            now = time.time()

        self._current = croniter(self._timer_str, now).get_next()
        return self._current

    @property
    def current(self):
        return self._current

    @property
    def param(self):
        return self._param_str

    @property
    def timer(self):
        return self._timer_str

    def __eq__(self, other):
        return ((self._timer_str, self._param_str) ==
                (other.timer, other.param))

    def __lt__(self, other):
        # 返回按照触发时间排序的比较结果
        return self. current < other.current


class _CronClock(object):
    """
    该类主要用于管理定时器队列
    """

    def __init__(self):
        """
        初始化方法

        """
        self._timer_queue = []
        self._lock = threading.Lock()

    def delete_cron(self, cron_list):
        """
        删除一组定时器

        :param list cron_list: 待删除的定时器列表
        :return: 返回删除的下标
        :rtype: list
        """

        pos = 0
        now = time.time()
        result = []
        timer_list = []

        # 获取每个定时器下次到期时间，按照到期时间排序
        # 避免self._timer_queue中timer被trigger()重新计算下次执行时间
        with self._lock:
            for cronstr in cron_list:
                timer = _CronTimer(cronstr[0], cronstr[1], now)
                timer_list.append(timer)

            timer_list.sort()


            # 按照下次到期时间的顺序，在定时器队列中寻找相同的定时器位置。将寻找到的位置记录到result中
            # 如果未找到，则忽略
            for timer in timer_list:
                pos = bisect.bisect_left(self._timer_queue, timer, pos)
                for i in range(pos, len(self._timer_queue)):
                    if self._timer_queue[i] == timer:
                        result.append(i)
                    if self._timer_queue[i] > timer:
                        break

            # 按照倒序方式删除对应元素
            for i in range(len(result) - 1, -1, -1):
                self._timer_queue.pop(result[i])

        return result

    def add_cron(self, cron_list):
        """
        增加一组定时器

        :param list cron_list: 待增加的定时器列表
        :return: 返回新增的下标
        :rtype: list
        """

        pos = 0
        now = time.time()
        result = []
        timer_list = []

        # 获取每个定时器下次到期时间，按照到期时间排序
        with self._lock:
            for cronstr in cron_list:
                timer = _CronTimer(cronstr[0], cronstr[1], now)
                timer_list.append(timer)

            timer_list.sort()

            for timer in timer_list:
                pos = bisect.bisect_right(self._timer_queue, timer, pos)
                self._timer_queue.insert(pos, timer)
                result.append(pos)
        return result

    def trigger(self):
        """
        触发一次定时器

        :return: 如果到时触发了一个定时器，则返回这个定时器的附属参数。否则返回None
        :rtype: str
        """

        with self._lock:
            if len(self._timer_queue) > 0 and self._timer_queue[0].current <= int(time.time()):
                top_timer = self._timer_queue.pop(0)
                param = top_timer.param
                top_timer.next()
                bisect.insort_right(self._timer_queue, top_timer)
                return param
            else:
                return None

    def clear(self):
        with self._lock:
            self._timer_queue = []


class CronSensor(PullCallbackSensor):
    """
    定时感知器。通过提供的定时列表，该感知器在某个定时器到时时，生成定时事件
    """

    def __init__(self, reload_interval, timer_interval=3):
        """
        初始化方法

        :param int reload_interval: 重新获取完整定时列表的间隔时间
        :param int timer_interval: 定时处理间隔
        """
        super(CronSensor, self).__init__(timer_interval)

        self._reload_interval = reload_interval
        self._timer_interval = timer_interval
        self._old_cron = set()
        self._clock = _CronClock()
        self._reload_thread = None

    def refresh(self):
        """
        重新获取定时列表。定时列表是一个list，每行均为二值元组：linux风格的定时时间（具体格式参考manpage）、附属参数（字符串）。

        .. Note:: 为了避免影响主消息泵及CronSensor的定时触发，刷新定时触发列表会在单独的子线程进行，因此访问共享数据时应注意加锁

        :return: 返回的定时列表
        :rtype: list
        """

        raise exception.ENotImplement("function is not implement")

    def _reload(self):
        """
        重新加载定时列表。加载时会对新的定时列表与上次获取的做比对。仅处理新增或者删除的定时器

        """
        while not self._stop_tag:
            try:
                current_cron = set(self.refresh())
                delete_list = self._old_cron - current_cron
                add_list = current_cron - self._old_cron
                if len(delete_list) != 0:
                    self._clock.delete_cron(delete_list)
                    log.d("refresh cron list, delete:{num}".format(num=len(delete_list)))
                if len(add_list) != 0:
                    self._clock.add_cron(add_list)
                    log.d("refresh cron list, add:{num}".format(num=len(add_list)))
                self._old_cron = current_cron
            except Exception as e:
                log.f("reload failed, err")

            time.sleep(self._reload_interval)

    def active(self):
        """
        重载感知器生效函数，加入自动刷新线程的生效操作。

        :return: 无返回
        :rtype: None
        :raises ThreadError: 创建线程失败
        """
        super(CronSensor, self).active()
        try:
            self._reload_thread = threading.Thread(
                target=self._reload)
            self._reload_thread.daemon = True
            self._reload_thread.start()
        except threading.ThreadError as e:
            log.r(e, "create new thread err")

    def inactive(self):
        """
        感知器结束工作。该函数会关闭拉取外部事件的线程，并清空事件队列，避免残留事件在感知器再次启动后继续执行。

        :return: 无返回
        :rtype: None
        """
        super(CronSensor, self).inactive()
        self._stop_tag = True
        if self._reload_thread:
            self._reload_thread = None
        self._clock.clear()
        self._old_cron.clear()

    def get_event(self):
        """
        根据当前时间判断是否需要触发事件

        :return: 定时器事件，事件参数以词典KV返回
        :rtype: dict
        """

        param = self._clock.trigger()
        if param:
            return json.loads(param)
        else:
            return None

