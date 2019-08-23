# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
使用从ElasticSearch拉取外部事件的方式实现的感知器
"""
import time
import Queue
import datetime
from ark.are.sensor import PullCallbackSensor
from ark.are.client import BaseClient
from ark.are import log
from ark.are import config
from ark.are import context
from ark.are import exception


class EsEventDriver(object):
    """
    提供不同版本ElasticSearch的QueryDsl适配转换。通过ES存储Event默认有如下假设：

    * Document中包含时间字段（field），用来记录事件发生时间，或开始时间和结束时间。Document写入后，如果被更新则一定同步更新事件字段
    * Index通过日期分表（可选），超过保存周期的Index将被删除

    """
    ARK_ES_HOST = "ARK_ES_HOST"
    ARK_ES_PORT = "ARK_ES_PORT"

    def __init__(self, index_tmpl, type, time_k,
                 filter_kv=None,
                 mustnot_kv=None,
                 should_kv=None, minimum_should_match=1,
                 header=None, query_size=100):
        """

        :param str index_tmpl: 需要查询的事件所处的index的模版，可以根据当前日期生成实际的index
        :param str type:  需要查询的事件的所处的type
        :param str time_k:  需要查询的事件的时间属性名
        :param dict filter_kv: field名（k）必须为指定值（v），可传入多个表示必须同时满足
        :param dict mustnot_kv: field名（k）必须不为指定值（v），可传入多个表示必须同时满足
        :param dict should_kv: field名（k）可选为指定值（v），可传入多个表示尽量同时满足
        :param int minimum_should_match: should_kv中filed名值最少满足数量
        :param dict header: http请求头
        :param int query_size: 单次查询数量

        """
        self._host = config.GuardianConfig.get(self.ARK_ES_HOST)
        self._port = int(config.GuardianConfig.get(self.ARK_ES_PORT))
        self._index_tmpl = index_tmpl
        self._type = type
        self._filter_kv = filter_kv
        self._mustnot_kv = mustnot_kv
        self._should_kv = should_kv
        if self._filter_kv is None:
            self._filter_kv = {}
        if self._mustnot_kv is None:
            self._mustnot_kv = {}
        if self._should_kv is None:
            self._should_kv = {}
        self._minimum_should_match = minimum_should_match
        self._time_k = time_k
        self._client = BaseClient()  # 避免继承于BaseClient单例类
        self._header = header
        self._query_size = query_size

    def _compose_filter(self, time_begin, time_end):
        """
        根据过滤项/或者过滤表达式，以及时间字段，构造完整的ES查询请求

        :param int time_begin: 查询筛选的开始时间
        :param int time_end:  查询筛选的结束时间
        :return: 拼接好的查询请求
        :rtype: str
        :raises ENotImplement: 未实现
        """
        raise exception.ENotImplement("function is not implement")

    def _query_event_perpage(self, index, time_begin, time_end, pageno):
        """
        处理分页文档中的每一页

        :param str index: 索引名
        :param int time_begin: 查询筛选的开始时间
        :param int time_end:  查询筛选的结束时间
        :param int pageno:  当前请求的页号
        :return: (事件文档列表, 所有匹配的总数量)
        :rtype: (list, int)

        """

        url = "/{}/{}/_search".format(index, self._type)
        filter_data = self._compose_filter(time_begin, time_end)
        data = '{%s, "size": %d, "from": %d}' % \
               (filter_data, self._query_size, pageno * self._query_size)
        response_code = [200, 404]
        ret = self._client.http_request(self._host, self._port, "GET", url,
                                        header=self._header,
                                        data=data, response_code=response_code)
        if "status" in ret:
            code = ret["status"]
            if code == 404:
                return [], 0

        event_list = []
        total = ret["hits"]["total"]
        for event in ret["hits"]["hits"]:
            event["_source"]["_id"] = event["_id"]
            event_list.append(event["_source"])
        return event_list, total

    def query_perday(self, index, time_begin, time_end):
        """
        处理分页文档，将分页文档合并成完整的文档列表

        :param str index: 索引名
        :param int time_begin: 查询筛选的开始时间
        :param int time_end:  查询筛选的结束时间
        :return: 事件文档列表
        :rtype: list

        """
        pageno = 0
        event_day = []
        while True:
            event_list, total = self._query_event_perpage(index, time_begin, time_end, pageno)
            event_day.extend(event_list)
            pageno += 1
            if pageno * self._query_size >= total:
                break
        return event_day

    def query_all(self, time_begin, time_end):
        """
        根据查询条件，查询出满足条件并且在指定时间范围内的事件文档，如果时间范围跨天，则会针对每一天都进行查询后再将合并的结果返回

        :param int time_begin: 查询筛选的开始时间
        :param int time_end:  查询筛选的结束时间
        :return: 事件文档列表
        :rtype: list
        """
        begtime = datetime.datetime.fromtimestamp(time_begin)
        endtime = datetime.datetime.fromtimestamp(time_end)
        end_index = endtime.strftime(self._index_tmpl)
        curtime = begtime
        event_all = []
        while True:
            index = curtime.strftime(self._index_tmpl)
            event_all.extend(self.query_perday(index, time_begin, time_end))
            if index == end_index:
                break
            curtime = curtime + datetime.timedelta(days=1)
        return event_all

    def get_ts(self, event):
        """
        从查询到的事件文档中，根据记录的时间戳filed的名，获取到时间戳

        :param event: 调用query_event返回的事件文档list中的一个元素
        :return: 时间戳
        :rtype: int
        """
        return event[self._time_k]

    def get_id(self, event):
        """
        从查询到的事件文档中，获取事件id

        :param event: 调用query_event返回的事件文档list中的一个元素
        :return: 事件的唯一id
        :rtype: str
        """
        return event["_id"]


class EsEventDriver2x(EsEventDriver):
    """
    提供ElasticSearch2及以上版本的QueryDsl适配转换
    """

    def _compose_filter(self, time_begin, time_end):
        """
        根据过滤项/或者过滤表达式，以及时间字段，构造完整的ES查询请求

        :param int time_begin: 查询筛选的开始时间
        :param int time_end:  查询筛选的结束时间
        :return: 拼接好的查询请求
        :rtype: str
        :raises ENotImplement: 未实现
        """
        request_model = \
            '"query": {"bool": {"filter": [%s],"must_not": [%s],"should": [%s],' \
            '"minimum_should_match": %d,"boost": 1.0}}'

        filter_clause_item = ['{"term": {"%s": "%s"}}' % (k, v) for k, v in self._filter_kv.iteritems()]
        filter_clause = ", ".join(filter_clause_item)
        should_clause_item = ['{"term": {"%s": "%s"}}' % (k, v) for k, v in self._should_kv.iteritems()]
        should_clause = ", ".join(should_clause_item)
        mustnot_clause_item = ['{"term": {"%s": "%s"}}' % (k, v) for k, v in self._mustnot_kv.iteritems()]
        mustnot_clause = ", ".join(mustnot_clause_item)
        time_range_clause = '{"range": {"%s": {"gte": %d,"lt": %d}}}' % (self._time_k, time_begin, time_end)
        if filter_clause != "":
            filter_clause = time_range_clause + "," + filter_clause
        else:
            filter_clause = time_range_clause
        return request_model % (filter_clause, mustnot_clause, should_clause, self._minimum_should_match)


class EsEventDriver1x(EsEventDriver):
    """
    提供ElasticSearch1.x版本的QueryDsl适配转换
    """

    def _compose_filter(self, time_begin, time_end):
        """
        根据过滤项/或者过滤表达式，以及时间字段，构造完整的ES查询请求

        :param int time_begin: 查询筛选的开始时间
        :param int time_end:  查询筛选的结束时间
        :return: 拼接好的查询请求
        :rtype: str
        :raises ENotImplement: 未实现
        """
        request_model = \
            '"query": {"filtered": {"filter": {"bool": {"must": [%s], "must_not": [%s], "should": [%s]}}}}'

        filter_clause_item = ['{"term": {"%s": "%s"}}' % (k, v) for k, v in self._filter_kv.iteritems()]
        filter_clause = ", ".join(filter_clause_item)
        should_clause_item = ['{"term": {"%s": "%s"}}' % (k, v) for k, v in self._should_kv.iteritems()]
        should_clause = ", ".join(should_clause_item)
        mustnot_clause_item = ['{"term": {"%s": "%s"}}' % (k, v) for k, v in self._mustnot_kv.iteritems()]
        mustnot_clause = ", ".join(mustnot_clause_item)
        time_range_clause = '{"range": {"%s": {"gte": %d,"lt": %d}}}' % (self._time_k, time_begin, time_end)
        if filter_clause != "":
            filter_clause = time_range_clause + "," + filter_clause
        else:
            filter_clause = time_range_clause
        return request_model % (filter_clause, mustnot_clause, should_clause)


class SortableEvent(object):
    """
    记录单个event数据，及该event所处的时间戳和时间戳内序号
    """
    def __init__(self, ts, uid, event):
        """
        :param int ts: 事件的时间戳
        :param str uid: 事件的唯一id
        :param dict event: 事件文档
        """
        self._ts = ts
        self._ts_seq = 0
        self._event = event
        self._id = uid

    @property
    def uid(self):
        """
        获取事件的唯一id

        :return:
        :rtype: int
        """
        return self._id

    @property
    def ts(self):
        """
        获取事件的时间戳

        :return:
        :rtype: int
        """
        return self._ts

    @property
    def ts_seq(self):
        """
        获取事件的时间戳内序号

        :return:
        :rtype: int
        """
        return self._ts_seq

    @ts_seq.setter
    def ts_seq(self, v):
        """
        设置事件的时间戳内序号

        :param int v: 序号
        :return: 无
        :rtype: None
        """
        self._ts_seq = v

    @property
    def event(self):
        """
        获取事件

        :return:
        :rtype: dict
        """
        return self._event

    def __lt__(self, other):
        if int(self.ts) == int(other.ts):
            return str(self.uid) < str(other.uid)
        else:
            return int(self.ts) < int(other.ts)


class EsEventCollector(object):
    """
    从ES中持续的收集事件。事件扩展了普通的ES文档定义，除了id字段之外，还包括一个标记事件开始的时间和结束的时间。
    """
    def __init__(self, event_driver, timestamp, id_seq_no=0, es_persist_time=1, max_collect_time=600):
        """
        :param EsEventDriver event_driver: 用于连接Es的EsEventDriver类对象
        :param int timestamp: 采集事件的起始时间戳
        :param int id_seq_no: 采集事件的起始时间戳内的序号
        :param int es_persist_time: es的持久化时间，即now-es_persist_time之前的事件才能保证已经被持久化完成
        :param int max_collect_time: 最长事件采集时间区间。超过此区间的事件将被忽略

        """
        self._driver = event_driver
        self._begin_time = timestamp  # 重新加载context后才会更新该变量，记录上次处理的时间以便断点恢复
        self._collect_begin_time = timestamp
        self._begin_isn = id_seq_no  # 重新加载context后才会更新该变量，记录上次处理的位置以便断点恢复
        self._persist_time = es_persist_time
        self._cache = []
        self._max_collect_time = max_collect_time

    @property
    def begin_time(self):
        """
        获取采集事件的起始时间戳

        :return:
        :rtype: int
        """
        return self._begin_time

    @begin_time.setter
    def begin_time(self, v):
        """
        设置采集事件的起始时间戳

        :param int v: 时间戳
        :return: 无
        :rtype: None
        """
        self._begin_time = v
        if self._collect_begin_time < v:
            self._collect_begin_time = v

    @property
    def begin_isn(self):
        """
        获取采集事件的起始时间戳内序号

        :return:
        :rtype: int
        """
        return self._begin_isn

    @begin_isn.setter
    def begin_isn(self, v):
        """
        设置采集事件的起始时间戳内序号

        :param int v: 序号
        :return: 无
        :rtype: None
        """
        self._begin_isn = v

    def _collect_new_events(self):
        """
        获取一批新的事件
        """
        now = time.time()
        collect_end_time = int(now) - self._persist_time - 1
        collect_begin_time = self._collect_begin_time

        # 没有可采集时间区间，跳过。采集区间至少为1s
        if collect_begin_time >= collect_end_time:
            return []

        # 超过最大可采集区间，重置采集开始时间。采集区间最大为max_collect_time
        if collect_end_time - collect_begin_time > self._max_collect_time:
            collect_begin_time = collect_end_time - self._max_collect_time - 1
            log.i("elastic_event collection_time exceed %d, begin time reset to %d"
                  % (self._max_collect_time, collect_begin_time))

        result = self._driver.query_all(collect_begin_time, collect_end_time)
        sorted_events = [SortableEvent(self._driver.get_ts(i), self._driver.get_id(i), i) for i in result]
        sorted_events.sort()
        # 当前已排序事件的时间戳内序号都是0，将该序号重置为排序后正确的时间戳内序号
        idx = 0
        last_ts = 0
        for se in sorted_events:
            if last_ts != se.ts:
                last_ts = se.ts
                idx = 0
            else:
                idx += 1
            se.ts_seq = idx

        # context记录的时间与本次的采集开始时间一致，则要从上次未处理的事件开始处理
        if self._begin_time >= collect_begin_time:
            sorted_events = [se for se in sorted_events
                             if (se.ts > self._begin_time or
                                 (se.ts == self._begin_time and se.ts_seq >= self._begin_isn))]
        self._collect_begin_time = collect_end_time
        return sorted_events

    def get(self):
        """
        返回一个已经获取到的事件。如果没有事件，则返回None

        :return: 从ES获取到的事件，事件参数以词典KV返回
        :rtype: dict
        """
        if len(self._cache) == 0:
            self._cache = self._collect_new_events()
            # 避免pop(0) 提升性能
            self._cache.reverse()
        if len(self._cache) == 0:
            return None

        return self._cache.pop()


class EsCallbackSensor(PullCallbackSensor):
    """
    使用从ElasticSearch拉取外部事件的方式实现的感知器。该感知器开启独立的线程从es中轮询消息，
    并在获取消息后放入暂存队列中，进行后续处理

    .. Note:: 由于EsCallbackSensor会记录时间戳到context指定的key中，所以应以单例方式使用EsCallbackSensor类
    """
    def __init__(self, index_tmpl, type, time_k,
                 filter_kv=None,
                 mustnot_kv=None,
                 should_kv=None, minimum_should_match=1,
                 header=None, query_size=100, es_persist_time=1, max_collect_time=600,
                 query_interval=3):
        """
        初始化方法

        :param str index_tmpl: 需要查询的事件所处的index的模版，可以根据当前日期生成实际的index
        :param str type:  需要查询的事件的所处的type
        :param str time_k:  需要查询的事件的时间属性名
        :param dict filter_kv: field名（k）必须为指定值（v），可传入多个表示必须同时满足
        :param dict mustnot_kv: field名（k）必须不为指定值（v），可传入多个表示必须同时满足
        :param dict should_kv: field名（k）可选为指定值（v），可传入多个表示尽量同时满足
        :param int minimum_should_match: should_kv中filed名值最少满足数量
        :param dict header: http请求头
        :param int query_size: 单次查询数量
        :param int es_persist_time: es的持久化时间，即now-es_persist_time之前的事件才能保证已经被持久化完成
        :param int max_collect_time: 最长事件采集时间区间。超过此区间的事件将被忽略
        :param int query_interval: 查询事件的事件间隔

        """

        es_driver = EsEventDriver1x(index_tmpl=index_tmpl, type=type, time_k=time_k,
                                    filter_kv=filter_kv, mustnot_kv=mustnot_kv,
                                    should_kv=should_kv, minimum_should_match=minimum_should_match,
                                    header=header, query_size=query_size)
        self._collector = EsEventCollector(event_driver=es_driver, timestamp=0, id_seq_no=0,
                                           es_persist_time=es_persist_time, max_collect_time=max_collect_time)
        super(EsCallbackSensor, self).__init__(query_interval)

    def get_event(self):
        """
        根据查询条件，从ES中获取相应事件

        :return: 从ES获取到的事件，事件参数以词典KV返回
        :rtype: dict
        """
        return self._collector.get()

    def active(self):
        """
        重载感知器生效函数，加入从context中获取上次查询的时间戳和序号的功能，确保可以从上次处理的时间点继续获取新事件。

        :return: 无返回
        :rtype: None
        :raises ThreadError: 创建线程失败
        """
        try:
            self._collector.begin_time = context.GuardianContext.get_context().sensor_es_event_timestamp
            self._collector.begin_isn = context.GuardianContext.get_context().sensor_es_event_ts_seq
        except:
            self._collector.begin_time = 0
            self._collector.begin_isn = 0
        super(EsCallbackSensor, self).active()

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

        # 不需要持久化context，该消息被DM处理后会持久化。在此之前均可以认为此消息未被感知并处理
        context.GuardianContext.get_context().sensor_es_event_timestamp = se.ts
        context.GuardianContext.get_context().sensor_es_event_ts_seq = se.ts_seq + 1
        return se.event
