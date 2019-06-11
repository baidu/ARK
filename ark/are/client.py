# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**client** 模块提供对第三方组件操作的统一抽象，规范化智能运维机器人与外部系统交互的方式

* ``BaseClient`` 提供第三方组件调用的一些通用功（如http请求重试、结果处理等）的封装
* ``ESClient`` 封装对Elasticsearch(后文简称ES)的操作，ES作为全文搜索数据库，支持多条件组合的高效查询，Guardian通过其提供的RESTful web接口进行操作记录的读写

.. Note:: 本模块只提供客户端基类的定义，并封装了Guardian运行时依赖组件的调用，跟业务相关的外部系统client应在 ``assemble`` 中实现
"""
import httplib
import json
import time

from ark.are import common
from ark.are import config
from ark.are import log
from ark.are import exception


class BaseClient(common.Singleton):
    """
    客户端基类，提供基本的http请求功能封装，统一对外http访问方式

    .. Note:: 为避免在接口异常时的频繁调用，重试时间间隔会以重试次数的平方方式增加

    """
    def http_request(self, host, port, method, url, header=None, data=None,
                     timeout=30,retry_times=2, response_code=None,
                     response_json=True):
        """
        http请求接口

        :param str host: 服务器地址
        :param int port: 服务器端口
        :param str method: http方法
        :param str url: url地址
        :param dict header: http消息头
        :param str data: http body数据
        :param int timeout: 请求超时时间
        :param int retry_times: 请求重试次数
        :param list response_code: 请求正确时的返回码
        :param bool response_json: True 返回json格式的数据，False 返回字符串
        :return: http 请求的数据
        :rtype: str
        :raises EFailedRequest: 请求失败
        """
        log.d("http request, host:{}, port:{}, method:{}, url:{}, header:"
                  "{}, data:{}, timeout:{}, retry_times:{}, response code:{}, "
                  "response_json:{}".format(host, port, method, url, header,
                                            data, timeout, retry_times,
                                            response_code, response_json))
        header = header or {}
        res_data = None
        for i in range(retry_times + 1):
            stop_tag = True if i == retry_times else False
            sleep_time = (i + 1) * (i + 1)
            try:
                conn = httplib.HTTPConnection(
                    host=host, port=port, timeout=timeout)
                conn.request(method=method, url=url, body=data, headers=header)
                resp = conn.getresponse()
                res_data = resp.read()
                log.d("http request ret:{}".format(res_data))
            except Exception as e:
                log.f("http request exe{}".format(res_data))
                if stop_tag:
                    raise exception.EFailedRequest(
                        "http request failed,error:{}".format(e))
                else:
                    time.sleep(sleep_time)
                    continue
            else:
                log.d("http request ok")
                if not response_code or not isinstance(response_code, list):
                    if 200 <= resp.status < 300:
                        break
                    elif stop_tag:
                        log.e("request failed,code:{},msg:{}".format(
                                resp.status, resp.msg))
                        raise exception.EFailedRequest(
                            "request failed,code:{},msg:{}".format(
                                resp.status, resp.msg))
                    else:
                        time.sleep((i + 1) * (i + 1))
                        continue
                else:
                    if resp.status in response_code:
                        break
                    elif stop_tag:
                        log.e("request failed,error,code:{},data:{}".format(
                                resp.status, data))
                        raise exception.EFailedRequest(
                            "request failed,error,code:{},data:{}".format(
                                resp.status, data))
                    else:
                        time.sleep(sleep_time)
                        continue
        log.d("http response data:{}".format(res_data))
        if response_json:
            return json.loads(res_data)
        else:
            return res_data


class ESClient(BaseClient):
    """
    Elasticsearch 客户端类，封装对elasticsearch操作，包括对索引(index)的增删改查
    用于智能运维机器人处理消息的状态存储，感知、决策和执行的整个流程
    """
    ARK_ES_HOST = "ARK_ES_HOST"
    ARK_ES_PORT = "ARK_ES_PORT"

    def __init__(self, index, type):
        """
        初始化方法

        :param str index: 索引参数
        :param str type: 类型参数
        """
        self.__host = config.GuardianConfig.get(self.ARK_ES_HOST)
        self.__port = int(config.GuardianConfig.get(self.ARK_ES_PORT))
        self.__index = index
        self.__type = type

    def post_data(self, data):
        """
        创建elasticsearch索引(index)，并存储数据

        :param str data: 请求数据
        :return: 请求结果
        :rtype: str
        :raises: None
        """
        url = "/{}/{}".format(self.__index, self.__type)
        method = "POST"
        ret = self.http_request(self.__host, int(self.__port),
                                method, url, data=data)
        return ret

    def put_data(self, uid, data):
        """
        写入elasticsearch文档(doc)

        :param str uid: 待查询对象的uid
        :param str data: 请求数据
        :return: 请求结果
        :rtype: str
        :raises EFailedRequest: 请求失败
        """
        url = "/{}/{}/{}".format(self.__index, self.__type, uid)
        method = "PUT"
        header = {"Content-Type": "application/json"}
        ret = self.http_request(self.__host, self.__port,
                                method, url, data=data, header=header)
        return ret

    def get_data_with_uid(self, uid):
        """
        根据uid查询doc

        :param str uid: 待查询对象的uid
        :return: 返回请求结果
        :rtype: str
        :raises EFailedRequest: 请求失败
        """
        url = "/{}/{}/{}".format(self.__index, self.__type, uid)
        method = "GET"
        ret = self.http_request(self.__host, self.__port, method, url)
        return ret

    def get_data_with_condition(self, condition):
        """
        根据复合条件查询elasticsearch

        :param dict condition: 查询条件
        :return: 返回请求结果
        :rtype: str
        :raises EFailedRequest: 请求失败
        """
        url = "/{}/{}/_query".format(self.__index, self.__type)
        method = "GET"
        ret = self.http_request(self.__host, self.__port,
                                method, url, data=json.dumps(condition))
        return ret
