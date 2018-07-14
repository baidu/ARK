# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**state_service** 状态展示模块，用于对支持中的状态进行展示，返回json数据

.. Note:: 当前只提供了根据operation_id进行操作展示的api接口，暂不支持其他复杂条件查询
"""

import SocketServer
import multiprocessing
import traceback
import urlparse
from SimpleHTTPServer import SimpleHTTPRequestHandler

from are import config
from are import log
from are import client
from are import exception


class RequestHandler(SimpleHTTPRequestHandler):
    """
    Http请求处理类
    """
    def do_GET(self):
        """
        处理http get方法请求，发送到Elasticsearch查询结果，并返回
        get方法中必须带有uid，否则无法查询

        :return: None
        """

        path_dict = self._path_to_dict(self.path)
        if 'uid' in path_dict:
            response = self._do_request(path_dict['uid'])
            self.wfile.write(response)
        else:
            notice = "please enter uid parameter!"
            self.wfile.write(notice)

    def _path_to_dict(self, path):
        """
        把http get方法字符串转字典

        :param str path: path字符串
        :return: get方法字符串转字典
        :rtype: dict
        """
        query = urlparse.urlparse(path).query

        return dict([(k, v[0]) for k, v in urlparse.parse_qs(query).items()])

    def _do_request(self, uid):
        """
        发送http请求
        发生异常会记录日志

        :raises: exception.EFailedRequest 请求失败
        :param str uid: 
        :return: None 或者 请求数据
        :rtype: None 或者str
        """

        response = None
        try: 
            condition = {"uid": uid}
            esc = client.ESClient("ark", "operation")
            response = esc.get_data_with_condition(condition)
        except exception.EFailedRequest as e:
            log.error(str(e.reason))
        finally:
            return response


class ArkServer(object):
    """
    状态机展示http服务器
    """

    def __init__(self):
        """
        初始化方法
        """
        self._port = config.GuardianConfig.get("ARK_SERVER_PORT")

    def start(self, daemon=True):
        """
        启动StateMachineServer

        :param bool daemon: 是否以daemon运行
        :return: None
        """
        process = multiprocessing.Process(target=self.__run)
        process.daemon = daemon
        process.start()

    def __run(self):
        """
        http server进程
        发生异常会记录日志
        :raises: Exception: 运行中可能抛出的异常
        """
        try:
            handler = RequestHandler
            SocketServer.TCPServer.allow_reuse_address = True
            _server = SocketServer.TCPServer(('', self._port), handler)
            _server.serve_forever()
        except Exception:
            log.error('Generic Exception' + traceback.format_exc())

