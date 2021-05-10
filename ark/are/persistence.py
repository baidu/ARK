# -*- coding: UTF-8 -*-
################################################################################
#
# Copyright (c) 2018 Baidu.com, Inc. All Rights Reserved
#
################################################################################
"""
**persistence** 模块用于配置信息、状态信息的持久化存储。
"""

import os
import json
import time
import copy
import string
import ark.are.common as common
import ark.are.exception as exception
import ark.are.log as log
import ark.are.config as config


class PersistenceEvent(object):
    """
    提供持久化事件通知支持

    """
    class PersistState(object):
        SUSPENDED = "SUSPENDED"
        CONNECTED = "CONNECTED"
        LOST = "LOST"

    class EventType(object):
        CREATED = 'CREATED'
        DELETED = 'DELETED'
        CHANGED = 'CHANGED'
        CHILD = 'CHILD'
        NONE = 'NONE'

    def __init__(self, type=None, state=None, path=None):
        self.type = type or PersistenceEvent.EventType.NONE
        self.state = state or PersistenceEvent.PersistState.LOST
        self.path = path or ""


class BasePersistence(common.Singleton):
    """
    持久化基类，提供标准化的持久化接口，以及单例等基本功能。

    .. Note:: 持久化功能以树型层级结构的节点存储数据，节点可以以路径形式索引。

    """

    PERSIST_MODE_NAME = "PERSIST_MODE"
    PERSIST_INTERVAL_NAME = "PERSIST_INTERVAL"
    PERSIST_TIMEOUT_NAME = "PERSIST_TIMEOUT"
    PERSIST_PARAMETERS_NAME = "PERSIST_PARAMETERS"

    def get_data(self, path):
        """
        获得指定路径path的节点数据

        :param str path: 数据存储路径
        :return: 节点数据
        :rtype: str
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        raise exception.ENotImplement("function is not implement")

    def save_data(self, path, data):
        """
        存储数据data到特定的path路径节点

        :param str path: 数据存储路径
        :param str data: 待存储的数据
        :return: 无返回
        :rtype: None
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        raise exception.ENotImplement("function is not implement")

    def delete_node(self, path, force=False):
        """
        删除node节点
        :param str path: 数据存储路径
        :param bool force: 是否强行删除而不判断节点有效性
        :return: 无返回
        :rtype: None
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        raise exception.ENotImplement("function is not implement")

    def get_children(self, path, watcher=None, include_data=False):
        """
        获取所有子节点
        :param str path: 待获取子节点的路径
        :param watcher: 状态监听函数。函数形参为(event)，event包括三个成员属性：path（发生状态变化的路径）、state（server链接状态）、type（事件类型，包括CREATED|DELETED|CHANGED|CHILD|NONE）
        :param bool include_data: 是否同时返回数据
        :return: 子节点名字列表
        :rtype: str
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        raise exception.ENotImplement("function is not implement")

    def create_node(self, path, value="",
                    ephemeral=False, sequence=False, makepath=False):
        """
        根据节点各属性创建节点

        :param str path: 待创建的节点路径
        :param str value: 待存数据
        :param bool ephemeral: 是否是临时节点
        :param bool sequence: 是否是自动分配节点序号
        :param bool makepath: 是否创建父节点
        :return: 新创建的节点路径
        :rtype: str
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        raise exception.ENotImplement("function is not implement")

    def exists(self, path):
        """
        查询制定path路径的节点是否存在

        :param str path: 待检查的节点路径
        :return: True表示节点存在，False表示节点不存在
        :rtype: bool
        :raises: exception.EPIOError IO异常
        """
        raise exception.ENotImplement("function is not implement")

    def add_listener(self, watcher):
        """
        监听会话状态

        :param watcher: 状态监听函数。函数形参为(state)，可能的取值包括"SUSPENDED"、"CONNECTED"、"LOST"
        :return: 无返回
        :rtype: None
        :raises: exception.EPIOError IO异常
        """
        raise exception.ENotImplement("function is not implement")

    def disconnect(self):
        """
        主动断开持久化请求

        :return: 无返回
        :rtype: None
        """
        raise exception.ENotImplement("function is not implement")


class PlainPersistence(BasePersistence):
    """
    通过通用存储（文件、redis等）实现的持久化类基类。主要用来完成基础的轮询刷新时间、比对节点状态触发事件的功能。
    实体节点的数据存放在.data子节点中。临时节点序号存放在.sequence中。临时节点会被定期touch以保持其最新，超时的会被自动删除。
    """
    _init = False

    def __init__(self):
        """
        初始化方法
        """
        import threading
        if self._init:
            return

        self._initf()
        self._lock = threading.Lock()
        self._inspect_results = {}
        self._ob_paths = {}  # 需要观察路径下节点变化的路径列表
        self._touch_paths = {}  # 针对临时节点，需要不断touch的路径列表
        self._interval = string.atof(config.GuardianConfig.get(self.PERSIST_INTERVAL_NAME, "0.4"))
        self._timeout = string.atof(config.GuardianConfig.get(self.PERSIST_TIMEOUT_NAME, "3"))
        self._session_thread = threading.Thread(target=self._thread_run)
        self._init = True
        self._session_thread.setDaemon(True)
        self._session_thread.start()

    def __del__(self):
        """
        析构方法，完成线程回收
        """
        self.disconnect()

    def delete_node(self, path, force=False):
        """
        删除node节点
        :param str path: 数据存储路径
        :param bool force: 是否强行删除而不判断节点有效性
        :return: 无返回
        :rtype: None
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        self._del_record_when_delnode(path)
        self._del_node(path, force)
        log.d("delete node success, path:{path}".format(path=path))

    def get_children(self, path, watcher=None, include_data=False):
        """
        获取所有子节点
        :param str path: 待获取子节点的路径
        :param watcher: 状态监听函数。函数形参为(event)，event是一个对象，包括三个成员属性：path（发生状态变化的路径）、state（server链接状态）、type（事件类型，包括CREATED|DELETED|CHANGED|CHILD|NONE）
        :param bool include_data: 是否同时返回数据
        :return: 子节点名字列表
        :rtype: str
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        chd = self._valid_chd(path, include_data)
        if not include_data:
            node_list = chd.keys()
        else:
            node_list = chd.items()
        if watcher:
            if not callable(watcher):
                raise exception.ETypeMismatch("watcher must callable")
            self._new_ob(path, watcher)
        return node_list

    def disconnect(self):
        """
        主动断开持久化请求

        :return: 无返回
        :rtype: None
        """
        self._init = False
        with self._lock:
            self._ob_paths = {}
            self._touch_paths = {}
        log.d("disconnect success")

    def _initf(self):
        """
        用于子类初始化
        :return:
        """
        raise exception.ENotImplement("function is not implement")

    def _del_node(self, path, force):
        """
        用于子类删除节点
        :return:
        """
        raise exception.ENotImplement("function is not implement")

    def _valid_chd(self, path, include_data=False):
        """
        用于获取有效的子节点
        :return:
        """
        raise exception.ENotImplement("function is not implement")

    def _valid_node(self, path):
        """
        用于获取有效的节点数据
        :return:
        """
        raise exception.ENotImplement("function is not implement")

    def _refresh(self, obpath):
        """
        刷新节点属性
        """
        result = {"exist": False, "md5": None, "children": {}}
        return result

    def _touch(self, tp, now):
        """
        更新临时节点的时间，并清理已经过期的临时节点
        """
        raise exception.ENotImplement("function is not implement")

    def _inspect(self, obpath, watcher):
        """
        检查节点是否有变化，如果有则触发wather函数
        """
        result = self._refresh(obpath)
        if obpath not in self._inspect_results:
            self._inspect_results[obpath] = result
            last_result = {"exist": False, "md5": None, "children": {}}
        else:
            last_result = self._inspect_results[obpath]

        # 判断目录状态是否有变化。
        if last_result["exist"] != result["exist"]:
            if result["exist"]:
                # 事实上，以现在已经提供出的接口参数，并不会产生CREATED事件。如果路径不存在，get_children会直接抛出异常
                watcher(PersistenceEvent(type=PersistenceEvent.EventType.CREATED,
                                         state=PersistenceEvent.PersistState.CONNECTED,
                                         path=obpath))
            else:
                watcher(PersistenceEvent(type=PersistenceEvent.EventType.DELETED,
                                         state=PersistenceEvent.PersistState.CONNECTED,
                                         path=obpath))
            self._ignore(obpath)
            return

        # 判断data是否变化
        if last_result["md5"] != result["md5"]:
            watcher(PersistenceEvent(type=PersistenceEvent.EventType.CHANGED,
                                     state=PersistenceEvent.PersistState.CONNECTED,
                                     path=obpath))
            self._ignore(obpath)
            return
        last_result.keys()
        # 判断子节点是否有变化
        if len(last_result["children"]) != len(result["children"]) or\
           len(set(last_result["children"].keys()) - set(result["children"].keys())) != 0:
            watcher(PersistenceEvent(type=PersistenceEvent.EventType.CHILD,
                                     state=PersistenceEvent.PersistState.CONNECTED,
                                     path=obpath))
            self._ignore(obpath)
            return
        return

    def _ignore(self, obpath):
        """
        清理需要监视的路径
        """
        self._del_ob(obpath)

    def _thread_run(self):
        """
        获取路径变化的事件，并保持临时节点的时间为最新
        """
        while self._init:
            with self._lock:
                ob_paths = copy.copy(self._ob_paths)
                touch_paths = copy.copy(self._touch_paths)
            now = long(time.time())
            for tp in touch_paths:
                self._touch(tp, now)
            for obp, watcher in ob_paths.iteritems():
                self._inspect(obp, watcher)

            time.sleep(self._interval)

    def _new_touch(self, path):
        """
        增加一个touch的路径
        """
        with self._lock:
            if not self._init:
                return
            self._touch_paths[path] = ""

    def _new_ob(self, path, watcher):
        """
        增加一个检测的路径
        """
        with self._lock:
            if not self._init:
                return
            self._ob_paths[path] = watcher

    def _del_touch(self, path):
        """
        增加一个touch的路径
        """
        with self._lock:
            self._touch_paths.pop(path)

    def _del_ob(self, path):
        """
        增加一个检测的路径
        """
        with self._lock:
            self._ob_paths.pop(path)
            self._inspect_results.pop(path)
        return

    def _del_record_when_delnode(self, path):
        """
        当路径不存在时清理与该路径相关的记录
        """
        with self._lock:
            for k in list(self._touch_paths):
                if k.startswith(path):
                    self._touch_paths.pop(k)
            for k in list(self._ob_paths):
                if k.startswith(path):
                    self._ob_paths.pop(k)
                    self._inspect_results.pop(path)


PersistenceDriver = BasePersistence


class ZkPersistence(BasePersistence):
    """
    Zookeeper持久化实现，封装对zookeeper的操作，包括对zookeeper节点的增删改查
    用于智能运维机器人运行时数据的持久化和异常恢复
    """
    _init = False

    def __init__(self):
        """
        初始化方法
        """
        if ZkPersistence._init:
            return
        self._client = ZkPersistence._new_session()
        self._client.start()
        ZkPersistence._init = True

    @classmethod
    def _run_catch(cls, func):
        """
        执行func并捕获异常，将kazoo异常转换为对应的异常对象
        """
        import kazoo
        try:
            return func()
        except kazoo.exceptions.NoNodeError:
            raise exception.EPNoNodeError()
        except kazoo.exceptions.ZookeeperError:
            log.f("zk fail")
            raise exception.EPServerError()
        except Exception as e:
            log.r(exception.EPIOError(), "Requesst I/O Error")

    @classmethod
    def _new_session(cls):
        """
        创建kazoo.client.KazooClient实例

        :return: kazoo.client.KazooClient实例
        :rtype: kazoo.client.KazooClient
        :raises: exception.EPConnectTimeout 连接超时异常
        """
        # 仅在必要的情况下才引入kazoo
        from kazoo import client
        hosts = config.GuardianConfig.get(config.STATE_SERVICE_HOSTS_NAME)
        params = json.loads(config.GuardianConfig.get(cls.PERSIST_PARAMETERS_NAME, '{}'))
        return ZkPersistence._run_catch(lambda: (client.KazooClient(hosts=hosts, **params)))

    def get_data(self, path):
        """
        获得指定路径path的节点数据

        :param str path: 数据存储路径
        :return: 节点数据
        :rtype: str
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError 连接异常
        """
        return ZkPersistence._run_catch(lambda: (self._client.get(path)[0]))

    def save_data(self, path, data):
        """
        存储数据data到特定的path路径节点

        :param str path: 数据存储路径
        :param str data: 待存储的数据
        :return: None
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError 连接异常
        """
        ZkPersistence._run_catch(lambda: (self._client.set(path, data)))
        log.d("save data success, path:{path}, data:{data}".format(path=path, data=data))

    def delete_node(self, path, force=False):
        """
        删除node节点
        :param str path: 数据存储路径
        :param bool force: 是否强行删除而不判断节点有效性
        :return None
        """
        if force:
            self._client.delete(path=path, recursive=True)
        else:
            self._run_catch(lambda: (self._client.delete(path=path, recursive=True)))
        log.d("delete node success, path:{path}".format(path=path))

    def get_children(self, path, watcher=None, include_data=False):
        """
        获取所有子节点
        :param str path: 待获取子节点的路径
        :param watcher: 状态监听函数。函数形参为(event)，event是一个对象，包括三个成员属性：path（发生状态变化的路径）、state（server链接状态）、type（事件类型，包括CREATED|DELETED|CHANGED|CHILD|NONE）
        :param bool include_data: 是否同时返回数据
        :return: 子节点名字列表
        :rtype: str
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        # 装饰watcher，将state、type转换为ARK内定义
        def dec(zkevent):
            if zkevent.state == "CONNECTED" or zkevent.state == "CONNECTED_RO":
                state = PersistenceEvent.PersistState.CONNECTED
            elif zkevent.state == "CONNECTING":
                state = PersistenceEvent.PersistState.SUSPENDED
            else:
                state = PersistenceEvent.PersistState.LOST
            event = PersistenceEvent(zkevent.type, state, zkevent.path)
            return watcher(event)

        return ZkPersistence._run_catch(
                        lambda: (self._client.get_children(path, watcher and dec, include_data)))

    def create_node(self, path, value="", ephemeral=False, sequence=False, makepath=False):
        """
        根据节点各属性创建节点

        :param str path: 节点路径
        :param str value: 待存数据
        :param bool ephemeral: 是否是临时节点
        :param bool sequence: 是否是顺序节点
        :param bool makepath: 是否创建父节点
        :return: 新创建的节点路径
        :rtype: str
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        node_path = ZkPersistence._run_catch(lambda: (self._client.create(path, value, None,
                                                              ephemeral, sequence,
                                                              makepath)))

        log.d("create node success, path:{path}, value:{value}, ephemeral:"
              "{ephemeral}, sequence:{sequence}, makepath:{makepath}".format(
                                                        path=node_path, value=value,
                                                        ephemeral=ephemeral, sequence=sequence,
                                                        makepath=makepath))
        return node_path

    def exists(self, path):
        """
        查询制定path路径的节点是否存在

        :param str path: 节点路径
        :return: True或False
        :rtype: bool
        :raises: exception.EPIOError IO异常
        """
        return ZkPersistence._run_catch(lambda: (self._client.exists(path)))

    def add_listener(self, watcher):
        """
        监听会话状态

        :param watcher: 状态监听函数。函数形参为(state)，可能的取值包括"SUSPENDED"、"CONNECTED"、"LOST"
        :return: 无返回
        :rtype: None
        :raises: exception.EPIOError IO异常
        """
        # 装饰watcher，将state、type转换为ARK内定义
        def dec(zkstate):
            state = zkstate
            return watcher(state)

        ZkPersistence._run_catch(lambda: (self._client.add_listener(watcher and dec)))
        log.d("add listener success")

    def disconnect(self):
        """
        主动断开持久化请求

        :return: 无返回
        :rtype: None
        """

        ZkPersistence._run_catch(lambda: (self._client.close()))
        log.d("disconnect success")


class FilePersistence(PlainPersistence):
    """
    通过文件系统实现的持久化类。
    实体节点用文件系统中的目录表示，实体节点的数据存放在目录下的.data文件中。
    临时节点用文件系统中的文件来表示，临时节点的数据存放在对应文件中，临时节点（文件）会被定期touch以保持其最新，超时的节点会在列出或获取数据时校验并删除。

    """
    _file_mode = "0755"

    def _initf(self):
        """
        用于子类初始化
        :return:
        """
        self._base = config.GuardianConfig.get(config.STATE_SERVICE_HOSTS_NAME)
        self._mode = string.atoi(config.GuardianConfig.get(self.PERSIST_MODE_NAME, self._file_mode), 8)
        if not os.path.exists(self._base):
            os.makedirs(self._base, self._mode)

    def _del_node(self, path, force):
        """
        删除node节点
        :param str path: 数据存储路径
        :return None
        """
        ospath = self._base + path

        def _delall():
            """
            :return:
            """
            if os.path.isdir(ospath):
                import shutil
                shutil.rmtree(ospath, True)
            else:
                os.remove(ospath)

        if force:
            _delall()
        else:
            self._run_catch(_delall, path)

    def _touch(self, tp, now):
        """
        更新临时节点的时间，并清理已经过期的临时节点
        """
        ospath = self._base + tp
        if not os.path.exists(ospath) or not os.path.isfile(ospath):
            self._del_record_when_delnode(tp)
            return
        # 更新临时节点时间
        os.utime(ospath, None)

    def _valid_chd(self, path, include_data=False):
        """
        获取所有子节点，并校验子节点是否有效。默认不获取子节点数据
        """
        def _valid():
            valid_time = time.time() - self._timeout
            ospath = self._base + path
            result = {}
            for node_name in os.listdir(ospath):
                if node_name == ".data" or node_name == ".sequence":
                    continue
                file_name = "/".join([ospath, node_name])
                node_name = "/".join([path, node_name])
                if os.path.isfile(file_name):
                    mtime = os.stat(file_name).st_mtime
                    if mtime < valid_time:
                        self.delete_node(node_name, True)
                        continue

                if not include_data:
                    result[node_name] = ""
                else:
                    data = self.get_data(node_name)
                    result[node_name] = data
            return result
        return self._run_catch(_valid, path, True)

    def _refresh(self, path):
        """
        刷新节点属性
        """
        obpath = self._base + path
        result = {}
        # 获取该路径所有状态
        if self.exists(path):
            result["exist"] = True
            # 获取该路径数据
            import hashlib
            data = self.get_data(path)
            md5 = hashlib.md5()
            md5.update(data)
            result["md5"] = md5.hexdigest()

            # 获取所有子节点，去除两个内置文件
            result["children"] = set([name for name in os.listdir(obpath)])
            if ".data" in result["children"]:
                result["children"].remove(".data")
            if ".sequence" in result["children"]:
                result["children"].remove(".sequence")
        else:
            result["exist"] = False
            result["md5"] = None
            result["children"] = {}
        return result

    def _run_catch(self, func, path, path_is_dir=False):
        """
        执行func并捕获异常，将文件系统异常转换为对应的异常对象
        """
        ospath = self._base + path
        try:
            if os.path.exists(ospath):
                if path_is_dir:
                    if os.path.isdir(ospath):
                        return func()
                else:
                    if os.path.isfile(ospath):
                        # 判断文件是否过期，过期直接报错
                        mtime = os.stat(ospath).st_mtime
                        if mtime < time.time() - self._timeout:
                            self.delete_node(path, True)
                        else:
                            return func()
                    else:
                        return func()
            raise exception.EPNoNodeError()
        except exception.EPNoNodeError as e:
            log.r(e, "Node not exist:{}".format(path))
        except Exception as e:
            log.r(exception.EPIOError(), "Request I/O Error")

    def get_data(self, path):
        """
        获得指定路径path的节点数据

        :param str path: 数据存储路径
        :return: 节点数据
        :rtype: str
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        ospath = self._base + path

        def _readdata():
            if os.path.isdir(ospath):
                file_path = "/".join((ospath, ".data"))
            else:
                file_path = ospath
            if not os.path.exists(file_path):
                return ""
            with open(file_path, 'r') as f:
                return f.read()

        return self._run_catch(_readdata, path)

    def save_data(self, path, data):
        """
        存储数据data到特定的path路径节点

        :param str path: 数据存储路径
        :param str data: 待存储的数据
        :return: 无返回
        :rtype: None
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        ospath = self._base + path

        def _writedata():
            if os.path.isdir(ospath):
                file_path = "/".join((ospath, ".data"))
            else:
                file_path = ospath
            with open(file_path, 'w') as f:
                return f.write(data)

        self._run_catch(_writedata, path)
        log.d("save data success, path:{path}".format(path=path))

    @classmethod
    def _seq_file_name(cls, ospath, value):
        """
        生成临时节点的序列号并写入数据。最大序列号会被记录在.sequence文件中。以避免前后多次运行使用同一序列号。序列号生成的过程通过文件锁保证事务
        """
        import re
        import fcntl
        dirname = os.path.dirname(ospath)
        basename = os.path.basename(ospath)
        max_sn = -1

        with open("/".join((dirname, ".sequence")), 'a+') as f:
            f.seek(0)
            try:
                max_sn = string.atoi("0" + f.read(11))
            except ValueError:
                pass
            # 加文件锁，保证序列id自增的唯一性
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            try:
                # 检测目录中的文件，如果文件名（或目录名）前缀匹配，则检查后缀是否全为数字序号。记录下最大的数字序号
                for name in os.listdir(dirname):
                    if name == ".data" or name == ".sequence":
                        continue
                    if len(name) < len(basename) or name[0:len(basename)] != basename:
                        continue
                    sn = name[len(basename):]
                    if not re.match("[0-9]+$", sn):
                        continue
                    sn = string.atoi(sn)
                    if sn > max_sn:
                        max_sn = sn
                # 生成新的临时节点名（比最大的数字序号还大1）
                f.seek(0)
                f.truncate()
                f.write("%d" % (max_sn + 1))
                file_path = ospath + ("%09d" % (max_sn + 1))
                with open(file_path, 'w') as node:
                    node.write(value)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

        return ospath + ("%09d" % (max_sn + 1))

    def create_node(self, path, value="", ephemeral=False, sequence=False, makepath=False):
        """
        根据节点各属性创建节点

        :param str path: 节点路径
        :param str value: 待存数据
        :param bool ephemeral: 是否是临时节点
        :param bool sequence: 是否是顺序节点
        :param bool makepath: 是否创建父节点
        :return: 新创建的节点路径
        :rtype: str
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """

        ospath = self._base + path
        try:
            if ephemeral:
                # 临时节点，直接用文件来表示
                dirname = os.path.dirname(ospath)
                if not os.path.exists(dirname):
                    if makepath:
                        os.makedirs(dirname, self._mode)
                    else:
                        raise exception.EPNoNodeError
                file_path = ospath
                if sequence:
                    file_path = self._seq_file_name(ospath, value)
                else:
                    with open(file_path, 'w') as f:
                        f.write(value)
                self._new_touch(file_path[len(self._base):])
            else:
                # 实体节点，用目录来表示，数据存放在目录下的.data文件中
                if not os.path.exists(ospath):
                    if not os.path.exists(os.path.dirname(ospath)) and not makepath:
                        raise exception.EPNoNodeError
                    os.makedirs(ospath, self._mode)

                file_path = "/".join((ospath, ".data"))

                with open(file_path, 'w') as f:
                    f.write(value)
            node_path = file_path[len(self._base):]
        except exception.EPNoNodeError as e:
            log.r(e, "Node not exist:{}".format(os.path.dirname(path)))
        except Exception as e:
            log.r(exception.EPIOError(), "Request I/O Error")
        log.d("create node success, path:{path}, value:{value}, ephemeral:"
              "{ephemeral}, sequence:{sequence}, makepath:{makepath}".format(
                                                        path=node_path, value=value,
                                                        ephemeral=ephemeral, sequence=sequence,
                                                        makepath=makepath))
        return node_path

    def exists(self, path):
        """
        查询制定path路径的节点是否存在

        :param str path: 节点路径
        :return: True或False
        :rtype: bool
        """
        ospath = self._base + path
        return os.path.exists(ospath)

    def add_listener(self, watcher):
        """
        监听会话状态

        :param watcher: 状态监听函数。函数形参为(state)，可能的取值包括"SUSPENDED"、"CONNECTED"、"LOST"
        :return: 无返回
        :rtype: None
        """
        log.i("nothing to do in FilePersistence.add_listener()")


class RedisPersistence(PlainPersistence):
    """
    通过Redis实现的持久化类：

    * 实体节点的 *节点路径* 以key形式存放。
    * 临时节点与实体节点类似，但是临时节点会被定期更新有效期，超期会被redis自动删除。仅列出子节点时额外校验是否超时。

    为了在部分不支持keys命令的Redis正常运行，节点管理采用了FilePersistence类似的方式：

    1. 所有节点均为Hash（类似FilePersistence的目录）
    2. 节点值存放在.data元素中（类似FilePersistence的.data隐藏文件）
    3. Hash中其他的key（非.开头的）为当前节点的子节点名，value为空，子节点的值存放在子节点Hash的.data中
    4. 生成临时节点序号的最大序号记录在.sequence元素中（类似FilePersistence的.sequence隐藏文件）
    5. 创建节点需要同时修改父节点的Hash并创建新的key，所以通过lua脚本保证操作原子化
    """

    def _initf(self):
        """
        初始化方法
        """
        self._handle = self._new_session()
        self._load_scripts()

    @classmethod
    def _split_node_name(cls, node_path):
        """
        将节点路径分解为父路径和节点名

        :return: 节点的父路径和节点名
        :rtype: (path, name)
        """
        result = node_path.rsplit("/", 1)
        if result[0] == "":
            result[0] = "/"
        return result[0], result[1]

    @classmethod
    def _new_session(cls):
        """
        创建redis.client.Redis实例

        :return: redis.client.Redis实例
        :rtype: redis.client.Redis
        :raises: exception.EPConnectTimeout 连接超时异常
        """
        # 仅在必要的情况下才引入redis
        import redis

        redis_url = config.GuardianConfig.get(config.STATE_SERVICE_HOSTS_NAME)
        params = json.loads(config.GuardianConfig.get(cls.PERSIST_PARAMETERS_NAME, "{}"))
        return RedisPersistence._run_catch(lambda: (redis.StrictRedis.from_url(redis_url, **params)))

    def _load_scripts(self):
        """
        加载lua脚本。包括创建节点和删除节点两个
        """
        # 传入参数，KEYS[1]=父节点全路径，KEYS[2]=子节点名，ARGV[1]=节点数据，ARGV[2]=是否序号节点（0 or 1），ARGV[3]=到期时间（0表示永久）。返回-1表示错误
        new_node_script = """
        local node_name = KEYS[2]
        local ret = redis.call('ttl', KEYS[1])
        if  ret ~= -1 then
            return -1       
        end
        if tostring(ARGV[2]) ~= "0" then
            local seq = redis.call('hincrby', KEYS[1], '.sequence', '1')
            node_name = string.format('%s%d', node_name, seq)
        end
        local expire_time = tonumber(ARGV[3])
        local node_path = string.format('%s/%s', KEYS[1], node_name)
        local ret = redis.call('hsetnx', KEYS[1], node_name, tostring(expire_time))
        if ret ~= 1
        then
            return -2
        end
        ret = redis.call('hsetnx', node_path, '.data', ARGV[1])
        if ret ~= 1
        then
            redis.call('hdel', KEYS[1], node_name)
            return -3
        end
        if expire_time ~= 0 then
            redis.call('expireat', node_path, expire_time)
        end
        return node_path
        """

        # 传入参数，KEYS[1]=父节点全路径，KEYS[2]=子节点名。返回-1表示错误
        delete_node_script = """
        local node_path = string.format('%s/%s', KEYS[1], KEYS[2])
        local ret = redis.call('hlen', node_path)
        if ret == 0 then
                redis.call('hdel', KEYS[1], KEYS[2])
                redis.call('del', node_path)
                return 1
        elseif ret < 3
        then
            local result = redis.call('hgetall', node_path)
            local fst_c = string.byte(result[1], 1)
            if #result < 3 then
                result[3] = '.'
            end
            local snd_c = string.byte(result[3], 1)
            if ( fst_c == 46 and snd_c == 46 )
            then
                redis.call('hdel', KEYS[1], KEYS[2])
                redis.call('del', node_path)
                return 1
            end
        end
        return -1
        """

        # 传入参数，KEYS[1]=父节点全路径，KEYS[2]=子节点名，ARGV[3]=到期时间。返回-1表示错误
        refresh_node_script = """
        local node_path = string.format('%s/%s', KEYS[1], KEYS[2])
        if redis.call('hexist', KEYS[1], KEYS[2]) == 0 then
            return -1
        end
        if redis.call('ttl', node_path) <0 then
            return -1
        end
        local ret = redis.call('expireat', node_path, ARGV[1])
        if ret == 1
        then
            redis.call('hset', KEYS[1], KEYS[2], ARGV[1])
            return 1
        else
            return -1
        end
        """
        self._new_lua_sha = self._handle.script_load(new_node_script)
        self._delete_lua_sha = self._handle.script_load(delete_node_script)
        self._refresh_lua_sha = self._handle.script_load(refresh_node_script)

    def _valid_chd(self, path, include_data=False):
        """
        获取所有子节点，并校验子节点是否有效。默认不获取子节点数据
        """
        def _valid():
            handle = self._handle
            valid_time = time.time() - self._timeout

            # 考虑到性能，不做path存在性检查
            chd = handle.hgetall(path)
            result = {}
            for k, v in chd.iteritems():
                # 忽略隐藏节点
                if k[0:1] == ".":
                    continue
                chd_path = path + "/" + k
                # 删除超时节点

                try:
                    if v == "":
                        v = "0"
                    tm = float(v)
                except ValueError:
                    continue
                if v != "0" and tm < valid_time:
                    self.delete_node(chd_path, True)
                    continue
                value = ""
                # 获取节点的数据
                if include_data:
                    value = self.get_data(chd_path)
                result[k] = value
            return result
        return self._run_catch(_valid, path, True)

    def _refresh(self, obpath):
        """
        刷新节点属性
        """
        result = {}
        # 获取该路径所有状态
        if self.exists(obpath):
            result["exist"] = True
            # 获取该路径数据
            import hashlib
            nodes = self._run_catch(lambda: (self._handle.hgetall(obpath)))
            if ".data" in nodes:
                data = nodes[".data"]
            else:
                data = ""
                nodes[".data"] = ""
            md5 = hashlib.md5()
            md5.update(data)
            result["md5"] = md5.hexdigest()

            # 获取所有子节点，去除两个内置文件
            result["children"] = set(nodes.keys())
            if ".data" in result["children"]:
                result["children"].remove(".data")
            if ".sequence" in result["children"]:
                result["children"].remove(".sequence")
        else:
            result["exist"] = False
            result["md5"] = None
            result["children"] = {}
        return result

    def _touch(self, tp, now):
        """
        更新临时节点的时间，并清理已经过期的临时节点
        """
        path, node_name = self._split_node_name(tp)

        # 更新临时节点时间
        try:
            ret = self._handle.evalsha(self._refresh_lua_sha, 2, path, node_name, now)
            if ret == -1:
                self._del_record_when_delnode(tp)
        except Exception as e:
            self._del_record_when_delnode(tp)

    @classmethod
    def _run_catch(cls, func, path="", path_is_dir=False):
        """
        执行func并捕获异常，将kazoo异常转换为对应的异常对象
        """
        import redis
        # noinspection PyBroadException
        try:
            return func()
        except redis.exceptions.ConnectionError:
            raise exception.EPConnectTimeout()
        except exception.EPNoNodeError as e:
            raise e
        except Exception as e:
            log.r(exception.EPIOError(), "Request I/O Error")

    def get_data(self, path):
        """
        获得指定路径path的节点数据

        :param str path: 数据存储路径
        :return: 节点数据
        :rtype: str
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        handle = self._handle

        def _readdata():
            value = handle.hget(path, ".data")
            if value == "" or value:
                return value
            else:
                raise exception.EPNoNodeError

        return self._run_catch(_readdata)

    def save_data(self, path, data):
        """
        存储数据data到特定的path路径节点

        :param str path: 数据存储路径
        :param str data: 待存储的数据
        :return: 无返回
        :rtype: None
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        handle = self._handle

        def _writedata():
            # 由于redis不支持事务，所以此处并不会区分节点是否存在，均直接set数据
            handle.hset(path, ".data", data)

        self._run_catch(_writedata)
        log.d("save data success, path:{path}".format(path=path))

    def _del_node(self, np, force):
        """
        删除node节点即所有子节点
        :param str np: 待删除节点
        :return None
        """
        def _deletenode(path=np):
            # 由于redis不支持事务，所以此处并不会区分节点是否存在，均直接set数据
            children = self.get_children(path)
            for child_node in children:
                _deletenode("/".join([path, child_node]))

            node_path, node_name = self._split_node_name(path)
            ret = self._handle.evalsha(self._delete_lua_sha, 2, node_path, node_name)
            if ret == -1:
                raise exception.EPNoNodeError("delete node[%s] error:%s" % (path, "Node has child"))
        if force:
            _deletenode()
        else:
            self._run_catch(_deletenode)

    def create_node(self, path, value="", ephemeral=False, sequence=False, makepath=False):
        """
        根据节点各属性创建节点

        :param str path: 节点路径
        :param str value: 待存数据
        :param bool ephemeral: 是否是临时节点
        :param bool sequence: 是否是顺序节点
        :param bool makepath: 是否创建父节点
        :return: None
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        def _createnode(np=path):
            node_path, node_name = self._split_node_name(np)
            errmsg = {-1: "Node has ttl or parents-node not exists",
                      -2: "Node exists(in parents-node record)",
                      -3: "Node exists"}

            if not self.exists(node_path) and node_path != "/":
                if makepath:
                    self.create_node(node_path, makepath=True)
                else:
                    raise exception.EPNoNodeError(node_path + " not exists")
            seq = 1 if sequence else 0
            tm = long(time.time()) + self._timeout if ephemeral else 0
            ret = self._handle.evalsha(self._new_lua_sha, 2, node_path, node_name, value, seq, tm)
            if ret < 0:
                raise exception.EPIOError("redis error when create[%s:%s]:%s" % (node_path, node_name, errmsg[ret]))
            if ephemeral:
                self._new_touch(ret)
            return ret

        ret = self._run_catch(_createnode)
        log.d("create node success, path:{path}, value:{value}, ephemeral:"
              "{ephemeral}, sequence:{sequence}, makepath:{makepath}".format(
                                                        path=ret, value=value,
                                                        ephemeral=ephemeral, sequence=sequence,
                                                        makepath=makepath))
        return ret

    def exists(self, path):
        """
        查询制定path路径的节点是否存在

        :param str path: 节点路径
        :return: True或False
        :rtype: bool
        """
        def _existnode():
            node_path, node_name = self._split_node_name(path)
            ret = self._handle.hexists(node_path, node_name)
            if ret != 1:
                return False
            ret = self._handle.exists(path)
            if ret != 1:
                return False
            else:
                return True

        return self._run_catch(_existnode)

    def add_listener(self, watcher):
        """
        监听会话状态

        :param watcher: 状态监听函数。函数形参为(state)，可能的取值包括"SUSPENDED"、"CONNECTED"、"LOST"
        :return: 无返回
        :rtype: None
        """
        log.i("nothing to do in RedisPersistence.add_listener()")

    def disconnect(self):
        """
        主动断开持久化请求

        :return: 无返回
        :rtype: None
        """
        super(RedisPersistence, self).disconnect()
        self._handle.close()
