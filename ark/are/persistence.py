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
from ark.are import common
from ark.are import exception
from ark.are import log
from ark.are import config


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

    def delete_node(self, path):
        """
        删除node节点
        :param str path: 数据存储路径
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
        :return: 无返回
        :rtype: None
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
        except kazoo.interfaces.IHandler.timeout_exception:
            raise exception.EPConnectTimeout()
        except kazoo.exceptions.NoNodeError:
            raise exception.EPNoNodeError()
        except:
            log.r(exception.EPIOError(), "Request I/O Error")

    @classmethod
    def _new_session(cls):
        """
        创建kazoo.client.KazooClient实例

        :return: kazoo.client.KazooClient实例
        :rtype: kazoo.client.KazooClient
        :raises: exception.EPConnectTimeout 连接超时异常
        """
        # 仅在必要的情况下才引入kazoo
        import kazoo
        hosts = config.GuardianConfig.get(config.STATE_SERVICE_HOSTS_NAME)
        return ZkPersistence._run_catch(lambda: (kazoo.client.KazooClient(hosts=hosts)))

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

    def delete_node(self, path):
        """
        删除node节点
        :param str path: 数据存储路径
        :return None
        """

        ZkPersistence._run_catch(lambda: (self._client.delete(path=path, recursive=True)))
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
        :return: None
        :raises: exception.EPNoNodeError 节点不存在
        :raises: exception.EPIOError IO异常
        """
        ZkPersistence._run_catch(lambda: (self._client.create(path, value, None,
                                                              ephemeral, sequence,
                                                              makepath)))

        log.d("create node success, path:{path}, value:{value}, ephemeral:"
                  "{ephemeral}, sequence:{sequence}, makepath:{makepath}".format(
                                                        path=path, value=value,
                                                        ephemeral=ephemeral, sequence=sequence,
                                                        makepath=makepath))

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


class FilePersistence(BasePersistence):
    """
    通过文件系统实现的持久化类。实体节点用文件系统中的目录表示，实体节点的数据存放在目录下的.data文件中。临时节点用文件系统中的文件来表示，临时节点的数据存放在对应文件中，临时节点（文件）会被定期touch以保持其最新，超过三个周期的会被自动删除。

    """
    _init = False
    _file_mode = "0755"

    def __init__(self):
        """
        初始化方法
        """
        import string
        import threading
        if FilePersistence._init:
            return

        self._lock = threading.Lock()
        self._inspect_results = {}
        self._ob_paths = {}  # 需要观察路径下节点变化的路径列表
        self._touch_paths = {}  # 针对临时节点，需要不断touch的路径列表
        self._base = config.GuardianConfig.get(config.STATE_SERVICE_HOSTS_NAME)
        self._mode = string.atoi(config.GuardianConfig.get("PERSIST_FILE_MODE", FilePersistence._file_mode), 8)
        self._interval = string.atof(config.GuardianConfig.get("PERSIST_FILE_INTERVAL", "0.4"))
        self._timeout = string.atof(config.GuardianConfig.get("PERSIST_FILE_TIMEOUT", "3"))
        if not os.path.exists(self._base):
            os.makedirs(self._base, self._mode)

        self._session_thread = threading.Thread(target=self._thread_run)
        FilePersistence._init = True
        self._session_thread.daemon = True
        self._session_thread.start()

    def __del__(self):
        """
        析构方法，完成线程回收
        """
        FilePersistence._init = False

    def _refresh(self, obpath):
        """
        刷新节点属性
        """
        result = {}
        # 获取该路径所有状态
        if os.path.exists(obpath):
            result["exist"] = True
            # 获取该路径数据
            import hashlib
            data = self.get_data(obpath[len(self._base):])
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
            result["children"] = None
        return result

    def _inspect(self, obpath, watcher):
        """
        检查节点是否有变化，如果有则触发wather函数
        """
        result = self._refresh(obpath)
        if obpath not in self._inspect_results:
            self._inspect_results[obpath] = result
            return
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

        # 判断子节点是否有变化
        if len(last_result["children"]) != len(result["children"]) or\
           len(last_result["children"] - result["children"]) != 0:
            watcher(PersistenceEvent(type=PersistenceEvent.EventType.CHILD,
                                     state=PersistenceEvent.PersistState.CONNECTED,
                                     path=obpath))
            self._ignore(obpath)
            return
        return

    def _ignore(self, obpath):
        with self._lock:
            self._ob_paths.pop(obpath)
            self._inspect_results.pop(obpath)
        return

    def _touch(self, tp, now):
        """
        更新临时节点的时间，并清理已经过期的临时节点
        """
        if not os.path.exists(tp) or os.path.isdir(tp):
            with self._lock:
                self._touch_paths.pop(tp)
        os.utime(tp, None)
        dirname = os.path.dirname(tp)
        for name in os.listdir(dirname):
            if name == ".data" or name == ".sequence":
                continue
            file_name = "/".join((dirname, name))
            if os.path.isdir(file_name):
                continue
            mtime = os.stat(file_name).st_mtime
            if now - mtime > self._timeout * 2:
                os.remove(file_name)

    def _thread_run(self):
        """
        获取路径变化的事件，并保持临时节点的时间为最新
        """
        import copy
        import time
        while self._init:
            with self._lock:
                ob_paths = copy.copy(self._ob_paths)
                touch_paths = copy.copy(self._touch_paths)
            now = time.time()
            for tp in touch_paths:
                self._touch(tp, now)
            for obp, watcher in ob_paths.iteritems():
                self._inspect(obp, watcher)

            time.sleep(self._interval)

    @classmethod
    def _run_catch(cls, func, path, path_is_dir=False):
        """
        执行func并捕获异常，将kazoo异常转换为对应的异常对象
        """
        try:
            if not os.path.exists(path) or (path_is_dir and not os.path.isdir(path)):
                raise exception.EPNoNodeError()
            return func()
        except exception.EPNoNodeError as e:
            log.r(e, "Node not exist:{}".format(path))
        except:
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
        path = self._base + path

        def _readdata():
            if os.path.isdir(path):
                file_path = "/".join((path, ".data"))
            else:
                file_path = path
            if not os.path.exists(file_path):
                return ""
            with open(file_path, 'r') as f:
                return f.read()

        return FilePersistence._run_catch(_readdata, path)

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
        path = self._base + path

        def _writedata():
            if os.path.isdir(path):
                file_path = "/".join((path, ".data"))
            else:
                file_path = path
            with open(file_path, 'w') as f:
                return f.write(data)

        FilePersistence._run_catch(_writedata, path)
        log.d("save data success, path:{path}".format(path=path))

    def delete_node(self, path):
        """
        删除node节点
        :param str path: 数据存储路径
        :return None
        """
        path = self._base + path
        import shutil
        FilePersistence._run_catch(lambda: (shutil.rmtree(path, True)), path)
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
        path = self._base + path

        def _list_and_watch():
            node_list = []
            for node_name in os.listdir(path):
                if node_name == ".data" or node_name == ".sequence":
                    continue
                if not include_data:
                    node_list.append(node_name)
                else:
                    data = self.get_data("/".join((path[len(self._base):], node_name)))
                    node_list.append((node_name, data))
            if watcher:
                if not callable(watcher):
                    raise exception.ETypeMismatch("watcher must callable")
                with self._lock:
                    self._ob_paths[path] = watcher
            return node_list

        return FilePersistence._run_catch(_list_and_watch, path, True)

    @classmethod
    def _seq_file_name(cls, path):
        """
        生成临时节点的序列号。最大序列号会被记录在.sequence文件中。以避免前后多次运行使用同一序列号。序列号生成的过程通过文件锁保证事务
        """
        import re
        import string
        import fcntl
        dirname = os.path.dirname(path)
        basename = os.path.basename(path)
        max_sn = -1

        with open("/".join((dirname, ".sequence")), 'a+') as f:
            f.seek(0)
            try:
                max_sn = string.atoi("0" + f.read(11))
            except ValueError:
                pass
            # 加文件锁，保证序列id自增的唯一性
            fcntl.flock(f, fcntl.LOCK_EX)
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
            fcntl.flock(f, fcntl.LOCK_UN)

        return path + ("%09d" % (max_sn + 1))

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
        path = self._base + path
        try:
            if ephemeral:
                # 临时节点，直接用文件来表示
                dirname = os.path.dirname(path)
                if not os.path.exists(dirname):
                    if makepath:
                        os.makedirs(dirname, self._mode)
                    else:
                        raise exception.EPNoNodeError
                file_path = path
                if sequence:
                    file_path = FilePersistence._seq_file_name(path)
                with open(file_path, 'w') as f:
                    f.write(value)
                with self._lock:
                    self._touch_paths[file_path] = ""
            else:
                # 实体节点，用目录来表示，数据存放在目录下的.data文件中
                if not os.path.exists(path):
                    if not os.path.exists(os.path.dirname(path)) and not makepath:
                        raise exception.EPNoNodeError
                    os.makedirs(path, self._mode)

                file_path = "/".join((path, ".data"))

                with open(file_path, 'w') as f:
                    f.write(value)
        except exception.EPNoNodeError as e:
            log.r(e, "Node not exist:{}".format(os.path.dirname(path)))
        except:
            log.r(exception.EPIOError(), "Request I/O Error")
        log.d("create node success, path:{path}, value:{value}, ephemeral:"
              "{ephemeral}, sequence:{sequence}, makepath:{makepath}".format(
                                                        path=path, value=value,
                                                        ephemeral=ephemeral, sequence=sequence,
                                                        makepath=makepath))

    def exists(self, path):
        """
        查询制定path路径的节点是否存在

        :param str path: 节点路径
        :return: True或False
        :rtype: bool
        """
        path = self._base + path
        return os.path.exists(path)

    def add_listener(self, watcher):
        """
        监听会话状态

        :param watcher: 状态监听函数。函数形参为(state)，可能的取值包括"SUSPENDED"、"CONNECTED"、"LOST"
        :return: 无返回
        :rtype: None
        """
        log.i("nothing to do in FilePersistence.add_listener()")
