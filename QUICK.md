# Quick Start

## 0.依赖

**智能运维机器人**在**正式**的运行的过程中依赖两个开源的第三方组件（[ZooKeeper][1]和[Elasticsearch][2]），需要用户<font color="Brown" face="微软雅黑" size=3>自行部署</font>。ZooKeeper作为分布式协调系统，为智能运维机器人提供主从选举的功能和异常安全的保障。Elasticsearch作为全文搜索引擎，为用户提供提供事件和运行状态的存储与查询功能。

为了便于快速上手验证，本文我们使用本地文件方式代替ZooKeeper。

## 1.下载源码

git clone git@github.com:baidu/ARK.git $ARK_DEV_PATH


## 2.准备环境

将ARK源码所在路径加入PYTHONPATH

`
export PYTHONPATH=$ARK_DEV_PATH:$PYTHONPATH
`

新建持久化路径(任意路径均可)，用于模拟ZooKeeper，存放持久化数据

`
mkdir $ARK_PERSIST_PATH
`

## 3.修改配置

修改配置文件$ARK_DEV_PATH/demo/local_guardian/conf/ark.conf，配置文件说明如下
```
{
    "STATE_SERVICE_HOSTS": "", # 填入$ARK_PERSIST_PATH的绝对路径
    "ARK_SERVER_PORT": "",# 添加guardian使用的端口，任意端口均可
    "GUARDIAN_ID": "",# 添加guardian唯一标识
    "INSTANCE_ID": "",# 添加guardian实例标识
    "ARK_ES_HOST": "", # 添加Elasticsearch地址
    "ARK_ES_PORT": "",# 添加Elasticsearch端口
}
```

## 4.运行程序

### 4.1 Demo示例1

#### 4.1.1示例代码

下面Demo代码展示的是最基本的运维机器人功能，即，从本地文件读取感知信息，经决策后执行<font color="Brown" face="微软雅黑" size=3>say_hello</font>方法在日志中输出执行结果。

Demo代码如下，可直接使用$ARK_DEV_PATH/demo/local_guardian/bin/main.py

```python
# -*- coding: UTF-8 -*-
# @Time    : 2018/5/24 下午3:49
# @File    : main.py
"""
guardian demo
"""
import os
import sys
from ark.assemble.localpull_keymapping import LocalPullKeyMappingGuardian
from ark.are.executor import BaseExecFuncSet
from ark.are import log


class DemoExecFuncSet(BaseExecFuncSet):
    """
    demo exec
    """
    def say_hello(self, params):
        """

        :param params:
        :return:
        """
        log.info("hello, event params:{}".format(params))
        return {}


def guardian_main(mode):
    """
    guardian_main
    :return:
    """
	# 报警参数为strategy，当收到的报警值为hello时，调用say_hello方法
	guardian = LocalPullKeyMappingGuardian(
        "../event", {"hello": "say_hello"}, "strategy", DemoExecFuncSet())
    return guardian
```

#### 4.1.2创建本地文件

在$ARK_DEV_PATH/demo/local_guardian目录下找到文件event，其内容为模拟感知的json数据：

* *strategy*：报警参数名
* *hello*：报警参数值

>当报警参数"strategy"的值为"hello"时，执行"say_hello"方法

```
echo '{"strategy": "hello"}' > ./event
```

#### 4.1.3运行

```
cd $ARK_DEV_PATH/demo/local_guardian/bin
python $ARK_DEV_PATH/ark/are/loader.py main.py -p local -c ../conf/ark.conf
```

#### 4.1.4查看结果

查看运行日志($ARK_DEV_PATH/demo/local_guardian/log/guardian-all.log)，可以看到如下输出，则表示运行成功

```
hello, event params:{'.inner_executor_key': 'say_hello', u'strategy': u'hello'}
```

### 4.2 Demo示例2

#### 4.2.1示例代码

本次Demo展示的是状态机智能运维机器人的用法。机器人从本地文件读取感知消息，经决策后分步骤执行用户定义的操作，这些操作默认都是不可重入的。
在下面的示例中，先执行State1，成功后执行State2。

```python
from assemble.localpull_statemachine import LocalPullStateMachineGuardian
from are.executor import BaseExecFuncSet
from are import log 
from are import graph
import time


class State1(graph.State):
    """ 
	状态机的第一个阶段，该阶段执行完成之后，接着返回的下一待执行阶段，本例中返回"state2"
    """
    def process(self, session, current_node, nodes_process):
        """ 

        :param session:
        :param current_node:
        :param nodes_process:
        :return:
        """
        log.info("state1 process---------------")
        time.sleep(2)
        return "state2"


class State2(graph.State):
    """ 
	状态机的第二个阶段，该阶段执行完之后，返回"ARK_NODE_END"，表示执行完成
    """
    def process(self, session, current_node, nodes_process):
        """ 

        :param session:
        :param current_node:
        :param nodes_process:
        :return:
        """
        log.info("state2 process-----------")
        time.sleep(2)
        return "ARK_NODE_END"


def guardian_main(mode):
    """ 

    :return:
    """
    nodes = [State1("state1"), State2("state2")]
    ls = LocalPullStateMachineGuardian('../event', nodes)
    return ls


```

Demo示例2的感知文件创建和执行流程同**Demo示例1**，不再赘述。执行成功之后，可在日志文件中看到打印信息


[1]:https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html
[2]:https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html    
