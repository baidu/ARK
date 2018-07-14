# Quick Start

## 1.下载源码

todo...

## 2.搭建环境

**智能运维机器人**在运行的过程中依赖两个开源的第三方组件（[ZooKeeper][1]和[Elasticsearch][2]），需要用户<font color="Brown" face="微软雅黑" size=3>自行部署</font>。ZooKeeper作为分布式协调系统，为智能运维机器人提供主从选举的功能和异常安全的保障。Elasticsearch作为全文搜索引擎，为用户提供提供事件和运行状态的存储与查询功能。

## 3.修改配置

解压压缩包
`
tar -zxvf ark.tar.gz
`

进入代码目录
`
cd ./ark
`

>**注意：** 
>    初次使用需要创建Elasticsearch所需的Schema
> `
>     sh scripts/create_schema.sh Elasticsearch地址 Elasticsearch端口
> `
> 

生成可执行文件，存放于./output目录
`
./build.sh
`

代码结构如下:
>```
>output
>|-- ark # ARK框架目录
>|-- bin # ARK控制脚本目录
>|-- conf # 配置文件目录
>|-- log # 运行时日志目录
>|-- src # 用户代码目录
>```

修改配置文件./output/conf/ark.conf，默认的配置文件如下
```
{
    "STATE_SERVICE_HOSTS": "", # 添加ZooKeeper地址+端口
    "ARK_SERVER_PORT": ,# 添加guardian使用的端口
    "GUARDIAN_ID": ,# 添加guardian唯一标志
    "INSTANCE_ID": ,# 添加guardian实例标志
    "ARK_ES_HOST": "", # 添加Elasticsearch地址
    "ARK_ES_PORT": ,# 添加Elasticsearch端口
}
```

## 4.运行程序

### 4.1 Demo示例1

#### 4.1.1示例代码

下面Demo代码展示的是最基本的运维机器人功能，即，从本地文件读取感知信息，经决策后执行<font color="Brown" face="微软雅黑" size=3>say_hello</font>方法在日志中输出执行结果。
Demo代码如下，将其写入./output/src/main.py
```python
from assemble.localpull_keymapping import LocalPullKeyMappingGuardian
from are.executor import BaseExecFuncSet
from are import log 


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


def guardian_main():
    """ 

    :return:
    """
	# 报警参数为strategy，当收到的报警值为hello时，调用say_hello方法
	guardian = LocalPullKeyMappingGuardian(
        "../event", {"hello": "say_hello"}, "strategy", DemoExecFuncSet())
    return guardian
```

#### 4.1.2创建本地文件

在./output/目录下创建文件event，并写入模拟感知的json数据。
*strategy*：报警参数名
*hello*：报警参数值
>当报警参数"strategy"的值为"hello"时，执行"say_hello"方法
```
echo '{"strategy": "hello"}' > ./event
```


#### 4.1.3运行

```
cd bin
./control start # 返回0成功，非0失败
```

#### 4.1.4查看结果

查看运行日志(./log/guardian-all.log)，可以看到如下输出，则表示运行成功
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


def guardian_main():
    """ 

    :return:
    """
    nodes = [State1("state1"), State2("state2")]
    ls = LocalPullStateMachineGuardian('../event', nodes)
    return ls


```

Demo示例2的感知文件创建和执行流程同**Demo示例1**，不再赘述。执行成功之后，可在日志文件中看到打印信息

## 5.其他操作-停止、重启

在/output/bin目录执行
停止
`
./control stop
`

重启
`
./control restart
`

[1]:https://zookeeper.apache.org/doc/r3.1.2/zookeeperStarted.html
[2]:https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html    
