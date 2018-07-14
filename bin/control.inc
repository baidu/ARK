#!/bin/bash


####检查文件是否存在且不是目录
function check_file_exist()
{
    local file_name
    if [ $# -ne 1 ];then
        return 1
    fi
    file_name=$1
    if [ -e $file_name -a ! -d $file_name ];then
        return 0
    else
        return 1
    fi
}


####检查文件是否可执行
function check_file_exec()
{
    local file_name
    if [ $# -ne 1 ];then
        return 1
    fi
    file_name=$1
    if [ -x $file_name -a ! -d $file_name ];then
        return 0
    else
        return 1
    fi
}


####检查某路径下的程序是否存在
####根据pid和path精确判断某个container下的程序
function check_proc_exist()
{
    local proc_pid proc_cwd
    if [ $# -ne 2 ];then
        return 1
    fi

    proc_pid=$1
    proc_cwd=$2
    if [ "x${proc_pid}x" == "xx" -o "x${proc_pid}x" == "x x" -o "${proc_pid}" == "NULL" ];then
        return 1
    fi

    #避免使用ssh调用的进程被杀：eg: ssh -n a.b.c.d "sh xxx/control restart"
    #避免使用gdb调用的进程被杀：eg: gdb da core
    #避免使用bash调用的进程被杀：eg: gdb的父进程是bash
    for whiteProg in ssh gdb -bash
    do
        if grep "^${whiteProg} " /proc/${proc_pid}/cmdline 2>/dev/null &> /dev/null; then
            return 1
        fi
    done

    #有些进程读不到cwd，所以使用cwd和cmdline进行两种检查，确保没有遗失
    if /usr/bin/readlink -f /proc/${proc_pid}/cwd | grep "${proc_cwd}" &>/dev/null
    then
        return 0
    elif grep "^${proc_cwd}" /proc/${proc_pid}/cmdline 2>/dev/null
    then
        return 0
    else
        return 1
    fi
}


####检查某路径下的程序线程数是否满足
####根据pid和path精确判断某个container下的程序
function check_thread_num()
{
    local proc_pid proc_cwd proc_num
    local real_num
    if [ $# -ne 3 ];then
        return 1
    fi
    proc_pid=$1
    if [ "x${proc_pid}x" == "xx" -o "x${proc_pid}x" == "x x" ];then
        return 1
    fi

    proc_num=$2
    proc_cwd=$3
    local proc_file="/proc/${proc_pid}/status"
    check_file_exist "${proc_file}"
    if [ $? -eq 0 ];then
        real_num=`grep "^Threads:" "${proc_file}" | awk '{print $2}'`
        if readlink -f /proc/${proc_pid}/cwd | grep "${proc_cwd}" &>/dev/null
        then
            if [ ${real_num} -ge ${proc_num} ];then
                return 0
            else
                return 1
            fi
        else
            return 1
        fi
    else
        return 1
    fi
}


####检查程序监听端口是否开启
function check_port_listen()
{
    local proc_port
    if [ $# -ne 1 ];then
        return 1
    fi
    proc_port=$1
    if [ "x${proc_port}x" == "xx" -o "x${proc_port}x" == "x x" ];then
        return 1
    fi

    if /usr/sbin/lsof -P -i :${proc_port} +c 100 | grep "*:${proc_port}\>" | grep "LISTEN" &>/dev/null
    then
        return 0
    else
        return 1
    fi
}


####检查日志文件是否滚动
function check_log_roll()
{
    local log_name i
    local time_limit this_time last_time
    if [ $# -ne 2 ];then
        return 1
    fi
    if [ ! -r $1 ];then
        return 1
    fi
    if [ $2 -gt 0 2>/dev/null ];then
        time_limit=$2
    else
        return 1
    fi
    log_name=$1
    for((i=0,this_time=`date +%s -r $log_name`,last_time=this_time;i<=time_limit;i++))
    do
        if [ $this_time -gt $last_time ];then
            return 0
        fi
        sleep 1
        last_time=$this_time
        this_time=`date +%s -r $log_name`
    done
    return 1
}


####根据一个pid或一组pid精确kill进程
####一个pid或一组pid不能使用引号引起，具体请参考kill命令语法
function kill_proc()
{
    local proc_pid

    if [ $# -eq 1 ];then
        proc_pid=$1
        local proc_file="/proc/${proc_pid}/status"
        check_file_exist "${proc_file}"
        if [ $? -eq 0 ];then
            kill -9 ${proc_pid} &>/dev/null
            sleep 1
            check_file_exist "${proc_file}"
            if [ $? -eq 0 ];then
                return 1
            else
                return 0
            fi
        else
            return 0
        fi
    elif [ $# -gt 1 ];then
        local proc_pids
        for proc_pid in "$@"
        do
            local proc_file="/proc/${proc_pid}/status"
            check_file_exist "${proc_file}"
            if [ $? -eq 0 ];then
                proc_pids="${proc_pids} ${proc_pid}"
            else
                continue
            fi
        done
        kill -9 ${proc_pids} &>/dev/null
        sleep 1
        local ret=0
        for proc_pid in ${proc_pids}
        do
            local proc_file="/proc/${proc_pid}/status"
            check_file_exist "${proc_file}"
            if [ $? -eq 0 ];then
                ((ret++))
            else
                continue
            fi
        done
        if [ ${ret} -eq 0 ];then
            return 0
        else
            return 1
        fi
    else
        return 0
    fi
}


####根据一个pid或一组pid精确term进程
####一个pid或一组pid不能使用引号引起，具体请参考kill命令语法
function term_proc()
{
    local proc_pid

    if [ $# -eq 1 ];then
        proc_pid=$1
        local proc_file="/proc/${proc_pid}/status"
        check_file_exist "${proc_file}"
        if [ $? -eq 0 ];then
            kill -15 ${proc_pid} &>/dev/null
            sleep 1
            check_file_exist "${proc_file}"
            if [ $? -eq 0 ];then
                return 1
            else
                return 0
            fi
        else
            return 0
        fi
    elif [ $# -gt 1 ];then
        local proc_pids
        for proc_pid in "$@"
        do
            local proc_file="/proc/${proc_pid}/status"
            check_file_exist "${proc_file}"
            if [ $? -eq 0 ];then
                proc_pids="${proc_pids} ${proc_pid}"
            else
                continue
            fi
        done
        kill -15 ${proc_pids} &>/dev/null
        sleep 1
        local ret=0
        for proc_pid in ${proc_pids}
        do
            local proc_file="/proc/${proc_pid}/status"
            check_file_exist "${proc_file}"
            if [ $? -eq 0 ];then
                ((ret++))
            else
                continue
            fi
        done
        if [ ${ret} -eq 0 ];then
            return 0
        else
            return 1
        fi
    else
        return 0
    fi
}


####根据pid进行递归查找子进程或孙子进程
####递归term某个pid下的所有子进程
function term_child_pids()
{
    local proc_pid=$1
    if [ "x${proc_pid}" == "x1" -o "x${proc_pid}x" == "xx" -o "x${proc_pid}x" == "x x" ];then
        return 0
    else
        local proc_cpids
        proc_cpids=`pgrep -P ${proc_pid}`
        kill -15 ${proc_pid} &> /dev/null
        if [ "x${proc_cpids}" == "x" ];then
            return 0
        else
            local proc_cpid
            for proc_cpid in ${proc_cpids}
            do
                if [ "x${proc_cpid}" == "x-1" -o "x${proc_cpid}" == "x" ];then
                    continue
                fi
                term_child_pids "${proc_cpid}"
            done
            return 0
        fi
    fi
}


####作用：将本脚本和父进程的PID从列表中删除
####目的：防止误杀本脚本或本脚本的父进程
####用法：left_pids=$(filter_script_pid $pid_list)
function filter_script_pid()
{
    local proc_pids="$*"
    local proc_ppid
    ## skip script_pid and parent_pid of script_pid
    local  temp_pid=${SCRIPT_PID}
    while  [ 1 ]
    do
        proc_pids=$(echo " ${proc_pids} " | sed -e "s/ ${temp_pid} / /g")
        proc_ppid=$(grep "^PPid" /proc/${temp_pid}/status | awk '{print $2}')
        if [ "x${proc_ppid}" == "x" -o "x${proc_ppid}" == "x1" ];then
            break
        fi
        temp_pid=${proc_ppid}
    done
    echo ${proc_pids}
}


####获取服务进程pid，使用全局变量传递
####通过程序路径支持同一个服务单机多实例部署
####二进制文件勿从非top路径之外软连
function get_pid()
{
    check_file_exist "${STA_DIR}/status"
    if [ $? -eq 0 ];then
        PROC_PID=$(/usr/bin/od -d --skip-bytes=16 ${STA_DIR}/status | awk '{print $2}')
        sleep 3
        check_proc_exist "${PROC_PID}" "${TOP_DIR}"
        if [ $? -eq 0 ];then
            PROC_PID=${PROC_PID}
            return
        fi
    fi
    proc_pids=`ps axo pid,cmd | egrep "${PROC_NAME}\>" | grep -v "${SUPERVISE}\>" | grep -v "grep" | awk '{print $1}'`
    #过滤掉本脚本和调用本脚本的脚本PID
    proc_pids=$(filter_script_pid ${proc_pids})
    PROC_PID=NULL
    for proc_pid in ${proc_pids[@]}
    do
        check_proc_exist "${proc_pid}" "${TOP_DIR}"
        if [ $? -eq 0 ];then
            PROC_PID=${proc_pid}
        fi
    done
}


####根据进程pid获取父进程pid，使用全局变量传递
####通过程序路径支持同一个服务单机多实例部署
function get_ppid()
{
    local proc_pid=$1
    PROC_PPID=$(grep "^PPid" /proc/${proc_pid}/status | awk '{print $2}')
    if [ ${PROC_PPID} -eq 1 ];then
        PROC_PPID=1
    else
        check_proc_exist "${PROC_PPID}" "${TOP_DIR}"
        if [ $? -eq 0 ];then
            PROC_PPID=${PROC_PPID}
        else
            PROC_PPID=NULL
        fi
    fi
}


####根据进程pid获取所有子进程pid
####暂时未使用
function get_cpid()
{
    local proc_pid=$1
    local proc_cpid
    local proc_cpids=""
    if [ "x${proc_pid}" == "x1" ];then
        PROC_CPID=NULL
    else
        PROC_CPID=$(pgrep -P ${proc_pid})
        for proc_cpid in ${PROC_CPID}
        do
            check_proc_exist "${proc_cpid}" "${TOP_DIR}"
            if [ $? -eq 0 ];then
                proc_cpids="${proc_cpids} ${proc_cpid}"
            else
                continue
            fi
        done
        if [ "x${proc_cpids}" == "x" ];then
            PROC_CPID=NULL
        else
            PROC_CPID=${proc_cpids}
        fi
    fi
}


####获取守护进程pid或pids，防止supervise反复启动supervise自身
function get_spid()
{
    local proc_spid
    local proc_spids
    check_file_exist "${STA_DIR}/lock"
    if [ $? -eq 0 ];then
        PROC_SPID=$(/sbin/fuser ${STA_DIR}/lock | awk '{print $NF}')
        check_proc_exist "${PROC_SPID}" "${TOP_DIR}"
        if [ $? -eq 0 ];then
            PROC_SPID=${PROC_SPID}
        else
            PROC_SPID=`ps axo pid,cmd | grep "${SUPERVISE}\>" | grep "${TOP_DIR}" | grep -v "grep" | awk '{print $1}'`
            if [ "x${PROC_SPID}" != "x" ];then
                for proc_spid in ${PROC_SPID}
                do
                    check_proc_exist "${proc_spid}" "${TOP_DIR}"
                    if [ $? -ne 0 -o "x${proc_spid}" == "x1" ];then
                        continue
                    fi
                    proc_spids="${proc_spids} ${proc_spid}"
                done
                PROC_SPID=${proc_spids}
            else
                PROC_SPID=NULL
            fi
        fi
    else
        PROC_SPID=`ps axo pid,cmd | grep "${SUPERVISE}\>" | grep "${TOP_DIR}" | grep -v "grep" | awk '{print $1}'`
        if [ "x${PROC_SPID}" != "x" ];then
            for proc_spid in ${PROC_SPID}
            do
                check_proc_exist "${proc_spid}" "${TOP_DIR}"
                if [ $? -ne 0 -o "x${proc_spid}" == "x1" ];then
                    continue
                fi
                proc_spids="${proc_spids} ${proc_spid}"
            done
            PROC_SPID=${proc_spids}
        else
            PROC_SPID=NULL
        fi
    fi
}


####获取某个container下所有的孤儿进程
####剔除control脚本执行时的pid，避免误杀control，agent调用control脚本不会被kill
function get_opid()
{
    local proc_opid
    local proc_ppid
    local proc_opids=""
    local temp_pid
    PROC_OPID=`ps axo pid,cmd | grep "${PROC_NAME}" | grep -v "grep" | awk '{print $1}'`
    if [ "x${PROC_OPID}" != "x" ];then
        for proc_opid in ${PROC_OPID}
        do
            check_proc_exist "${proc_opid}" "${TOP_DIR}"
            if [ $? -ne 0 ];then
                continue
            fi
            if [ "x${proc_opid}" == "x${SCRIPT_PID}" ]; then
                continue
            fi
            proc_opids="${proc_opids} ${proc_opid}"
            # add parent_pid of proc_opid
            temp_pid=$proc_opid
            while [ 1 ]
            do
                get_ppid ${temp_pid}
                if [ "${PROC_PPID}" == "NULL" -o "x${PROC_PPID}" == "x1" ];then
                    break
                fi
                proc_opids="${proc_opids} ${PROC_PPID}"
                temp_pid=${PROC_PPID}
            done
        done
        #过滤掉本脚本和调用本脚本的进程PID 
        PROC_OPID=$(filter_script_pid ${proc_opids})
    else
        PROC_OPID=NULL
    fi
}


####服务启动状态检查接口，判断进程是否存在、服务进程的线程数是否满足要求，服务监听的端口是否开启
####针对basa框架下不同子进程(ac,da)的线程暂时不做判断，由basa框架内部判断
####后续使用libmonitor实现，或由用户自定义，control使用hook加载
function check_serv_alive()
{
    check_proc_exist "${PROC_PID}" "${TOP_DIR}"
    if [ $? -ne 0 ];then
        return 1
    fi
    check_thread_num "${PROC_PID}" "${TRD_NUM}" "${TOP_DIR}"
    if [ $? -ne 0 ];then
        return 1
    fi
    check_guardian
    if [ $? -ne 0 ];then
        return 1
    fi
    #check_port_listen "${PROC_PORT}"
    #if [ $? -ne 0 ];then
    #    return 1
    #fi
    return 0
}


####服务启动命令
####后续由用户自定义该接口，control使用hook加载
function start_cmd()
{
########
    if [ -e "${STA_DIR}/status" ];then
        cd ${STA_DIR} && rm -rf ./status
    fi
    if [ -e "${STA_DIR}/lock" ];then
        cd ${STA_DIR} && rm -rf ./lock
    fi
    if [ -e "${STA_DIR}/svcontrol" ];then
        cd ${STA_DIR} && rm -rf ./svcontrol
    fi
########
    cd ${BIN_DIR}
    ${START_COMMAND} 1>${LOG_DIR}/${CONTROL_LOG} 2>&1 </dev/null &
    cd ${TOP_DIR}
########
    local tmp=0
    while [ 1 ]
    do
        if [ ${tmp} -gt 5 ];then
            break
        fi
        ((tmp++))
        check_file_exist "${STA_DIR}/status"
        if [ $? -ne 0 ];then
            sleep 1
            continue
        else
            break
        fi
    done
}


####服务停止命令
####默认提供kill -9或kill -15或优雅退出方式
####优雅退出命令后续在control.conf定义，原因是不同程序优雅退出实现不同
####后续由用户自定义该接口，control使用hook加载
function stop_cmd()
{
    case "${STOP_COMMAND_FLAG}" in
        0)
            if [ "${PROC_PID}" != "NULL" -a "${PROC_PID}" != "1" ];then
                kill_proc "${PROC_PID}"
            fi
            ;;
        1)
            if [ "${PROC_PID}" != "NULL" -a "${PROC_PID}" != "1" ];then
                term_proc "${PROC_PID}"
            fi
            ;;
        2)
            cd ${TOP_DIR}
            ${BIN_DIR}/${PROC_RELOAD_NAME} -p ${PROC_RELOAD_PORT} -x &>${LOG_DIR}/${CONTROL_LOG}
            ;;
        *)
            echo "Stop service command is not available"
            ;;
    esac
}


####程序健康检查
function healthcheck()
{
    cd ${TOP_DIR}
    get_pid
    local tmp=0
    while [ 1 ]
    do
        if [ ${tmp} -gt 5 ];then
            return 1
        fi
        ((tmp++))
        check_serv_alive
        if [ $? -ne 0 ];then
            sleep 1
            continue
        else
            break
        fi
    done
    return 0
}


function start_check()
{
    cd ${TOP_DIR}
    get_pid
    if [ "NULL" == "${PROC_PID}" ]; then
        return 1
    fi
    local tmp=0
    while [ 1 ]
    do
        if [ ${tmp} -gt ${MAX_START_CHK_TIME} ];then
            return 1
        fi
        ((tmp++))
        check_serv_alive
        if [ $? -ne 0 ];then
            sleep 1
            continue
        else
            break
        fi
    done
    return 0
}


####检查guardian是否启动
function check_guardian
{
    GUARDIAN_PID=`ps x | grep ${BIN_DIR}/../ark/are/${PROC_NAME} | grep -v "supervise" | grep -v "vim" | grep -v "cat" | grep -v grep | awk '{print $1}'`
    if [ "x${GUARDIAN_PID}" != "x" ];then
        for guardian_pid in ${GUARDIAN_PID}
        do
            check_proc_exist "${guardian_pid}" "${TOP_DIR}"
            if [ $? -eq 1 ];then
                return 1
            fi
        done
        return 0
    else
        return 1
    fi
}


function stop_check()
{
    cd ${TOP_DIR}
    get_pid
    local tmp=0
    while [ 1 ]
    do
        if [ ${tmp} -gt ${MAX_STOP_CHK_TIME} ];then
            return 1
        fi
        ((tmp++))
        check_serv_alive
        if [ $? -eq 0 ];then
            sleep 1
            continue
        else
            break
        fi
    done
    return 0
}

####加载部署时下发的环境变量配置信息
function prepare_env()
{
    if [ -f ${AS_ENV_CONF_FILE} ];then
        set -o allexport
        source ${AS_ENV_CONF_FILE}
        set +o allexport
    fi

    if [ -f ${MC_ENV_CONF_FILE} ];then
        set -o allexport
        source ${MC_ENV_CONF_FILE}
        set +o allexport
    fi

    return 0
}
