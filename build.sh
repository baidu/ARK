#!/bin/bash

WORK_DIR="$(cd $(dirname "$0"); pwd)"
OUTPUT_DIR=${WORK_DIR}/output

function usage()
{
    echo ""
    echo "不输入参数，默认进行打包"
    echo "ut 运行单测"
    echo ""
    exit 255
}

function prepare_python_env()
{
    echo "prepare python env"

    PYTHON_VERSION=`python -V 2>&1|awk '{print $2}'`
    [ $? -ne 0 ] && echo "python is not installed, please install python 2.7.x first" && exit 1

    PYTHON_STANDARD_VERSION="^2\.7\."
    if [[ $PYTHON_VERSION =~ $PYTHON_STANDARD_VERSION ]]; then
        echo "python version is match"
    else
        echo "python version is not match, please install python 2.7.x first" && exit 1
    fi

    pip -V
    [ $? -ne 0 ] && echo "pip is not installed, please install pip first" && exit 1

    pip install -r requirements.txt
    [ $? -ne 0 ] && echo "pip install requirements failed" && exit 1

}

function package()
{
    rm -rf ${OUTPUT_DIR}
    mkdir ${OUTPUT_DIR}
    mkdir ${OUTPUT_DIR}/log

    cp -r ${WORK_DIR}/ark ${OUTPUT_DIR}
    [ $? -ne 0 ] && echo "cp ark lib dir failed" && exit 1

    cp -r ${WORK_DIR}/bin ${OUTPUT_DIR}
    [ $? -ne 0 ] && echo "cp ark bin dir failed" && exit 1

    cp -r ${WORK_DIR}/conf ${OUTPUT_DIR}
    [ $? -ne 0 ] && echo "cp ark conf dir failed" && exit 1

    cp -r ${WORK_DIR}/demo ${OUTPUT_DIR}/src
    [ $? -ne 0 ] && echo "cp ark src dir failed" && exit 1
}


function unit_test()
{
    export PYTHONPATH=${WORK_DIR}/ark:${PYTHONPATH:-}

    nosetests tests/* --cover-erase --with-coverage --cover-branches --cover-xml --cover-html --cover-package=ark/are --cover-package=ark/opal
    [ $? -ne 0 ] && echo "unit test failed" && exit 1

    BRANCH_RATE=$(grep "coverage branch-rate=" coverage.xml | awk '{print $2}')
    echo "分支覆盖率：${BRANCH_RATE}"
}

function main()
{
    case "${COMMAND}" in
        '')
            prepare_python_env
            package
            ;;
        'ut')
            prepare_python_env
            unit_test
            ;;
        -h|--help|*)
            usage
            ;;
    esac
}

COMMAND=$1
main $@
