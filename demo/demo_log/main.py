# -*- coding: UTF-8 -*-

from ark.are import config
from ark.are import log
from ark.are import exception
import sys
config.GuardianConfig.set({"LOG_CONF_DIR": "./", "LOG_ROOT": "./log"})


def func(a):
    try:
        log.f("ddd")
        raise exception.EPNoNodeError("def")
    except:
        print sys.exc_info()
        log.f("abc")



func(1)
