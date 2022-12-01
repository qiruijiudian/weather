# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/11/30 10:04
# @Author  : MAYA

import platform

BASE_PATH = "/home/weather"
LOG_FILE = {"tianjin": "/home/weather/log/tianjin/logs.txt", "cona": "/home/weather/log/cona/logs.txt"}
SIG_FILE = {"tianjin": "/home/weather/sig/tianjin.txt", "cona": "/home/weather/sig/cona.txt"}
CONN_CONF = {"host": "121.199.48.82", "user": "root", "password": "cdqr2008", "database": "weather"}
DATA_CHECK_PATH = {"tianjin": "/home/weather/data_check/tianjin", "cona": "/home/weather/data_check/cona"}
TABLES = {"cona": "cona", "tianjin": "tianjin"}

if platform.system() == "Windows":
    LOG_FILE = {"tianjin": "./log/tianjin/logs.txt", "cona": "./log/cona/logs.txt"}
    CONN_CONF = {"host": "localhost", "user": "root", "password": "cdqr2008", "database": "weather"}
    SIG_FILE = {"tianjin": "./sig/tianjin.txt", "cona": "./sig/cona.txt"}
    DATA_CHECK_PATH = {"tianjin": "./data_check/tianjin", "cona": "./data_check/cona"}



