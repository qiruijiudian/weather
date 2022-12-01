# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/11/30 10:26
# @Author  : MAYA
import json
import logging
import time
import traceback

import pymysql
import requests

from settings import CONN_CONF, TABLES, LOG_FILE
from copy import deepcopy
from datetime import datetime
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr


def get_conf(block):
    """
    在基础数据库连接信息中追加table属性
    :param block: 数据源
    :return: 完整数据库连接信息
    """
    conf = deepcopy(CONN_CONF)
    conf["table"] = TABLES[block]
    return conf


def get_conn(conf):
    """
    获取数据库连接和游标
    :param conf: 数据库配置
    :return: 数据库连接，游标
    """
    conn = pymysql.connect(
        host=conf["host"], user=conf["user"], password=conf["password"], database=conf["database"]
    )
    cur = conn.cursor()
    return conn, cur


def convert_datetime_to_str_by_step(date_obj, step):
    """
    根据step值将datetime转换为字符串
    :param date_obj: datetime对象
    :param step: 转换key值
    :return: 日期字字符串
    """

    if step == "s":
        return datetime.strftime(date_obj, "%Y-%m-%d %H:%M:%S")
    elif step == "m":
        return datetime.strftime(date_obj, "%Y-%m-%d %H:%M")
    elif step == "d":
        return datetime.strftime(date_obj, "%Y-%m-%d")


def convert_str_to_datetime(date_str):
    """
    将日期字符串转换为datetime对象
    :param date_str: 日期字符串，如20220301
    :return: datetime类型日期对象
    """
    return datetime.strptime(date_str, "%Y%m%d")


def convert_datetime_to_str(date_obj):
    """
    将datetime对象转化为 年月日的格式如20220301
    :param date_obj: datetime类型日期对象
    :return: 日期字符串
    """
    return datetime.strftime(date_obj, "%Y%m%d")


def get_response(url, retry=3):
    """
    获取网页请求返回值

    :param url: 链接
    :param retry: 重试次数
    :return: 网页请求返回值
    """
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'cache-control': "no-cache"
    }
    num = 1
    while num <= retry:
        try:
            r = requests.get(url, headers=headers)
            return r.text
        except Exception as e:
            logging.debug("访问异常：{}， 重试第{}次".format(e, num))
        num += 1
        time.sleep(num)
    logging.error("访问数据失败(get_response)，当前时间：{}".format(convert_datetime_to_str_by_step(datetime.now(), "s")))


def address_format(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))


def send_msg(msg_str, subject="温度数据获取情况"):
    """
    发送邮件
    :param msg_str: str 信息
    :param subject: str 邮件主题
    """
    from_addr = '3491435752@qq.com'
    password = 'rcmfbrdqmqkvcjhh'
    to_addr = '3491435752@qq.com'
    smtp_server = 'smtp.qq.com'
    msg = MIMEText(msg_str, 'plain', 'utf-8')

    msg['From'] = address_format('栖睿服务器 <%s>' % from_addr)
    msg['To'] = address_format('管理员 <%s>' % to_addr)
    msg['Subject'] = Header(subject, 'utf-8').encode()
    smtp = smtplib.SMTP_SSL(smtp_server)
    smtp.ehlo(smtp_server)
    smtp.login(from_addr, password)
    smtp.sendmail(from_addr, [to_addr], msg.as_string())
    smtp.quit()


def log_conf_init(block):
    """
    初始化日志配制

    :param block: 数据源
    """
    logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
                        filename=LOG_FILE[block],
                        filemode='a+',
                        format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def program_debug(level):
    """
    调试错误打印

    :param level: 是否打印错误
    """
    if level:
        traceback.print_exc()
