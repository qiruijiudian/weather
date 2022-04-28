# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/3/24 16:17
# @Author  : MAYA

import requests
import platform
import pymysql
import smtplib
import logging
import time
import os
import json
from datetime import datetime, timedelta
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr


if platform.system() == "Windows":
    log_file = "./log/tianjin/logs.txt"
else:
    log_file = "/home/weather/log/tianjin/logs.txt"


logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
                    filename=log_file,
                    filemode='a+',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class Weather:
    def __init__(self):
        self.init_start = "20220401"
        self.conn_conf = {
            "host": "localhost",
            "user": "root",
            "password": "299521",
            "database": "weather",
            "table": "tianjin"
            } if platform.system() == "Windows" else {
                "host": "121.199.48.82",
                "user": "root",
                "password": "cdqr2008",
                "database": "weather",
                "table": "tianjin"
        }
        logging.info("*" * 100)
        logging.info(
            "===============     Start 数据检查开始 {}     ===============\n".format(
                self.convert_datetime_to_str_by_step(datetime.today(), "s")
            )
        )

    def get_conn(self):
        """获取数据库连接

        :return: 数据库连接，数据库游标
        """
        conf = self.conn_conf
        conn = pymysql.connect(
            host=conf["host"], user=conf["user"], password=conf["password"], database=conf["database"]
        )
        cur = conn.cursor()
        return conn, cur

    def latest_data(self, cur):
        """返回数据库中最新的一条数据
        :param cur: 数据库游标
        :return: 组新的一条数据
        """
        cur.execute("select time, temp, humidity from {} order by time desc limit 1".format(self.conn_conf["table"]))
        return cur.fetchone()

    def data_check(self, items):
        if platform.system() == "Windows":
            dir_path = "./weather/data_check/tianjin"
        else:
            dir_path = "/home/weather/data_check/tianjin"
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        now, num = datetime.today().strftime("%Y%m%d_%H"), 1

        res = {}
        length = len(items)
        res["time"] = self.convert_datetime_to_str_by_step(datetime.today(), "m")
        res["length"] = length
        res["first"] = [self.convert_datetime_to_str_by_step(items[0][0], "m"), items[0][1], items[0][2]]
        if length >= 2:
            res["last"] = [self.convert_datetime_to_str_by_step(items[-1][0], "m"), items[-1][1], items[-1][2]]

        name = "{}.json".format(now)
        while os.path.exists(name):
            num += 1
            name = "{}({}).json".format(now, num)

        with open(os.path.join(dir_path, name), 'w', encoding="utf-8") as f:
            f.write(json.dumps(res, ensure_ascii=False, indent=4))

    def is_complete_for_yesterday(self, cur):
        """昨日数据是否完整

        :param cur: 数据库游标
        :return: True or False
        """
        day = datetime.today() - timedelta(days=1)
        day_str = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
        day_start, day_end = "{} 00:00".format(day_str), "{} 23:59".format(day_str)
        complete_date = [
            datetime(year=day.year, month=day.month, day=day.day, hour=i, minute=j) for i in range(24) for j in [0, 30]
        ]

        sql = "select time, temp, humidity from {} where time between '{}' and '{}'".format(
            self.conn_conf["table"], day_start, day_end
        )
        cur.execute(sql)
        items = cur.fetchall()

        if complete_date == [item[0] for item in items]:
            return True
        else:
            return False

    @staticmethod
    def update_new_items(new_items, notify, items, contrast_item):
        """更新需要存储的数据列表

        :param new_items: 新增数据列表
        :param notify: 需要邮件提示的日期字符串
        :param items: 某日完整数据列表
        :param contrast_item: 数据库最新数据
        :return: (更新后的需要存储的数据列表， 需要邮件提示的日期字符串)
        """

        last_item = items[-1]
        if last_item[0] > contrast_item[0]:
            for item in items:
                if item[0] > contrast_item[0]:
                    new_items.append(item)

                if item[0].hour == 23 and item[0].minute == 30:
                    notify = datetime.strftime(item[0], "%Y-%m-%d")

        return new_items, notify

    def store_data_to_db(self, items, conn, cur):

        # print(items)

        if items:
            # 数据检查
            self.data_check(items)

            sql = "insert into {}(time, temp, humidity) values (%s, %s, %s)".format(self.conn_conf["table"])
            cur.executemany(sql, items)
            conn.commit()
            start, end = items[0][0], items[-1][0]

            logging.info("数据更新 {} - {}".format(
                self.convert_datetime_to_str_by_step(start, "m"),
                self.convert_datetime_to_str_by_step(end, "m"),
            ))
        else:
            now = datetime.today()
            logging.info("无新增数据：{}".format(self.convert_datetime_to_str_by_step(now, "m")))

    def update_real_time_data(self):
        """更新每日实时数据

        :param conn: 数据库连接
        :param cur: 数据库游标
        :return:
        """
        logging.debug("实时数据更新 - 当前执行时间：{}".format(datetime.today().strftime("%Y-%m-%d %H:%M")))
        conn, cur = self.get_conn()
        try:

            # new_items: 新增数据 notify：需要发送邮件通知的日期字符串，空字符串则默认不用通知
            new_items, notify, today = [], "", datetime.today()

            latest_data = self.latest_data(cur)
            if today.hour < 12:
                yesterday = today - timedelta(days=1)
                if not self.is_complete_for_yesterday(cur):
                    yesterday_success, yesterday_items = self.get_data_by_date(yesterday)
                    if yesterday_success:
                        new_items, notify = self.update_new_items(new_items, notify, yesterday_items, latest_data)
                        self.store_data_to_db(new_items, conn, cur)

                        if notify:
                            self.send_msg("{} 实时数据更新完毕，数据库当前最新时间： {}".format(
                                notify, self.convert_datetime_to_str_by_step(new_items[-1][0], "m"))
                            )

                        new_items, notify = [], ""
                    else:
                        self.send_msg(
                            "获取昨日遗漏数据失败， 当前最新数据时间： {}".format(
                                self.convert_datetime_to_str_by_step(latest_data[0], "m")
                            )
                        )

            success, items = self.get_data_by_date(today)

            if success:
                new_items, notify = self.update_new_items(new_items, notify, items, latest_data)

            else:
                self.send_msg(
                    "实时数据更新失败， 当前最新数据时间： {}".format(
                        self.convert_datetime_to_str_by_step(latest_data[0], "s")
                    )
                )

            self.store_data_to_db(new_items, conn, cur)

            if notify:
                self.send_msg("{} 实时数据更新完毕，数据库当前最新时间： {}".format(
                    notify, self.convert_datetime_to_str_by_step(new_items[-1][0], "m"))
                )

        except Exception as e:
            now = datetime.today()
            logging.error(
                "实时数据更新异常：{}, Error: {}".format(self.convert_datetime_to_str_by_step(now, "m"), e))

        finally:
            logging.info("===============     Start 数据检查完成 {}     ===============\n".format(
                self.convert_datetime_to_str_by_step(datetime.today(), "s"))
            )
            logging.info("*" * 100)
            cur.close()
            conn.close()

    def update_history_data(self):
        conn, cur = self.get_conn()
        init_start = self.convert_str_to_datetime(self.init_start)
        today_obj = datetime.today()
        start = datetime(year=init_start.year, month=init_start.month, day=init_start.day)
        today = datetime(year=today_obj.year, month=today_obj.month, day=today_obj.day)
        new_items = []
        try:
            while start <= today:
                success, items = self.get_data_by_date(start)
                if success:
                    new_items.extend(items)
                    if start == today:
                        break
                    start += timedelta(days=1)
                else:
                    self.send_msg(
                        "历史数据更新失败， 日期：{}, 数据截止日期：{}".format(
                            self.convert_datetime_to_str_by_step(today_obj, "s"),
                            self.convert_datetime_to_str_by_step(start, "d")
                        )
                    )
                    logging.error(
                        "历史数据更新失败，日期：{}, 数据获取截止日期：{}".format(
                            self.convert_datetime_to_str_by_step(today_obj, "s"),
                            self.convert_datetime_to_str_by_step(start, "d")
                        )
                    )
                    exit()
            self.store_data_to_db(new_items, conn, cur)
            self.send_msg("历史数据更新完成，数据时间范围：{} - {}".format(
                self.convert_datetime_to_str_by_step(init_start, "d"),
                self.convert_datetime_to_str_by_step(start, "d")
            ))
        except Exception as e:
            now = datetime.today()
            logging.error(
                "历史数据更新异常：{}, Error: {}".format(self.convert_datetime_to_str_by_step(now, "m"), e))
        finally:
            logging.info("===============     Start 数据检查完成 {}     ===============\n".format(
                self.convert_datetime_to_str_by_step(datetime.today(), "s"))
            )
            logging.info("*" * 100)
            cur.close()
            conn.close()

    def get_data_by_date(self, date_obj):
        """获取某天的数据
        :param date_obj: datetime日期对象
        :return: 元祖，第一项为正常True或者异常False，第二项为完整数据
        """
        date_str = self.convert_datetime_to_str(date_obj)
        # date_obj = datetime.strptime(date_str, "%Y%m%d")
        res = []
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}
        url = "https://api.weather.com/v1/location/ZBTJ:9:CN/observations/historical.json?apiKey=e1f10a1e78da46f5b10a1e78da96f525&units=e&startDate={0}&endDate={0}".format(date_str)
        try:
            r = requests.get(url, headers=headers)

            items = json.loads(r.text)
            for item in items["observations"]:
                gmt = time.localtime(item["valid_time_gmt"])
                hour = gmt.tm_hour
                minute = gmt.tm_min
                second = gmt.tm_sec
                res.append([datetime(date_obj.year, date_obj.month, date_obj.day, hour, minute, second), item["temp"], item["rh"]])
            return True, res
        except Exception as e:
            logging.error("数据获取失败， 错误原因：{} 时间：{}".format(e, datetime.today().strftime("%Y-%m-%d %H:%M:%S")))
            return False, res

    @staticmethod
    def convert_str_to_datetime(date_str):
        """将日期字符串转换为datetime对象
        :param date_str: 日期字符串，如20220301
        :return: datetime类型日期对象
        """
        return datetime.strptime(date_str, "%Y%m%d")

    @staticmethod
    def convert_datetime_to_str(date_obj):
        """将datetime对象转化为 年月日的格式如20220301
        :param date_obj: datetime类型日期对象
        :return: 日期字符串
        """
        return datetime.strftime(date_obj, "%Y%m%d")

    @staticmethod
    def convert_datetime_to_str_by_step(date_obj, step):
        if step == "s":
            return datetime.strftime(date_obj, "%Y-%m-%d %H:%M:%S")
        elif step == "m":
            return datetime.strftime(date_obj, "%Y-%m-%d %H:%M")
        elif step == "d":
            return datetime.strftime(date_obj, "%Y-%m-%d")

    @staticmethod
    def address_format(s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(), addr))

    def send_msg(self, msg_str):
        """发送邮件
        :param msg_str: 信息
        """
        from_addr = '3491435752@qq.com'
        password = 'rcmfbrdqmqkvcjhh'
        to_addr = '3491435752@qq.com'
        smtp_server = 'smtp.qq.com'
        msg = MIMEText(msg_str, 'plain', 'utf-8')

        msg['From'] = self.address_format('栖睿服务器 <%s>' % from_addr)
        msg['To'] = self.address_format('管理员 <%s>' % to_addr)
        msg['Subject'] = Header('温度数据获取情况', 'utf-8').encode()
        smtp = smtplib.SMTP_SSL(smtp_server)
        smtp.ehlo(smtp_server)
        smtp.login(from_addr, password)
        smtp.sendmail(from_addr, [to_addr], msg.as_string())
        smtp.quit()


# Weather().update_history_data()
Weather().update_real_time_data()

