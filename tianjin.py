# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/3/24 16:17
# @Author  : MAYA
import os
import time
import json
import logging
from datetime import datetime, timedelta
from settings import SIG_FILE, DATA_CHECK_PATH
from tools import get_conf, convert_datetime_to_str_by_step, convert_str_to_datetime, convert_datetime_to_str, \
    get_response, send_msg, log_conf_init, program_debug, get_conn


class Weather:
    def __init__(self):
        self.block = "tianjin"
        self.init_start = "20220301"
        self.conn_conf = get_conf(self.block)
        self.is_debug = False
        log_conf_init(self.block)
        logging.info("*" * 100)
        logging.info(
            "===============     Start 数据检查开始 {}     ===============\n".format(
                convert_datetime_to_str_by_step(datetime.today(), "s")
            )
        )

    def latest_data(self, cur):
        """
        返回数据库中最新的一条数据
        :param cur: 数据库游标
        :return: 最新的一条数据
        """
        cur.execute("select time, temp, humidity from {} order by time desc limit 1".format(self.conn_conf["table"]))
        return cur.fetchone()

    def data_check(self, items):
        """
        数据检查
        :param items: 数据集
        """
        if not os.path.exists(DATA_CHECK_PATH[self.block]):
            os.mkdir(DATA_CHECK_PATH[self.block])

        now, num = datetime.today().strftime("%Y%m%d_%H"), 1

        res = {}
        length = len(items)
        res["time"] = convert_datetime_to_str_by_step(datetime.today(), "m")
        res["length"] = length
        res["first"] = [convert_datetime_to_str_by_step(items[0][0], "m"), items[0][1], items[0][2]]
        if length >= 2:
            res["last"] = [convert_datetime_to_str_by_step(items[-1][0], "m"), items[-1][1], items[-1][2]]

        name = "{}.json".format(now)
        while os.path.exists(os.path.join(DATA_CHECK_PATH[self.block], name)):
            num += 1
            name = "{}({}).json".format(now, num)

        with open(os.path.join(DATA_CHECK_PATH[self.block], name), 'w', encoding="utf-8") as f:
            f.write(json.dumps(res, ensure_ascii=False, indent=4))

    def is_complete_for_yesterday(self, cur):
        """
        昨日数据是否完整
        :param cur: 数据库游标
        :return: True（是） or False（否）
        """
        day = datetime.today() - timedelta(days=1)
        day_str = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
        day_start, day_end = "{} 00:00".format(day_str), "{} 23:59".format(day_str)
        complete_date = [
            datetime(year=day.year, month=day.month, day=day.day, hour=i, minute=j) for i in range(24) for j in
            [0, 30]
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

    @staticmethod
    def get_update_items(items, notify, contrast_item):
        """
        需要更新的数据
        :param items: 数据集
        :param notify: 需要邮件提示的日期字符串
        :param contrast_item: 数据库最新数据
        :return: 待更新的数据集，提示字符串
        """
        res = []
        for item in items:
            if item[0] > contrast_item:
                res.append(item)
        if res:
            notify = datetime.strftime(res[-1][0], "%Y-%m-%d")
        return res, notify

    def store_data_to_db(self, items, conn, cur):
        """
        存储数据至数据库
        :param items: 数据集
        :param conn: 数据库连接
        :param cur: 游标
        """

        if items:
            # 数据检查
            self.data_check(items)

            sql = "insert into {}(time, temp, humidity) values (%s, %s, %s)".format(self.conn_conf["table"])
            cur.executemany(sql, items)
            conn.commit()
            start, end = items[0][0], items[-1][0]

            logging.info("数据更新 {} - {}".format(
                convert_datetime_to_str_by_step(start, "m"),
                convert_datetime_to_str_by_step(end, "m"),
            ))
        else:
            now = datetime.today()
            logging.info("无新增数据：{}".format(convert_datetime_to_str_by_step(now, "m")))

    @property
    def yesterday_status(self):
        with open(SIG_FILE[self.block]) as f:
            if "0" in f.read():
                return False
            return True

    @yesterday_status.setter
    def yesterday_status(self, value):
        with open(SIG_FILE[self.block], "w") as f:
            f.write(str(value))

    def update_real_time_data(self):
        # 更新每日实时数据

        logging.debug("实时数据更新 - 当前执行时间：{}".format(datetime.today().strftime("%Y-%m-%d %H:%M")))
        conn, cur = get_conn(self.conn_conf)
        try:

            # new_items: 新增数据 notify：需要发送邮件通知的日期字符串，空字符串则默认不用通知
            new_items, notify, today = [], "", datetime.today()

            latest_data = self.latest_data(cur)
            latest_date = latest_data[0]
            start_date = datetime(latest_date.year, latest_date.month, latest_date.day)

            while start_date <= today:
                prev_success, prev_items = self.get_data_by_date(start_date)
                if prev_success:
                    new_items.extend(prev_items)
                    start_date += timedelta(days=1)
                else:
                    send_msg(
                        "实时数据获取失败， 获取数据日期：{}".format(
                            convert_datetime_to_str_by_step(start_date, "s"),
                        )
                    )
                    logging.error(
                        "实时数据获取失败， 获取数据日期：{}".format(
                            convert_datetime_to_str_by_step(start_date, "s"),
                        )
                    )
                    break

            new_items, notify = self.get_update_items(new_items, notify, latest_date)

            if not new_items:
                logging.info("无新增数据， 获取数据日期：{}".format(convert_datetime_to_str_by_step(start_date, "s")))

            else:
                self.store_data_to_db(new_items, conn, cur)

                if notify:
                    logging.info("成功更新数据， 获取数据日期：{}".format(convert_datetime_to_str_by_step(datetime.now(), "s")))

                    if datetime.now().hour == 3:
                        if not os.path.exists(SIG_FILE[self.block]):

                            os.makedirs(SIG_FILE[self.block].split("/")[0])
                            open(SIG_FILE[self.block], "w").write("0")

                        if self.is_complete_for_yesterday(cur):
                            self.yesterday_status = 1
                            send_msg("INFO：前日({})数据更新完成".format(
                               convert_datetime_to_str_by_step(datetime.now() - timedelta(days=1), "m"))
                            )

                    if datetime.now().hour == 5:

                        if self.yesterday_status:
                            self.yesterday_status = 0
                        else:
                            send_msg("WARNING：前日({})数据更新异常".format(
                                convert_datetime_to_str_by_step(datetime.now() - timedelta(days=1), "m"))
                            )

        except Exception as e:
            program_debug(self.is_debug)
            now = datetime.today()
            logging.error("实时数据更新异常：{}, Error: {}".format(convert_datetime_to_str_by_step(now, "m"), e))

        finally:
            logging.info("===============     End 数据检查完成 {}     ===============\n".format(
                convert_datetime_to_str_by_step(datetime.now(), "s"))
            )
            logging.info("*" * 100)
            cur.close()
            conn.close()

    def update_history_data(self):
        # 更新历史数据
        conn, cur = get_conn(self.conn_conf)
        init_start = convert_str_to_datetime(self.init_start)
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
                    send_msg(
                        "历史数据更新失败， 日期：{}, 数据截止日期：{}".format(
                            convert_datetime_to_str_by_step(today_obj, "s"),
                            convert_datetime_to_str_by_step(start, "d")
                        )
                    )
                    logging.error(
                        "历史数据更新失败，日期：{}, 数据获取截止日期：{}".format(
                            convert_datetime_to_str_by_step(today_obj, "s"),
                            convert_datetime_to_str_by_step(start, "d")
                        )
                    )
                    exit()
            self.store_data_to_db(new_items, conn, cur)
            send_msg("历史数据更新完成，数据时间范围：{} - {}".format(
                convert_datetime_to_str_by_step(init_start, "d"),
                convert_datetime_to_str_by_step(start, "d")
            ))
        except Exception as e:
            now = datetime.today()
            logging.error(
                "历史数据更新异常：{}, Error: {}".format(convert_datetime_to_str_by_step(now, "m"), e))
        finally:
            logging.info("===============     Start 数据检查完成 {}     ===============\n".format(
                convert_datetime_to_str_by_step(datetime.today(), "s"))
            )
            logging.info("*" * 100)
            cur.close()
            conn.close()

    @staticmethod
    def get_data_by_date(date_obj):
        """获取某天的数据
        :param date_obj: datetime日期对象
        :return: 元祖，第一项为正常True或者异常False，第二项为完整数据
        """
        time.sleep(1)
        date_str = convert_datetime_to_str(date_obj)
        res = []
        url = "https://api.weather.com/v1/location/ZBTJ:9:CN/observations/historical.json?apiKey=e1f10a1e78da46f5b10a1e78da96f525&units=e&startDate={0}&endDate={0}".format(date_str)
        try:
            r = get_response(url)
            items = json.loads(r)
            for item in items["observations"]:
                gmt = time.localtime(item["valid_time_gmt"])
                hour = gmt.tm_hour
                minute = gmt.tm_min
                second = gmt.tm_sec
                res.append(
                    [
                        datetime(date_obj.year, date_obj.month, date_obj.day, hour, minute, second), item["temp"],
                        item["rh"]
                    ]
                )
            return True, res
        except Exception as e:
            logging.error(
                "数据解析失败， 错误原因：{} 时间：{}".format(e, convert_datetime_to_str_by_step(datetime.today(), "s"))
            )
            return False, res


# Weather().update_history_data()
Weather().update_real_time_data()
