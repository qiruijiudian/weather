# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/4/26 10:21
# @Author  : MAYA

import platform
import logging
import os
import json
from datetime import datetime
from lxml import etree
from dateutil.relativedelta import relativedelta

from tools import get_conf, get_conn, program_debug, get_response, send_msg

if platform.system() == "Windows":
    log_file = "./log/cona/logs.txt"
else:
    log_file = "/home/weather/log/cona/logs.txt"


logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
                    filename=log_file,
                    filemode='a+',
                    format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')


class Weather:
    def __init__(self):
        # self.init_start = "202012"
        self.init_start = "202201"
        self.block = "cona"
        self.conn_conf = get_conf(self.block)
        self.is_debug = False
        logging.info("*" * 100)
        logging.info(
            "===============     Start 数据检查开始 {}     ===============\n".format(
                datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            )
        )

    def latest_data(self, cur):
        """返回数据库中最新的一条数据
        :param cur: 数据库游标
        :return: 组新的一条数据
        """
        cur.execute("select time, temp from {} order by time desc limit 1".format(self.conn_conf["table"]))
        return cur.fetchone()

    def store_data_to_db(self, items, conn, cur):

        if items:

            sql = "insert into {}(time, temp) values (%s, %s)".format(self.conn_conf["table"])
            cur.executemany(sql, items)
            conn.commit()
            start, end = items[0][0], items[-1][0]
            if start != end:
                logging.info("数据更新 {} - {}".format(
                    start.strftime("%Y-%m-%d"),
                    end.strftime("%Y-%m-%d")
                ))
            return True
        else:
            now = datetime.today()
            logging.info("无新增数据：{}".format(now.strftime("%Y-%m-%d %H:%M:%S")))
            return False

    def update_real_time_data(self):
        """更新每日实时数据
        :return:
        """
        logging.debug("实时数据更新 - 当前执行时间：{}".format(datetime.today().strftime("%Y-%m-%d %H:%M")))
        conn, cur = get_conn(self.conn_conf)

        # new_items: 新增数据 notify：需要发送邮件通知的日期字符串，空字符串则默认不用通知
        new_items, notify, today = [], "", datetime.today()
        try:
            latest_data = self.latest_data(cur)
            start_date = latest_data[0]
            while start_date <= today:
                success, items = self.get_data_by_date(start_date)
                if success:
                    new_items.extend([item for item in items if item[0] > latest_data[0]])
                    if start_date == today:
                        break
                    start_date += relativedelta(months=1)
                else:
                    send_msg(
                        "实时数据更新失败， 当前日期：{}, 数据获取截止日期：{}".format(
                            today.strftime("%Y-%m-%d %H:%M:%S"),
                            start_date.strftime("%Y-%m")
                        )
                    )
                    logging.error(
                        "历史数据更新失败，日期：{}, 数据获取截止日期：{}".format(
                            today.strftime("%Y-%m-%d %H:%M:%S"),
                            start_date.strftime("%Y-%m")
                        )
                    )
                    break

            if not new_items:
                logging.debug("无新增数据 {} ".format(today.strftime("%Y-%m-%d")))
            else:
                notify = self.store_data_to_db(new_items, conn, cur)

            if notify:
                send_msg("错那温度 - 实时数据更新完毕，数据库当前最新时间： {}".format(new_items[-1][0].strftime("%Y-%m-%d")))

        except Exception as e:
            program_debug(self.is_debug)
            logging.error("实时数据更新异常：{}, Error: {}".format(today.strftime("%Y-%m-%d"), e))

        finally:
            logging.info("===============     End 数据检查完成 {}     ===============\n".format(
                today.strftime("%Y-%m-%d"))
            )
            logging.info("*" * 100 + "\n")
            cur.close()
            conn.close()

    @staticmethod
    def data_check(items, date_obj):
        """数据检查
        :param items: 数据集合
        :param date_obj: 日期对象（哪一个月的数据）
        """
        start_obj, end_obj = items[0][0], items[-1][0]
        start, end = start_obj.strftime("%Y-%m-%d"), end_obj.strftime("%Y-%m-%d")
        start_value, end_value = items[0][1], items[-1][1]
        days_items = [[item[0].strftime("%Y-%m-%d"), item[1]] for item in items]

        if platform.system() == "Windows":
            dir_path = "./data_check/cona"
        else:
            dir_path = "/home/weather/data_check/cona"

        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        name, num = "{}.json".format(date_obj.strftime("%Y%m")), 1
        while os.path.exists(os.path.join(dir_path, name)):
            num += 1
            name = "{}({}).json".format(start_obj.strftime("%Y%m"), num)

        res = {
            "execute_time": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
            "start": {
                "time": start,
                "value": start_value
            },
            "end": {
                "time": end,
                "value": end_value
            },
            "items": days_items
        }

        with open(os.path.join(dir_path, name), "w", encoding="utf-8") as f:
            f.write(json.dumps(res, ensure_ascii=False, indent=4))

    def update_history_data(self):
        logging.info("历史数据更新 - 当前执行时间：{}".format(datetime.today().strftime("%Y-%m-%d %H:%M")))
        conn, cur = get_conn(self.conn_conf)
        start = datetime.strptime(self.init_start, "%Y%m")
        today = datetime.today()
        new_items = []
        try:
            while start <= today:
                success, items = self.get_data_by_date(start)
                if success:
                    new_items.extend(items)
                    if start == today:
                        break
                    start += relativedelta(months=1)
                else:
                    send_msg(
                        "历史数据更新失败， 日期：{}, 数据获取截止日期：{}".format(
                            today.strftime("%Y-%m-%d %H:%M:%S"),
                            start.strftime("%Y-%m")
                        )
                    )
                    logging.error(
                        "历史数据更新失败，日期：{}, 数据获取截止日期：{}".format(
                            today.strftime("%Y-%m-%d %H:%M:%S"),
                            start.strftime("%Y-%m")
                        )
                    )
                    exit()
            notify = self.store_data_to_db(new_items, conn, cur)

            if notify:
                update_start, update_end = new_items[0][0], new_items[-1][0]

                send_msg("错那温度 - 历史数据更新完成，数据时间范围：{} - {}".format(
                    update_start.strftime("%Y-%m-%d"),
                    update_end.strftime("%Y-%m-%d")
                ))
        except Exception as e:
            program_debug(self.is_debug)
            logging.error("历史数据更新异常：{}, Error: {}".format(today.strftime("%Y-%m-%d %H:%M:%S"), e))
        finally:
            logging.info("===============     Start 数据检查完成 {}     ===============\n".format(
                today.strftime("%Y-%m-%d %H:%M:%S"))
            )
            logging.info("*" * 100 + "\n")
            cur.close()
            conn.close()

    def get_data_by_date(self, date_obj):
        """获取某天的数据
        :param date_obj: datetime日期对象
        :return: 元祖，第一项为正常True或者异常False，第二项为完整数据
        """

        date_str = date_obj.strftime("%Y%m")
        res = []

        url = "http://www.tianqihoubao.com/lishi/cuona/month/{}.html".format(date_str)
        try:
            response = get_response(url)
            html = etree.HTML(response)
            trs = html.xpath("//table//tr")
            for tr in trs:
                temps = tr.xpath("./td[3]/text()")
                dates = tr.xpath("./td[1]/a/text()")
                if temps and "/" in temps[0] and dates:
                    temps = temps[0].replace("\r\n", "").replace("\r", "").replace("\n", "").strip().split("/")
                    high = float(temps[0].strip().rstrip("℃").strip())
                    low = float(temps[1].strip().rstrip("℃").strip())
                    temp = (high + low) / 2
                    date = datetime.strptime(dates[0].strip(), "%Y年%m月%d日")
                    res.append([date, temp])
            # 数据检查
            self.data_check(res, date_obj)
            return True, res
        except Exception as e:
            logging.error("数据获取失败， 错误原因：{} 时间：{}".format(e, datetime.today().strftime("%Y-%m-%d %H:%M:%S")))
            return False, res


# Weather().update_history_data()
Weather().update_real_time_data()
# Weather().get_data_by_date(datetime.today())
