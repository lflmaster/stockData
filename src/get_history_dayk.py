import os
import re
from datetime import datetime
from threading import Thread
from get_stock_info import get_stock_info
import requests
import random
import time
import urllib
import json
import pandas as pd
import sqlite3
import config


def get_stock_dayk_by_code(code, begin, end, fqt=0):
    data_info = {
        'f51': '交易日期',
        'f52': '开盘价',
        'f53': '收盘价',
        'f54': '最高价',
        'f55': '最低价',
        'f56': '成交量',
        'f57': '成交额',
        'f58': '振幅',
        'f59': '涨跌幅',
        'f60': '涨跌额',
        'f61': '换手率'
    }
    url = 'https://push2his.eastmoney.com/api/qt/stock/kline/get?'
    header = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36 Edg/107.0.1418.62",
        'Cookie': 'qgqp_b_id=e66305de7e730aa89f1c877cc0849ad1; qRecords=%5B%7B%22name%22%3A%22%u6D77%u9E25%u4F4F%u5DE5%22%2C%22code%22%3A%22SZ002084%22%7D%5D; st_pvi=80622161013438; st_sp=2022-09-29%2022%3A47%3A13; st_inirUrl=https%3A%2F%2Fcn.bing.com%2F; HAList=ty-1-000300-%u6CAA%u6DF1300%2Cty-0-002108-%u6CA7%u5DDE%u660E%u73E0%2Cty-1-600455-%u535A%u901A%u80A1%u4EFD%2Cty-0-002246-%u5317%u5316%u80A1%u4EFD',
        'Referer': 'https://data.eastmoney.com/',
        'Host': 'push2his.eastmoney.com'}
    stock_df = pd.DataFrame(columns=data_info)
    # 更换DataFrame的header
    stock_df.rename(columns=data_info, inplace=True)
    # 构建url参数
    jq = re.sub('\D', '', '1.12.3' + str(random.random()))
    tm = int(time.time() * 1000)
    c = 1 if code[0] == '6' else 0
    url_params = {'cb': 'jQuery{}_{}'.format(jq, tm),
              'fields1': urllib.request.unquote('f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6', encoding='utf-8'),
              'fields2': urllib.request.unquote('f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61', encoding='utf-8'),
              'ut': 'b2884a393a59ad64002292a3e90d46a5',
              'klt': '101',  # klt = 1,5,15,20,101,102 分别是1,5,15,20分钟 日,周 默认为101即日k
              'fqt': fqt,  # 0:不复权, 1:前复权, 2:后复权
              'secid': '{}.{}'.format(c, code),
              'beg': begin,
              'end': end,
              '_': '{}'.format(tm)
              }
    # 发送请求
    res = requests.get(url.format(code), headers=header, params=url_params)
    res.encoding = "utf-8"
    # 去除js数据中的无关字符，以便符合json数据格式
    html = res.text.lstrip('jQuery{}_{}'.format(jq, tm) + '(').rstrip(');')
    # 转换为json数据
    js_html = json.loads(html)
    js_data = js_html['data']
    js_klines = js_data['klines']
    for k, v in enumerate(js_klines):
        stock_df.loc[k] = v.split(',')
    return stock_df


def get_history_dayk0(results, end_date):
    codes = [row[0] for row in results]
    datas = [row[1] for row in results]
    for k, v in enumerate(codes):
        if os.path.exists(config.dayk0_dir + "/" + v + ".csv"):
            continue
        kdata = get_stock_dayk_by_code(code=v, begin=datas[k], end=end_date, fqt=0)
        kdata.to_csv(config.dayk0_dir + "/" + v + ".csv", index=False, encoding='utf-8')
        print(v+" save history dayK0 success.")


# def get_history_dayk1(results, end_date):
#     codes = [row[0] for row in results]
#     datas = [row[1] for row in results]
#     for k, v in enumerate(codes):
#         if os.path.exists(config.dayk1_dir + "/" + v + ".csv"):
#             continue
#         kdata = get_stock_dayk_by_code(code=v, begin=datas[k], end=end_date, fqt=1)
#         kdata.to_csv(config.dayk1_dir + "/" + v + ".csv", index=False, encoding='utf-8')
#         print(v+" save history dayK1 success.")


def get_history_dayk2(results, end_date):
    codes = [row[0] for row in results]
    datas = [row[1] for row in results]
    for k, v in enumerate(codes):
        if os.path.exists(config.dayk2_dir + "/" + v + ".csv"):
            continue
        kdata = get_stock_dayk_by_code(code=v, begin=datas[k], end=end_date, fqt=2)
        kdata.to_csv(config.dayk2_dir + "/" + v + ".csv", index=False, encoding='utf-8')
        print(v+" save history dayK2 success.")


def save_history_dayk(end_date):
    # 获取股票代码和上市时间
    # 连接到SQLite数据库，数据库文件是 stock.db，如果文件不存在，会自动在当前目录创建:
    conn = sqlite3.connect(config.data_dir + '/' + 'stock.db')
    # 创建一个Cursor:
    cursor = conn.cursor()
    # 执行查询语句:
    cursor.execute('SELECT 股票代码,上市日期 FROM stock_info WHERE 上市日期!=0')
    # 使用 fetchall() 取出所有结果集:
    results = cursor.fetchall()
    # 关闭Cursor:
    cursor.close()
    # 关闭连接:
    conn.close()
    # print(len(results))

    # 获取不复权日线数据
    if not os.path.exists(config.dayk0_dir):
        os.makedirs(config.dayk0_dir)
    # 获取前复权日线数据
    # if not os.path.exists(config.dayk1_dir):
    #     os.makedirs(config.dayk1_dir)
    # 获取后复权日线数据
    if not os.path.exists(config.dayk2_dir):
        os.makedirs(config.dayk2_dir)
    # 创建 Thread 实例
    dayk0_t1 = Thread(target=get_history_dayk0, args=(results[:1000], end_date))
    dayk0_t2 = Thread(target=get_history_dayk0, args=(results[1000:2000], end_date))
    dayk0_t3 = Thread(target=get_history_dayk0, args=(results[2000:3000], end_date))
    dayk0_t4 = Thread(target=get_history_dayk0, args=(results[3000:4000], end_date))
    dayk0_t5 = Thread(target=get_history_dayk0, args=(results[4000:5000], end_date))
    dayk0_t6 = Thread(target=get_history_dayk0, args=(results[5000:], end_date))
    # dayk1_t1 = Thread(target=get_history_dayk1, args=(results[:1000], end_date))
    # dayk1_t2 = Thread(target=get_history_dayk1, args=(results[1000:2000], end_date))
    # dayk1_t3 = Thread(target=get_history_dayk1, args=(results[2000:3000], end_date))
    # dayk1_t4 = Thread(target=get_history_dayk1, args=(results[3000:4000], end_date))
    # dayk1_t5 = Thread(target=get_history_dayk1, args=(results[4000:5000], end_date))
    # dayk1_t6 = Thread(target=get_history_dayk1, args=(results[5000:], end_date))
    dayk2_t1 = Thread(target=get_history_dayk2, args=(results[:1000], end_date))
    dayk2_t2 = Thread(target=get_history_dayk2, args=(results[1000:2000], end_date))
    dayk2_t3 = Thread(target=get_history_dayk2, args=(results[2000:3000], end_date))
    dayk2_t4 = Thread(target=get_history_dayk2, args=(results[3000:4000], end_date))
    dayk2_t5 = Thread(target=get_history_dayk2, args=(results[4000:5000], end_date))
    dayk2_t6 = Thread(target=get_history_dayk2, args=(results[5000:], end_date))
    # 启动线程运行
    dayk0_t1.start()
    dayk0_t2.start()
    dayk0_t3.start()
    dayk0_t4.start()
    dayk0_t5.start()
    dayk0_t6.start()
    # dayk1_t1.start()
    # dayk1_t2.start()
    # dayk1_t3.start()
    # dayk1_t4.start()
    # dayk1_t5.start()
    # dayk1_t6.start()
    dayk2_t1.start()
    dayk2_t2.start()
    dayk2_t3.start()
    dayk2_t4.start()
    dayk2_t5.start()
    dayk2_t6.start()
    # 等待所有线程执行完毕
    dayk0_t1.join()
    dayk0_t2.join()
    dayk0_t3.join()
    dayk0_t4.join()
    dayk0_t5.join()
    dayk0_t6.join()
    # dayk1_t1.join()
    # dayk1_t2.join()
    # dayk1_t3.join()
    # dayk1_t4.join()
    # dayk1_t5.join()
    # dayk1_t6.join()
    dayk2_t1.join()
    dayk2_t2.join()
    dayk2_t3.join()
    dayk2_t4.join()
    dayk2_t5.join()
    dayk2_t6.join()


def save_one_stock_history(stock_code):
    # 获取股票代码和上市时间
    # 连接到SQLite数据库，数据库文件是 stock.db，如果文件不存在，会自动在当前目录创建:
    conn = sqlite3.connect(config.data_dir + '/' + 'stock.db')
    # 创建一个Cursor:
    cursor = conn.cursor()
    # 执行查询语句:
    cursor.execute('SELECT 上市日期 FROM stock_info WHERE 上市日期!=0 and 股票代码 = ?', (stock_code,))
    # 使用 fetchall() 取出所有结果集:
    results = cursor.fetchall()
    # 关闭Cursor:
    cursor.close()
    # 关闭连接:
    conn.close()
    datas = [row[0] for row in results]
    data_start = ''.join(datas[0].strip().split("-"))
    # 获取当前日期
    data_end = datetime.now().strftime('%Y%m%d')

    if not os.path.exists(config.dayk0_dir):
        os.makedirs(config.dayk0_dir)
    kdata = get_stock_dayk_by_code(code=stock_code, begin=data_start, end=data_end, fqt=0)
    kdata.to_csv(config.dayk0_dir + "/" + stock_code + ".csv", index=False, encoding='utf-8')
    print(stock_code+" save history dayK0 success.")

    # if not os.path.exists(config.dayk1_dir):
    #     os.makedirs(config.dayk1_dir)
    # kdata = get_stock_dayk_by_code(code=stock_code, begin=data_start, end=data_end, fqt=1)
    # kdata.to_csv(config.dayk1_dir + "/" + stock_code + ".csv", index=False, encoding='utf-8')
    # print(stock_code+" save history dayK1 success.")

    if not os.path.exists(config.dayk2_dir):
        os.makedirs(config.dayk2_dir)
    kdata = get_stock_dayk_by_code(code=stock_code, begin=data_start, end=data_end, fqt=2)
    kdata.to_csv(config.dayk2_dir + "/" + stock_code + ".csv", index=False, encoding='utf-8')
    print(stock_code+" save history dayK2 success.")


if __name__ == '__main__':
    # 记录开始时间
    start_time = time.time()

    end_dt = '20241217'

    # 获取股票信息
    get_stock_info()

    # 只运行一次即可获取历史数据，以后每日进行更新
    save_history_dayk(end_dt)  # 没有前复权数据

    # save_one_stock_history("000001")

    # 记录结束时间
    end_time = time.time()
    # 计算耗时
    elapsed_time = end_time - start_time
    print(f"The elapsed time is {elapsed_time} seconds.")
