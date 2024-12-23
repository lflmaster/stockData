import csv
import os
from datetime import datetime
from threading import Thread
from get_history_dayk import get_stock_dayk_by_code
from get_stock_info import get_stock_info
import time
import pandas as pd
import sqlite3

import config


def get_dayk0(results, end_data):
    for k, v in enumerate(results):
        # 更新不复权
        file0 = config.dayk0_dir + '/' + v[0] + '.csv'
        # 检查CSV文件是否存在
        if not os.path.exists(file0):
            # 如果文件不存在，则创建一个新的CSV文件
            with open(file0, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                # 写入标题（如果需要的话）
                writer.writerow(['交易日期', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率'])
                print("Created a new CSV file: " + v[0] + ".csv On dayk0 dir")
        with open(file0, 'r', encoding="utf-8") as file:
            reader = csv.reader(file)
            rows = list(reader)
        if len(rows) > 1:
            file_last_trade_date_ = rows[-1][0]
            file_last_trade_date = ''.join(file_last_trade_date_.split('-'))
            kdata = get_stock_dayk_by_code(code=v[0], begin=file_last_trade_date, end=end_data, fqt=0)
            df = pd.read_csv(file0)[:-1]
            df = pd.concat([df, kdata])
            df.to_csv(file0, index=False, encoding='utf-8')
            print(v[0] + " update dayk0 success.")
            continue
        kdata = get_stock_dayk_by_code(code=v[0], begin=v[1], end=end_data, fqt=0)
        kdata.to_csv(file0, index=False, encoding='utf-8')
        print(v[0] + " update dayk0 success.")


# def get_dayk1(results, end_data):
#     for k, v in enumerate(results):
#         # 更新前复权
#         file0 = config.dayk1_dir + '/' + v[0] + '.csv'
#         # 检查CSV文件是否存在
#         if not os.path.exists(file0):
#             # 如果文件不存在，则创建一个新的CSV文件
#             with open(file0, mode='w', newline='', encoding='utf-8') as file:
#                 writer = csv.writer(file)
#                 # 写入标题（如果需要的话）
#                 writer.writerow(['交易日期', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率'])
#                 print("Created a new CSV file: " + v[0] + ".csv On dayk1 dir")
#         with open(file0, 'r', encoding="utf-8") as file:
#             reader = csv.reader(file)
#             rows = list(reader)
#         if len(rows) > 1:
#             file_last_trade_date_ = rows[-1][0]
#             file_last_trade_date = ''.join(file_last_trade_date_.split('-'))
#             kdata = get_stock_dayk_by_code(code=v[0], begin=file_last_trade_date, end=end_data, fqt=0)
#             df = pd.read_csv(file0)[:-1]
#             df = pd.concat([df, kdata])
#             df.to_csv(file0, index=False, encoding='utf-8')
#             print(v[0] + " update dayk1 success.")
#             continue
#         kdata = get_stock_dayk_by_code(code=v[0], begin=v[1], end=end_data, fqt=0)
#         kdata.to_csv(file0, index=False, encoding='utf-8')
#         print(v[0] + " update dayk1 success.")


def get_dayk2(results, end_data):
    for k, v in enumerate(results):
        # 更新不复权
        file0 = config.dayk2_dir + '/' + v[0] + '.csv'
        # 检查CSV文件是否存在
        if not os.path.exists(file0):
            # 如果文件不存在，则创建一个新的CSV文件
            with open(file0, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                # 写入标题（如果需要的话）
                writer.writerow(['交易日期', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率'])
                print("Created a new CSV file: " + v[0] + ".csv On dayk2 dir")
        with open(file0, 'r', encoding="utf-8") as file:
            reader = csv.reader(file)
            rows = list(reader)
        if len(rows) > 1:
            file_last_trade_date_ = rows[-1][0]
            file_last_trade_date = ''.join(file_last_trade_date_.split('-'))
            kdata = get_stock_dayk_by_code(code=v[0], begin=file_last_trade_date, end=end_data, fqt=0)
            df = pd.read_csv(file0)[:-1]
            df = pd.concat([df, kdata])
            df.to_csv(file0, index=False, encoding='utf-8')
            print(v[0] + " update dayk2 success.")
            continue
        kdata = get_stock_dayk_by_code(code=v[0], begin=v[1], end=end_data, fqt=0)
        kdata.to_csv(file0, index=False, encoding='utf-8')
        print(v[0] + " update dayk2 success.")


def update_check_dayk():
    # 获取当前日期
    current_date = datetime.now().strftime('%Y%m%d')
    # 连接到SQLite数据库，数据库文件是 stock.db，如果文件不存在，会自动在当前目录创建:
    conn = sqlite3.connect(config.data_dir + '/' + 'stock.db')
    # 创建一个Cursor:
    cursor = conn.cursor()
    # 执行查询语句:
    cursor.execute('SELECT 股票代码,上市日期 FROM check_date WHERE 数据状态 = -1')
    # 使用 fetchall() 取出所有结果集:
    results = cursor.fetchall()
    # 关闭Cursor:
    cursor.close()
    # 关闭连接:
    conn.close()
    # 创建 Thread 实例
    dayk0 = Thread(target=get_dayk0, args=(results, current_date))
    # dayk1 = Thread(target=get_dayk1, args=(results, current_date))
    dayk2 = Thread(target=get_dayk2, args=(results, current_date))
    # 启动线程运行
    dayk0.start()
    # dayk1.start()
    dayk2.start()
    # 等待所有线程执行完毕
    dayk0.join()
    # dayk1.join()
    dayk2.join()


def update_dayk():
    # 获取当前日期
    current_date = datetime.now().strftime('%Y%m%d')
    # 连接到SQLite数据库，数据库文件是 stock.db，如果文件不存在，会自动在当前目录创建:
    conn = sqlite3.connect(config.data_dir + '/' + 'stock.db')
    # 创建一个Cursor:
    cursor = conn.cursor()
    # 执行查询语句:
    cursor.execute('SELECT 股票代码,上市日期 FROM check_date')
    # 使用 fetchall() 取出所有结果集:
    results = cursor.fetchall()
    # 关闭Cursor:
    cursor.close()
    # 关闭连接:
    conn.close()
    # 创建 Thread 实例
    dayk0_t1 = Thread(target=get_dayk0, args=(results[:1000], current_date))
    dayk0_t2 = Thread(target=get_dayk0, args=(results[1000:2000], current_date))
    dayk0_t3 = Thread(target=get_dayk0, args=(results[2000:3000], current_date))
    dayk0_t4 = Thread(target=get_dayk0, args=(results[3000:4000], current_date))
    dayk0_t5 = Thread(target=get_dayk0, args=(results[4000:5000], current_date))
    dayk0_t6 = Thread(target=get_dayk0, args=(results[5000:], current_date))
    # dayk1_t1 = Thread(target=get_dayk1, args=(results[:1000], current_date))
    # dayk1_t2 = Thread(target=get_dayk1, args=(results[1000:2000], current_date))
    # dayk1_t3 = Thread(target=get_dayk1, args=(results[2000:3000], current_date))
    # dayk1_t4 = Thread(target=get_dayk1, args=(results[3000:4000], current_date))
    # dayk1_t5 = Thread(target=get_dayk1, args=(results[4000:5000], current_date))
    # dayk1_t6 = Thread(target=get_dayk1, args=(results[5000:], current_date))
    dayk2_t1 = Thread(target=get_dayk2, args=(results[:1000], current_date))
    dayk2_t2 = Thread(target=get_dayk2, args=(results[1000:2000], current_date))
    dayk2_t3 = Thread(target=get_dayk2, args=(results[2000:3000], current_date))
    dayk2_t4 = Thread(target=get_dayk2, args=(results[3000:4000], current_date))
    dayk2_t5 = Thread(target=get_dayk2, args=(results[4000:5000], current_date))
    dayk2_t6 = Thread(target=get_dayk2, args=(results[5000:], current_date))
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


def update_dayk2():
    # 获取当前日期
    current_date = datetime.now().strftime('%Y%m%d')
    # 连接到SQLite数据库，数据库文件是 stock.db，如果文件不存在，会自动在当前目录创建:
    conn = sqlite3.connect(config.data_dir + '/' + 'stock.db')
    # 创建一个Cursor:
    cursor = conn.cursor()
    # 执行查询语句:
    cursor.execute('SELECT 股票代码,上市日期 FROM check_date')
    # 使用 fetchall() 取出所有结果集:
    results = cursor.fetchall()
    # 关闭Cursor:
    cursor.close()
    # 关闭连接:
    conn.close()
    # 创建 Thread 实例
    dayk2_t1 = Thread(target=get_dayk2, args=(results[:1000], current_date))
    dayk2_t2 = Thread(target=get_dayk2, args=(results[1000:2000], current_date))
    dayk2_t3 = Thread(target=get_dayk2, args=(results[2000:3000], current_date))
    dayk2_t4 = Thread(target=get_dayk2, args=(results[3000:4000], current_date))
    dayk2_t5 = Thread(target=get_dayk2, args=(results[4000:5000], current_date))
    dayk2_t6 = Thread(target=get_dayk2, args=(results[5000:], current_date))
    # 启动线程运行
    dayk2_t1.start()
    dayk2_t2.start()
    dayk2_t3.start()
    dayk2_t4.start()
    dayk2_t5.start()
    dayk2_t6.start()
    # 等待所有线程执行完毕
    dayk2_t1.join()
    dayk2_t2.join()
    dayk2_t3.join()
    dayk2_t4.join()
    dayk2_t5.join()
    dayk2_t6.join()


if __name__ == '__main__':
    # 记录开始时间
    start_time = time.time()

    # 获取股票信息
    get_stock_info()

    # update_check_dayk()
    update_dayk()
    # update_dayk2()

    # 记录结束时间
    end_time = time.time()
    # 计算耗时
    elapsed_time = end_time - start_time
    print(f"The elapsed time is {elapsed_time} seconds.")
