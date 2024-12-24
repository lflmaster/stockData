import os
import time
import sqlite3
import numpy as np
import pandas as pd
from . import config
from threading import Thread


def get_stock_code():
    # 连接到SQLite数据库，数据库文件是 stock.db，如果文件不存在，会自动在当前目录创建:
    conn = sqlite3.connect(config.data_dir + '/' + 'stock.db')
    # 创建一个Cursor:
    cursor = conn.cursor()
    # 执行查询语句:
    cursor.execute('SELECT 股票代码 FROM stock_info WHERE 上市日期!=0')
    # 使用 fetchall() 取出所有结果集:
    results = cursor.fetchall()
    # 关闭Cursor:
    cursor.close()
    # 关闭连接:
    conn.close()
    codes = [row[0] for row in results]
    return codes


def get_stock_data(stock_code, fqt):
    stock_file = ''
    if fqt == 0:
        stock_file = config.dayk0_dir + '/' + stock_code + '.csv'
    # if fqt == 1:
    #     stock_file = config.dayk1_dir + '/' + stock_code + '.csv'
    if fqt == 2:
        stock_file = config.dayk2_dir + '/' + stock_code + '.csv'
    data = pd.read_csv(stock_file)
    return data


def get_stock_index(stock_code, fqt):
    data = get_stock_data(stock_code, fqt)
    df = pd.DataFrame()

    df['交易日期'] = data['交易日期']
    df['MA_5'] = data['收盘价'].rolling(window=5).mean()
    df['MA_10'] = data['收盘价'].rolling(window=10).mean()
    df['MA_15'] = data['收盘价'].rolling(window=15).mean()
    df['MA_20'] = data['收盘价'].rolling(window=20).mean()
    df['MA_30'] = data['收盘价'].rolling(window=30).mean()
    df['MA_60'] = data['收盘价'].rolling(window=60).mean()
    df['MA_120'] = data['收盘价'].rolling(window=120).mean()
    df['MA_250'] = data['收盘价'].rolling(window=250).mean()
    df['MA_377'] = data['收盘价'].rolling(window=377).mean()

    # 计算EMA(12)和EMA(16)
    data['EMA12'] = data['收盘价'].ewm(alpha=2 / 13, adjust=False).mean()
    data['EMA26'] = data['收盘价'].ewm(alpha=2 / 27, adjust=False).mean()
    # 计算DIFF、DEA、MACD
    df['MACD_DIFF'] = data['EMA12'] - data['EMA26']
    df['MACD_DEA'] = df['MACD_DIFF'].ewm(alpha=2 / 10, adjust=False).mean()
    df['MACD_M'] = 2 * (df['MACD_DIFF'] - df['MACD_DEA'])

    lowList = data['最低价'].rolling(9).min()
    lowList.fillna(value=data['最低价'].expanding().min(), inplace=True)
    highList = data['最高价'].rolling(9).max()
    highList.fillna(value=data['最高价'].expanding().max(), inplace=True)
    # 计算rsv
    rsv = (data['收盘价'] - lowList) / (highList - lowList) * 100
    # 计算k,d,j
    df['KDJ_K'] = rsv.ewm(alpha=1 / 3, adjust=False).mean()  # ewm是指数加权函数
    df['KDJ_D'] = df['KDJ_K'].ewm(alpha=1 / 3, adjust=False).mean()
    df['KDJ_J'] = 3.0 * df['KDJ_K'] - 2.0 * df['KDJ_D']

    data['MA1'] = data['收盘价'].rolling(10).mean()
    data['MA2'] = data['收盘价'].rolling(50).mean()
    df['DMA_DIF'] = data['MA1'] - data['MA2']
    df['DMA_AMA'] = df['DMA_DIF'].rolling(6).mean()

    df['BIAS1'] = (data['收盘价'] - data['收盘价'].rolling(6).mean()) / data['收盘价'].rolling(6).mean() * 100
    df['BIAS2'] = (data['收盘价'] - data['收盘价'].rolling(12).mean()) / data['收盘价'].rolling(12).mean() * 100
    df['BIAS3'] = (data['收盘价'] - data['收盘价'].rolling(24).mean()) / data['收盘价'].rolling(24).mean() * 100

    df['BOLL_MID'] = data['收盘价'].rolling(20).mean()
    df['BOLL_UP'] = df['BOLL_MID'] + 2 * data['收盘价'].rolling(20).std()
    df['BOLL_LOW'] = df['BOLL_MID'] - 2 * data['收盘价'].rolling(20).std()

    data.loc[0, '涨跌幅'] = 0  # 如果是首日，change记为0
    data['x'] = data['涨跌幅'].apply(lambda x: max(x, 0))  # 涨跌幅<0换为0
    df['RSI1'] = data['x'].ewm(alpha=1 / 6, adjust=False).mean() / (
        np.abs(data['涨跌幅']).ewm(alpha=1 / 6, adjust=False).mean()) * 100
    df['RSI2'] = data['x'].ewm(alpha=1 / 12, adjust=False).mean() / (
        np.abs(data['涨跌幅']).ewm(alpha=1 / 12, adjust=False).mean()) * 100
    df['RSI3'] = data['x'].ewm(alpha=1 / 24, adjust=False).mean() / (
        np.abs(data['涨跌幅']).ewm(alpha=1 / 24, adjust=False).mean()) * 100

    # 计算指标
    df['WR1'] = 100 * (data['最高价'].rolling(10).max() - data['收盘价']) / (
                data['最高价'].rolling(10).max() - data['最低价'].rolling(10).min())
    df['WR2'] = 100 * (data['最高价'].rolling(6).max() - data['收盘价']) / (
                data['最高价'].rolling(6).max() - data['最低价'].rolling(6).min())
    # 缺失值填充
    df.fillna({'WR1': 100 * (data['最高价'].expanding().max() - data['收盘价']) / (
                data['最高价'].expanding().max() - data['最低价'].expanding().min())}, inplace=True)
    df.fillna({'WR2': 100 * (data['最高价'].expanding().max() - data['收盘价']) / (
                data['最高价'].expanding().max() - data['最低价'].expanding().min())}, inplace=True)

    df = df.fillna(0)
    df = df.round(2)
    df = df[df['交易日期'] != 0]
    return df


def get_history_dayk0Index(codes):
    for k, v in enumerate(codes):
        kdata = get_stock_index(v, 0)
        kdata.to_csv(config.dayk0Index_dir + "/" + v + ".csv", index=False, encoding='utf-8')
        print(v + " save dayk0 history index success.")


# def get_history_dayk1Index(codes):
#     for k, v in enumerate(codes):
#         kdata = get_stock_index(v, 1)
#         kdata.to_csv(config.dayk1Index_dir + "/" + v + ".csv", index=False, encoding='utf-8')
#         print(v + " save dayk1 history index success.")


def get_history_dayk2Index(codes):
    for k, v in enumerate(codes):
        kdata = get_stock_index(v, 2)
        kdata.to_csv(config.dayk2Index_dir + "/" + v + ".csv", index=False, encoding='utf-8')
        print(v + " save dayk2 history index success.")


def save_history_daykIndex():
    # 连接到SQLite数据库，数据库文件是 stock.db，如果文件不存在，会自动在当前目录创建:
    conn = sqlite3.connect(config.data_dir + '/' + 'stock.db')
    # 创建一个Cursor:
    cursor = conn.cursor()
    # 执行查询语句:
    cursor.execute('SELECT 股票代码 FROM stock_info WHERE 上市日期!=0')
    # 使用 fetchall() 取出所有结果集:
    results = cursor.fetchall()
    # 关闭Cursor:
    cursor.close()
    # 关闭连接:
    conn.close()
    codes = [row[0] for row in results]

    # 获取不复权日线指标数据
    if not os.path.exists(config.dayk0Index_dir):
        os.makedirs(config.dayk0Index_dir)
    # 获取前复权日线指标数据
    # if not os.path.exists(config.dayk1Index_dir):
    #     os.makedirs(config.dayk1Index_dir)
    # 获取后复权日线指标数据
    if not os.path.exists(config.dayk2Index_dir):
        os.makedirs(config.dayk2Index_dir)
    # 创建 Thread 实例
    dayIndex0_t1 = Thread(target=get_history_dayk0Index, args=(codes[:2000],))
    dayIndex0_t2 = Thread(target=get_history_dayk0Index, args=(codes[2000:4000],))
    dayIndex0_t3 = Thread(target=get_history_dayk0Index, args=(codes[4000:],))
    # dayIndex1_t1 = Thread(target=get_history_dayk1Index, args=(codes[:2000],))
    # dayIndex1_t2 = Thread(target=get_history_dayk1Index, args=(codes[2000:4000],))
    # dayIndex1_t3 = Thread(target=get_history_dayk1Index, args=(codes[4000:],))
    dayIndex2_t1 = Thread(target=get_history_dayk2Index, args=(codes[:2000],))
    dayIndex2_t2 = Thread(target=get_history_dayk2Index, args=(codes[2000:4000],))
    dayIndex2_t3 = Thread(target=get_history_dayk2Index, args=(codes[4000:],))
    # 启动线程运行
    dayIndex0_t1.start()
    dayIndex0_t2.start()
    dayIndex0_t3.start()
    # dayIndex1_t1.start()
    # dayIndex1_t2.start()
    # dayIndex1_t3.start()
    dayIndex2_t1.start()
    dayIndex2_t2.start()
    dayIndex2_t3.start()
    # 等待所有线程执行完毕
    dayIndex0_t1.join()
    dayIndex0_t2.join()
    dayIndex0_t3.join()
    # dayIndex1_t1.join()
    # dayIndex1_t2.join()
    # dayIndex1_t3.join()
    dayIndex2_t1.join()
    dayIndex2_t2.join()
    dayIndex2_t3.join()


def save_one_stock_history_daykIndex(stock_code):
    if not os.path.exists(config.dayk0Index_dir):
        os.makedirs(config.dayk0Index_dir)
    kdata = get_stock_index(stock_code, 0)
    kdata.to_csv(config.dayk0Index_dir + "/" + stock_code + ".csv", index=False, encoding='utf-8')
    print(stock_code + " save dayk0 history index success.")
    # if not os.path.exists(config.dayk1Index_dir):
    #     os.makedirs(config.dayk1Index_dir)
    # kdata = get_stock_index(stock_code, 1)
    # kdata.to_csv(config.dayk1Index_dir + "/" + stock_code + ".csv", index=False, encoding='utf-8')
    # print(stock_code + " save dayk1 history index success.")
    if not os.path.exists(config.dayk2Index_dir):
        os.makedirs(config.dayk2Index_dir)
    kdata = get_stock_index(stock_code, 2)
    kdata.to_csv(config.dayk2Index_dir + "/" + stock_code + ".csv", index=False, encoding='utf-8')
    print(stock_code + " save dayk2 history index success.")


if __name__ == '__main__':
    # 记录开始时间
    start_time = time.time()

    # 只运行一次即可获取历史数据，以后每日进行更新
    save_history_daykIndex()

    # save_one_stock_history_daykIndex("600715")

    # 记录结束时间
    end_time = time.time()
    # 计算耗时
    elapsed_time = end_time - start_time
    print(f"The elapsed time is {elapsed_time} seconds.")

