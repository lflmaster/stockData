import sqlite3
import time
import pandas as pd
from datetime import datetime, timedelta
import exchange_calendars as xcals
from sqlalchemy import create_engine
from . import config


def get_trade_data_day():
    # 获取当前日期
    current_date = datetime.now()
    open_time = datetime.strptime(str(datetime.now().date()) + '09:15', '%Y-%m-%d%H:%M')
    # close_time = datetime.strptime(str(datetime.now().date()) + '15:30', '%Y-%m-%d%H:%M')
    # 获取当前数据的交易日期
    xshg = xcals.get_calendar("XSHG")
    if xshg.is_session(current_date.strftime('%Y-%m-%d')) and current_date > open_time:
        current_date = current_date.strftime('%Y-%m-%d')
        return current_date
    else:
        for i in range(30):
            current_date = (current_date - timedelta(days=1))
            if xshg.is_session(current_date.strftime('%Y-%m-%d')):
                current_date = current_date.strftime('%Y-%m-%d')
                return current_date
            else:
                current_date = (current_date - timedelta(days=1))


def get_index_data(stock_code, fqt=0):
    index_file = ''
    if fqt == 0:
        index_file = config.dayk0Index_dir + '/' + stock_code + '.csv'
    # if fqt == 1:
    #     index_file = config.dayk1Index_dir + '/' + stock_code + '.csv'
    if fqt == 2:
        index_file = config.dayk2Index_dir + '/' + stock_code + '.csv'
    data = pd.read_csv(index_file)
    return data


def get_stock_data(stock_code, fqt=0):
    stock_file = ''
    if fqt == 0:
        stock_file = config.dayk0_dir + '/' + stock_code + '.csv'
    # if fqt == 1:
    #     stock_file = config.dayk1_dir + '/' + stock_code + '.csv'
    if fqt == 2:
        stock_file = config.dayk2_dir + '/' + stock_code + '.csv'
    data = pd.read_csv(stock_file)
    return data


def get_stock_code():
    # 连接到SQLite数据库，数据库文件是 stock.db，如果文件不存在，会自动在当前目录创建:
    conn = sqlite3.connect(config.data_dir + '/' + 'stock.db')
    # 创建一个Cursor:
    cursor = conn.cursor()
    # 执行查询语句:
    cursor.execute('SELECT 股票代码,股票名称 FROM select_1')
    # 使用 fetchall() 取出所有结果集:
    results = cursor.fetchall()
    # 关闭Cursor:
    cursor.close()
    # 关闭连接:
    conn.close()
    return results


# 策略1：基本面策略
def select_1():
    last_trade_date = get_trade_data_day()
    file = config.stock_info_dir + '/' + last_trade_date + '.csv'
    data = pd.read_csv(file, dtype={'股票代码': str})

    df = pd.DataFrame()
    df['股票代码'] = data['股票代码']
    df['股票名称'] = data['股票名称']
    df['上市日期'] = data['上市日期']
    df['最新价'] = data['最新价']
    df['行业板块'] = data['行业板块']
    df['地区板块'] = data['地区板块']
    df['概念题材'] = data['概念题材']

    # 选择A股主板股票
    df['index1'] = data['股票代码'].str.startswith('0') | data['股票代码'].str.startswith('60') | data['股票代码'].str.startswith('30')

    # 剔除包含ST的股票
    df['index2'] = ~data['股票名称'].str.contains('ST')

    # 总市值 15亿到200亿
    df['index3'] = data['总市值'].between(15*100000000, 200*100000000)

    # 流通市值 15亿到200亿
    df['index4'] = data['流通市值'].between(15*100000000, 200*100000000)

    # 最新价 2-10元
    df['index5'] = data['最新价'].between(2, 10)

    df['index'] = df['index1'] & df['index2'] & df['index3'] & df['index4'] & df['index5']
    df = df[df['index']]
    del df['index'], df['index1'], df['index2'], df['index3'], df['index4'], df['index5']

    df.to_csv(config.data_dir + "/" + "select_1.csv", index=False, encoding='utf-8')

    # 创建SQLite引擎，指定数据库文件名
    engine = create_engine('sqlite:///' + config.data_dir + '/stock.db')
    # 将DataFrame保存到SQLite数据库中，表名为'check_date'
    df.to_sql('select_1', con=engine, if_exists='replace', index=False)


def select_index(stock_code):
    data1 = get_stock_data(stock_code, fqt=2).tail(10)
    data2 = get_index_data(stock_code, fqt=2).tail(10)

    if len(data1) == 0 or len(data2) == 0:
        return 0

    df = pd.DataFrame()
    df['交易日期'] = data1['交易日期']
    df['收盘价'] = data1['收盘价']

    # 计算根据指标计算自定义策略指标

    # index1：开盘价、收盘价
    df['index1_1'] = data1['开盘价'] >= data1['开盘价'].shift(1)
    df['index1_2'] = data1['开盘价'] >= data2['MA_5'].shift(1)
    df['index1_3'] = data1['收盘价'] >= data1['收盘价'].shift(1)
    df['index1_4'] = data1['收盘价'] >= data2['MA_5']
    df['index1'] = df['index1_1'] & df['index1_2'] & df['index1_3'] & df['index1_4']
    del df['index1_1'], df['index1_2'], df['index1_3'], df['index1_4']

    # index2：最高价、最低价
    df['index2_1'] = data1['最高价'] >= data1['最高价'].shift(1)
    df['index2_2'] = data1['最高价'] >= data2['MA_5']
    df['index2_3'] = data1['最低价'] >= data1['最低价'].shift(1)
    df['index2_4'] = data1['最低价'] >= data2['MA_5'].shift(1)
    df['index2'] = df['index2_1'] & df['index2_2'] & df['index2_3'] & df['index2_4']
    del df['index2_1'], df['index2_2'], df['index2_3'], df['index2_4']

    # index2：最低价在日线MA30附近
    # df['index2'] = (data1['最低价'] - data2['MA_30']).abs() < 0.05
    # index1: 开盘价, 收盘价, 最高价, 最低价 与 日线MA5的距离之和的平均值
    # df['index1'] = data1[['开盘价', '收盘价', '最高价', '最低价']].sub(data2['MA_5'], axis=0).abs().mean(axis=1)
    # index1：收盘价日线M5、日线M10的关系
    # df['index1_1'] = data1['收盘价'] >= data2['MA_10']
    # df['index1_2'] = data1['收盘价'] <= data2['MA_5']
    # df['index1'] = df['index1_1'] & df['index1_2']
    # del df['index1_1'], df['index1_2']
    #
    # # index2: 比较日线均线 5 > 10 > 20 > 30 、 5 < 60 < 120 、 120 > 250
    # df['index2_1'] = data2['MA_5'] > data2['MA_10']
    # df['index2_2'] = data2['MA_10'] > data2['MA_30']
    # df['index2_3'] = data2['MA_20'] > data2['MA_30']
    # df['index2_4'] = data2['MA_5'] < data2['MA_60']
    # df['index2_5'] = data2['MA_60'] < data2['MA_120']
    # df['index2_6'] = data2['MA_120'] > data2['MA_250']
    # # df['index2'] = df['index2_1'] & df['index2_2'] & df['index2_3'] & df['index2_4'] & df['index2_5'] & df['index2_6']
    # # df['index2'] = df['index2_1'] & df['index2_2']
    # df['index2'] = df['index2_1'] & df['index2_2'] & df['index2_3']
    # del df['index2_1'], df['index2_2'], df['index2_3'], df['index2_4'], df['index2_5'], df['index2_6']
    # # df = df.replace({True: 1, False: 0})
    # # df['index2'] = df[['index2_1', 'index2_2', 'index2_3', 'index2_4', 'index2_5', 'index2_6']].sum(axis=1)
    #
    # # index3：最近3天的收盘价、日线m5、日线m10的价格浮动程度
    # df['close_tail3_mean'] = (data1['收盘价'] + data1['收盘价'].shift(1) + data1['收盘价'].shift(2)) / 3
    # df['index3_close'] = ((data1['收盘价'] - df['close_tail3_mean']).abs() +
    #                       (data1['收盘价'].shift(1) - df['close_tail3_mean']).abs() +
    #                       (data1['收盘价'].shift(2) - df['close_tail3_mean']).abs()) / 3
    # df['ma5_tail3_mean'] = (data2['MA_5'] + data2['MA_5'].shift(1) + data2['MA_5'].shift(2)) / 3
    # df['index3_M5'] = ((data2['MA_5'] - df['ma5_tail3_mean']).abs() +
    #                    (data2['MA_5'].shift(1) - df['ma5_tail3_mean']).abs() +
    #                    (data2['MA_5'].shift(2) - df['ma5_tail3_mean']).abs()) / 3
    # df['ma10_tail3_mean'] = (data2['MA_10'] + data2['MA_10'].shift(1) + data2['MA_10'].shift(2)) / 3
    # df['index3_M10'] = ((data2['MA_10'] - df['ma10_tail3_mean']).abs() +
    #                     (data2['MA_10'].shift(1) - df['ma10_tail3_mean']).abs() +
    #                     (data2['MA_10'].shift(2) - df['ma10_tail3_mean']).abs()) / 3
    # df['index3'] = df[['index3_close', 'index3_M5', 'index3_M10']].sum(axis=1) < 0.02
    # del df['close_tail3_mean'], df['index3_close'], df['ma5_tail3_mean'], df['index3_M5'], df['ma10_tail3_mean'], df['index3_M10']
    #
    # # index4: macd 指标
    # df['index4_1'] = data2['MACD_M'] >= data2['MACD_M'].shift(1)
    # df['index4_2'] = data2['MACD_DIFF'] >= data2['MACD_DIFF'].shift(1)
    # df['index4_3'] = data2['MACD_DEA'] >= data2['MACD_DEA'].shift(1)
    # df['index4_4'] = data2['MACD_DIFF'] >= data2['MACD_DEA']
    # df['index4_5'] = data2['MACD_M'] >= 0
    # df['index4_6'] = data2['MACD_DIFF'] >= 0
    # # df['index4'] = df['index4_1'] & df['index4_2'] & df['index4_3'] & df['index4_4'] & df['index4_5'] & df['index4_6']
    # df['index4'] = df['index4_1'] & df['index4_2'] & df['index4_3'] & df['index4_4'] & df['index4_5']
    # del df['index4_1'], df['index4_2'], df['index4_3'], df['index4_4'], df['index4_5'], df['index4_6']
    # # df['index4'] = df['index4_1'] & df['index4_2'] & df['index4_3'] & df['index4_6']
    # # del df['index4_1'], df['index4_2'], df['index4_3'], df['index4_4'], df['index4_5'], df['index4_6']
    # # df['index4_1'] = data2['MACD_M'] - data2['MACD_M'].shift(1)  # 当天的MACD_M值相对于前一天的变化  一阶导
    # # df['index4_2'] = df['index4_1'] - df['index4_1'].shift(1)  # 当天的MACD_M值相对于前一天的变化程度 二阶导
    # # df['index4_3'] = data2['MACD_M'] - data2['MACD_DIFF']  # 当天的MACD_M值和MACD_DIFF的差值
    # # df['index4_4'] = df['index4_3'] - df['index4_3'].shift(1)  # 当天的MACD_M值和MACD_DIFF的差值的变化程度
    # # df['index4'] = data2[['MACD_DIFF', 'MACD_DEA', 'MACD_M']].abs().mean(axis=1)
    #
    # # index4: jdk 指标
    # df['index5_1'] = data2['KDJ_J'] >= data2['KDJ_J'].shift(1)
    # df['index5_2'] = data2['KDJ_K'] >= data2['KDJ_K'].shift(1)
    # df['index5_3'] = data2['KDJ_D'] >= data2['KDJ_D'].shift(1)
    # df['index5_4'] = data2['KDJ_J'] >= data2['KDJ_K']
    # df['index5_5'] = data2['KDJ_K'] >= data2['KDJ_D']
    # df['index5_6'] = data2['KDJ_J'] >= 80
    # df['index5'] = df['index5_1'] & df['index5_2'] & df['index5_3'] & df['index5_4'] & df['index5_5'] & df['index5_6']
    # del df['index5_1'], df['index5_2'], df['index5_3'], df['index5_4'], df['index5_5'], df['index5_6']
    #
    #
    # # df['date_signal'] = df['date'].isin(["2024-03-20","2024-03-21"])
    # # if len(trade_date) == 0:
    # #     trade_date = [get_trade_data_day()]
    # # df['date_signal'] = df['date'].isin(trade_date)
    #
    # # 选择指标
    # # df['index'] = df['index1'] & df['index2'] & df['index3'] & df['index4'] & df['index5'] & df['date_signal']
    # df['index'] = df['index4'] & df['index5'] & df['date_signal']
    # df['index'] = df['index'] & df['date_signal']

    df['index'] = df['index1'] & df['index2']
    df = df[df['index']]
    del df['index'], df['index1'], df['index2']

    return df


# 策略2：技术面策略
def select_2():
    codes = get_stock_code()
    # 初始化一个空 DataFrame，用于存储结果
    df = pd.DataFrame()
    for k, v in enumerate(codes):
        res = select_index(v[0])
        res["股票代码"] = v[0]
        res["股票名称"] = v[1]
        # 拼接
        df = pd.concat([df, res], ignore_index=True)
        print(v[0] + " 筛选完成。")

    # 排序
    df = df.loc[:, ['股票代码', '股票名称', '交易日期', '收盘价']]

    # 创建SQLite引擎，指定数据库文件名
    engine = create_engine('sqlite:///' + config.data_dir + '/stock.db')
    # 将DataFrame保存到SQLite数据库中，表名为'check_date'
    df.to_sql('select_2', con=engine, if_exists='replace', index=False)


if __name__ == '__main__':
    pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
    # 记录开始时间
    start_time = time.time()

    # 获取股票信息
    # get_stock_info()

    select_1()
    select_2()

    # 记录结束时间
    end_time = time.time()
    # 计算耗时
    elapsed_time = end_time - start_time
    print(f"The elapsed time is {elapsed_time} seconds.")
