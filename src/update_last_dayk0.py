import os
import csv
import pandas as pd
import requests
from datetime import datetime, timedelta
import exchange_calendars as xcals
import time

import config


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


def get_trade_kdata():
    data_info = {
        'f2': '收盘价',
        'f3': '涨跌幅',
        'f4': '涨跌额',
        'f5': '成交量',
        'f6': '成交额',
        'f7': '振幅',
        'f8': '换手率',
        'f12': '股票代码',
        'f14': '股票名称',
        'f15': '最高价',
        'f16': '最低价',
        'f17': '开盘价'
    }
    url = 'http://27.push2.eastmoney.com/api/qt/clist/get'
    url_parameters = {
        'fields': ','.join(list(data_info.keys())),
        'pz': 10000,  # 每页条数
        'pn': 1,  # 页码
        'fs': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048',
    }
    response = requests.get(url, url_parameters)
    response_json = response.json()
    # 返回数据为空时停止循环
    if response_json['data']['diff'] is None:
        print("无法从东方财富获取数据。已退出")
        exit()
    else:
        data = response_json['data']['diff']
        print("获取数据:", len(data))
        # 将字典转换为DataFrame
        df = pd.DataFrame(data).transpose()
        # 更换DataFrame的header
        df.rename(columns=data_info, inplace=True)
        df['收盘价'] = df['收盘价'] / 100
        df['涨跌幅'] = df['涨跌幅'] / 100
        df['涨跌额'] = df['涨跌额'] / 100
        df['振幅'] = df['振幅'] / 100
        df['换手率'] = df['换手率'] / 100
        df['最高价'] = df['最高价'] / 100
        df['最低价'] = df['最低价'] / 100
        df['开盘价'] = df['开盘价'] / 100
        # print(type( df['收盘价'][0]))
        # 排序 交易日期,开盘价,收盘价,最高价,最低价,成交量,成交额,振幅,涨跌幅,涨跌额,换手率
        df = df.loc[:,
             ['股票代码', '股票名称', '开盘价', '收盘价', '最高价', '最低价', '成交量', '成交额', '振幅', '涨跌幅',
              '涨跌额', '换手率']]
        return df


def update_last_dayk0(replace=0):
    last_trade_date_ = get_trade_data_day()
    trade_kdata = get_trade_kdata()
    for k, v in trade_kdata.iterrows():
        code = v['股票代码']
        line = (str(last_trade_date_) + ',' + str(v['开盘价']) + ',' + str(v['收盘价']) + ',' + str(v['最高价']) + ','
                + str(v['最低价']) + ',' + str(v['成交量']) + ',' + str(v['成交额']) + ',' + str(v['振幅']) + ',' +
                str(v['涨跌幅']) + ',' + str(v['涨跌额']) + ',' + str(v['换手率']))
        # 更新未复权
        # 读取.csv文件
        file0 = config.dayk0_dir + '/' + code + '.csv'
        # 检查CSV文件是否存在
        if not os.path.exists(file0) or v['开盘价'] == 0:
            continue
        with open(file0, 'r', encoding="utf-8") as file:
            reader = csv.reader(file)
            rows = list(reader)
        if len(rows) > 1:
            file_last_trade_day = rows[-1][0]
            if file_last_trade_day == last_trade_date_:
                if replace == 0:
                    continue
                elif replace == 1:
                    # 定位修改最后一行
                    rows[-1] = line.split(',')
                    # 保存修改后的文件
                    with open(file0, 'w', newline='', encoding="utf-8") as file:
                        writer = csv.writer(file)
                        writer.writerows(rows)
                        print(code, "成功替换 " + last_trade_date_ + " 数据 0")
                    continue
        target_file0 = open(file0, 'a+', encoding="utf-8")  # 以追加读写模式打开文件
        target_file0.write(line + '\n')
        print(code, "成功添加 " + last_trade_date_ + " 数据 0")


if __name__ == '__main__':
    # 记录开始时间
    start_time = time.time()

    update_last_dayk0(replace=0)

    # 记录结束时间
    end_time = time.time()
    # 计算耗时
    elapsed_time = end_time - start_time
    print(f"The elapsed time is {elapsed_time} seconds.")
