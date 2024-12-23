import os
from datetime import datetime
import config
import requests
import pandas as pd
from sqlalchemy import create_engine


def get_stock_info():
    data_info = {
        'f12': '股票代码',
        'f14': '股票名称',
        'f26': '上市日期',
        'f2': '最新价',
        'f17': '开盘价',
        'f15': '最高价',
        'f16': '最低价',
        'f18': '昨收',
        'f3': '涨跌幅',
        'f4': '涨跌额',
        'f11': '5分钟涨跌幅',
        'f5': '总手',
        'f6': '成交额',
        'f7': '振幅',
        'f8': '换手率',
        'f9': '市盈率',
        'f10': '量比',
        'f13': '市场',
        'f20': '总市值',
        'f21': '流通市值',
        'f22': '涨速',
        'f23': '市净率',
        'f24': '60日涨跌幅',
        'f25': '年初至今涨跌幅',
        'f28': '昨日结算价',
        'f30': '现手',
        'f31': '买入价',
        'f32': '卖出价',
        'f33': '委比',
        'f34': '外盘',
        'f35': '内盘',
        'f36': '人均持股数',
        'f37': '净资产收益率(加权)',
        'f38': '总股本',
        'f39': '流通股',
        'f40': '营业总收入',
        'f41': '营业总收入同比',
        'f42': '营业利润',
        'f43': '投资收益',
        'f44': '利润总额',
        'f45': '归属净利润',
        'f46': '归属净利润同比',
        'f47': '未分配利润',
        'f48': '每股未分配利润',
        'f49': '销售毛利率',
        'f50': '总资产',
        'f51': '流动资产',
        'f52': '固定资产',
        'f53': '无形资产',
        'f54': '总负债',
        'f55': '流动负债',
        'f56': '长期负债',
        'f57': '资产负债比率',
        'f58': '股东权益',
        'f59': '股东权益比',
        'f60': '公积金',
        'f61': '每股公积金',
        'f62': '主力净流入',
        'f63': '集合竞价',
        'f64': '超大单流入',
        'f65': '超大单流出',
        'f66': '超大单净额',
        'f69': '超大单净占比',
        'f70': '大单流入',
        'f71': '大单流出',
        'f72': '大单净额',
        'f75': '大单净占比',
        'f76': '中单流入',
        'f77': '中单流出',
        'f78': '中单净额',
        'f81': '中单净占比',
        'f82': '小单流入',
        'f83': '小单流出',
        'f84': '小单净额',
        'f87': '小单净占比',
        'f88': '当日DDX',
        'f89': '当日DDY',
        'f90': '当日DDZ',
        'f91': '5日DDX',
        'f92': '5日DDY',
        'f94': '10日DDX',
        'f95': '10日DDY',
        'f97': 'DDX飘红天数(连续)',
        'f98': 'DDX飘红天数(5日)',
        'f99': 'DDX飘红天数(10日)',
        'f100': '行业板块',
        'f102': '地区板块',
        'f103': '概念题材',
        'f112': '每股收益',
        'f113': '每股净资产',
        'f114': '市盈率（静）',
        'f115': '市盈率（TTM）',
        'f124': '当前交易时间',
        'f129': '净利润',
        'f130': '市销率TTM',
        'f131': '市现率TTM',
        'f132': '总营业收入TTM',
        'f133': '股息率',
        'f135': '净资产',
        'f221': '财务数据更新日期'
    }
    url = 'http://27.push2.eastmoney.com/api/qt/clist/get'
    url_parameters = {
        'fields': ','.join(list(data_info.keys())),
        'pz': 10000,  # 每页条数
        'pn': 1,  # 页码
        'fs': 'm:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048'
    }
    response = requests.get(url, url_parameters)
    response_json = response.json()
    # 返回数据为空时停止循环
    if response_json['data']['total'] == 0:
        print("无法从东方财富获取数据。")
    else:
        data = response_json['data']['diff']
        print("获取数据:", len(data))
        # 将字典转换为DataFrame
        df = pd.DataFrame(data).transpose()
        df['f2'] = df['f2'] / 100
        df['f3'] = df['f3'] / 100
        df['f4'] = df['f4'] / 100
        df['f7'] = df['f7'] / 100
        df['f8'] = df['f8'] / 100
        df['f9'] = df['f9'] / 100
        df['f10'] = df['f10'] / 100
        df['f11'] = df['f11'] / 100
        df['f15'] = df['f15'] / 100
        df['f16'] = df['f16'] / 100
        df['f17'] = df['f17'] / 100
        df['f18'] = df['f18'] / 100
        df['f22'] = df['f22'] / 100
        df['f23'] = df['f23'] / 100
        df['f24'] = df['f24'] / 100
        df['f25'] = df['f25'] / 100
        df['f31'] = df['f31'] / 100
        df['f32'] = df['f32'] / 100
        df['f33'] = df['f33'] / 100

        # 获取当前日期
        current_date = datetime.now().strftime('%Y-%m-%d')
        stock_info_dir = config.stock_info_dir
        if not os.path.exists(stock_info_dir):
            os.makedirs(stock_info_dir)
        # 调整DataFrame列的顺序
        df = df[list(data_info.keys())]
        # 更换DataFrame的header
        df.rename(columns=data_info, inplace=True)
        # print(df.columns)

        # 保存csv文件
        df.to_csv(stock_info_dir + "/" + current_date + ".csv", index=False, encoding='utf-8', header=list(data_info.values()))
        # 创建SQLite引擎，指定数据库文件名

        engine = create_engine('sqlite:///' + config.data_dir + '/stock.db')
        # 将DataFrame保存到SQLite数据库中，表名为'stock_info'
        df.to_sql('stock_info', con=engine, if_exists='replace', index=False)


# 按装订区域中的绿色按钮以运行脚本。
if __name__ == '__main__':
    # 获取股票信息
    get_stock_info()
