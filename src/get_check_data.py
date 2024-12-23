import os
import sqlite3
import time
import pandas as pd
from sqlalchemy import create_engine
import config


def get_stock_sh_date():
    # 连接到SQLite数据库，数据库文件是 stock.db，如果文件不存在，会自动在当前目录创建:
    conn = sqlite3.connect(config.data_dir + '/' + 'stock.db')
    # 创建一个Cursor:
    cursor = conn.cursor()
    # 执行查询语句:
    cursor.execute('SELECT 股票代码,股票名称,上市日期 FROM stock_info WHERE 上市日期!=0')
    # 使用 fetchall() 取出所有结果集:
    results = cursor.fetchall()
    # 关闭Cursor:
    cursor.close()
    # 关闭连接:
    conn.close()
    return results


def get_kdata_start_end_date(target_dir):
    res = []
    if not os.path.exists(target_dir):
        return
    # 遍历文件列表，输出文件名
    for file in os.listdir(target_dir):
        code = file[:-4]
        target_file = target_dir + '/' + file
        target_file = open(target_file, 'r', encoding="utf-8")  # 以追加读写模式打开文件
        target_file_content = target_file.readlines()  # 逐行读取文件内容
        row_number = len(target_file_content)  # 获得文件行数
        if row_number > 1:
            first_row = target_file_content[1][:10]
            last_row = target_file_content[-1][:10]
        else:
            first_row = ""
            last_row = ""
        res.append([code, row_number-1, first_row, last_row])
    return res


def get_check_date():
    df1 = pd.DataFrame(get_stock_sh_date(), columns=['股票代码', '股票名称', '上市日期'])
    df2 = pd.DataFrame(get_kdata_start_end_date(config.dayk0_dir), columns=['股票代码', '交易天数0', '交易开始0', '交易结束0'])
    # df3 = pd.DataFrame(get_kdata_start_end_date(config.dayk1_dir), columns=['股票代码', '交易天数1', '交易开始1', '交易结束1'])
    df4 = pd.DataFrame(get_kdata_start_end_date(config.dayk2_dir), columns=['股票代码', '交易天数2', '交易开始2', '交易结束2'])
    df5 = pd.DataFrame(get_kdata_start_end_date(config.dayk0Index_dir), columns=['股票代码', '指标天数0', '指标开始0', '指标结束0'])
    # df6 = pd.DataFrame(get_kdata_start_end_date(config.dayk1Index_dir), columns=['股票代码', '指标天数1', '指标开始1', '指标结束1'])
    df7 = pd.DataFrame(get_kdata_start_end_date(config.dayk2Index_dir), columns=['股票代码', '指标天数2', '指标开始2', '指标结束2'])
    res = pd.merge(df1, df2, left_on='股票代码', right_on='股票代码', how='left')
    # res = pd.merge(res, df3, left_on='股票代码', right_on='股票代码', how='left')
    res = pd.merge(res, df4, left_on='股票代码', right_on='股票代码', how='left')
    res = pd.merge(res, df5, left_on='股票代码', right_on='股票代码', how='left')
    # res = pd.merge(res, df6, left_on='股票代码', right_on='股票代码', how='left')
    res = pd.merge(res, df7, left_on='股票代码', right_on='股票代码', how='left')
    # res['0_1'] = res['交易天数0'] == res['交易天数1']
    res['0_2'] = res['交易天数0'] == res['交易天数2']
    res['0_3'] = res['交易天数0'] == res['指标天数0']
    # res['0_4'] = res['交易天数0'] == res['指标天数1']
    res['0_5'] = res['交易天数0'] == res['指标天数2']

    res['数据状态'] = (res['0_2'] & res['0_3'] & res['0_5']) - 1
    del res['0_2'], res['0_3'], res['0_5']
    # 排序
    res = res.loc[:, ['股票代码', '股票名称', '上市日期', '数据状态', '交易天数0', '交易天数2', '指标天数0', '指标天数2',
                      '交易开始0', '交易开始2', '指标开始0', '指标开始2', '交易结束0', '交易结束2', '指标结束0', '指标结束2']]

    # res['数据状态'] = (res['0_1'] & res['0_2'] & res['0_3'] & res['0_4'] & res['0_5']) - 1
    # del res['0_1'], res['0_2'], res['0_3'], res['0_4'], res['0_5']
    # # 排序
    # res = res.loc[:, ['股票代码', '股票名称', '上市日期', '数据状态', '交易天数0', '交易天数1', '交易天数2', '指标天数0', '指标天数1',
    #                   '指标天数2', '交易开始0', '交易开始1', '交易开始2', '指标开始0', '指标开始1', '指标开始2', '交易结束0',
    #                   '交易结束1', '交易结束2', '指标结束0', '指标结束1', '指标结束2']]

    # 创建SQLite引擎，指定数据库文件名
    engine = create_engine('sqlite:///' + config.data_dir + '/stock.db')
    # 将DataFrame保存到SQLite数据库中，表名为'check_date'
    res.to_sql('check_date', con=engine, if_exists='replace', index=False)
    # print(df3)


# 按装订区域中的绿色按钮以运行脚本。
if __name__ == '__main__':
    # 记录开始时间
    start_time = time.time()

    get_check_date()

    # 记录结束时间
    end_time = time.time()
    # 计算耗时
    elapsed_time = end_time - start_time
    print(f"The elapsed time is {elapsed_time} seconds.")
