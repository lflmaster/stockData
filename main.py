import time
from src.get_check_data import get_check_date
from src.get_stock_info import get_stock_info
from src.get_history_dayk import save_history_dayk, save_one_stock_history
from src.get_history_daykIndex import save_history_daykIndex, save_one_stock_history_daykIndex
from src.select_stock import select_1, select_2
from src.update_history_dayk import update_dayk, update_check_dayk, update_dayk2
from src.update_history_daykIndex import update_daykIndex, update_check_daykIndex
from src.update_last_dayk0 import update_last_dayk0


# 第一次运行需要获取历史数据
def first_day():
    # 获取所有股票的最新数据
    get_stock_info()
    # 获取所有股票的历史数据：从上市当日到end_dt的数据
    end_dt = '20241217'
    save_history_dayk(end_dt)  # 没有前复权数据
    # 计算所有股票的指标数据
    save_history_daykIndex()
    # 更新数据更新表
    get_check_date()


# 当天选股前需要更新数据
def every_day():
    # 获取所有股票的最新数据
    get_stock_info()
    # 获取所有股票的历史数据：从文件中最后一日到当日的数据
    update_dayk()  # 没有前复权数据
    # 计算所有股票的指标数据
    update_daykIndex()
    # 更新数据更新表
    get_check_date()


# 只更新当日数据，当日之前的数据已经存在
def last_day():
    # 获取所有股票的最新数据
    get_stock_info()
    # 获取所有股票的当日数据
    update_last_dayk0(replace=0)  # 不替换最后一行
    update_dayk2()
    # 计算所有股票的指标数据
    update_daykIndex()
    # 更新数据更新表
    get_check_date()


# 在stock.db中查看check_data表的数据状态字段，存在-1则数据出错
# 只更新数据状态出错的数据
def every_day_check():
    # 获取所有股票的历史数据：从文件中最后一日到当日的数据
    update_check_dayk()  # 没有前复权数据
    # 计算所有股票的指标数据
    update_check_daykIndex()
    # 更新数据更新表
    get_check_date()


# 只更新一只股票的数据
def update_stock_by_code():
    stock_code = "000001"
    # 更新指定股票的历史数据：从文件中最后一日到当日的数据
    save_one_stock_history(stock_code)
    # 更新指定股票的指标数据
    save_one_stock_history_daykIndex(stock_code)


# 选择股票
def select_stock():
    # 通过基本面选择股票
    select_1()
    # 在基本面基础上，通过技术面选择股票
    select_2()


if __name__ == '__main__':
    # 记录开始时间
    start_time = time.time()

    # 建议在当日的15:30后更新数据
    # every_day()
    last_day()
    # 选择股票
    select_stock()

    # 记录结束时间
    end_time = time.time()
    # 计算耗时
    elapsed_time = end_time - start_time
    print(f"The elapsed time is {elapsed_time} seconds.")
