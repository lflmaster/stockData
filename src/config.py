import os


# 获取当前文件所在目录的绝对路径
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 指定 data 文件夹的绝对路径
data_dir = os.path.join(BASE_DIR, 'data')
# 保存日线未复权文件夹
dayk0_dir = os.path.join(data_dir, 'dayk0')
# 保存日线后复权文件夹
dayk2_dir = os.path.join(data_dir, 'dayk2')
# 保存日线后复权文件夹
dayk1_dir = os.path.join(data_dir, 'dayk1')


# 保存股票每日交易信息信息文件夹
stock_info_dir = os.path.join(data_dir, 'stock_info')
# 保存日线未复权指标文件夹
dayk0Index_dir = os.path.join(data_dir, 'dayk0Index')
# 保存日线后复权指标文件夹
dayk1Index_dir = os.path.join(data_dir, 'dayk1Index')
# 保存日线后复权指标文件夹
dayk2Index_dir = os.path.join(data_dir, 'dayk2Index')
