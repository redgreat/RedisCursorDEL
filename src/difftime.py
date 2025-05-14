import redis
import json
import os
from datetime import datetime
import configparser
from loguru import logger

# 数据库连接定义
config = configparser.ConfigParser()
config.read("conf/db.cnf")

redis_host = config.get("espd_test", "host")
redis_database = int(config.get("espd_test", "database"))
redis_user = config.get("espd_test", "user")
redis_password = config.get("espd_test", "password")
redis_port = int(config.get("espd_test", "port"))

# 日志配置
logDir = os.path.expanduser("../log/")
if not os.path.exists(logDir):
    os.mkdir(logDir)
logFile = os.path.join(logDir, "redis.log")

logger.add(
    logFile,
    colorize=True,
    rotation="1 days",
    retention="3 days",
    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
    backtrace=True,
    diagnose=True,
    level="INFO",
)

# 连接到Redis
try:
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_database,
                    password=redis_user + ':' + redis_password, decode_responses=True)
    logger.info("成功连接到 Redis 数据库")
except Exception as e:
    logger.info(f"连接 Redis 数据库失败：{e}")
    exit()

# 获取所有键
keys = r.keys('*')

time_diff_list = []

for key in keys:
    value = r.get(key)
    if value:
        data = json.loads(value)
        create_time_str = data.get('createTime')
        mysql_insert_time_str = data.get('MySQLInsertTime')
        if not create_time_str or not mysql_insert_time_str:
            continue       

        try:
            create_time = datetime.strptime(create_time_str, '%Y-%m-%d %H:%M:%S')
            mysql_insert_time = datetime.strptime(mysql_insert_time_str, '%Y-%m-%d %H:%M:%S')
            
            # 计算时间差（以秒为单位）
            time_diff = (create_time - mysql_insert_time).total_seconds()
            
            # 将结果存储到列表中
            time_diff_list.append((key, time_diff))  # 直接使用 key，无需 decode
        except ValueError as e:
            logger.error(f"Error processing key {key}: {e}")

# 按时间差倒序排序并取前10条
sorted_time_diff_list = sorted(time_diff_list, key=lambda x: x[1], reverse=True)[:10]

# 打印前10条记录
print("Top 10 records with the largest time differences:")
for key, diff in sorted_time_diff_list:
    print(f"Key: {key}, Time Difference: {diff} seconds")

# 计算所有时间差的平均值
if time_diff_list:
    avg_time_diff = sum(diff for _, diff in time_diff_list) / len(time_diff_list)
    print(f"\nAverage Time Difference: {avg_time_diff} seconds")
else:
    print("\nNo valid records found.")
