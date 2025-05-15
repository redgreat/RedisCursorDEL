import redis
import json
import os
from datetime import datetime
import configparser
from loguru import logger
import concurrent.futures

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
    logger.error(f"连接 Redis 数据库失败：{e}")
    exit()

# 获取所有键
keys = r.keys('*')

# 打印总键的数量
total_keys = len(keys)
print(f"总键的数量: {total_keys}")

# 用于存储时间差和键的列表
time_diff_list = []

def process_key(key):
    value = r.get(key)
    if value:
        data = json.loads(value)
        create_time_str = data.get('createTime')
        insert_time_str = data.get('insertTime')  # 假设每个键的值中都有一个insertTime字段
        if not create_time_str or not insert_time_str:
            return None

        try:
            create_time = datetime.strptime(create_time_str, '%Y-%m-%d %H:%M:%S')
            insert_time = datetime.strptime(insert_time_str, '%Y-%m-%d %H:%M:%S')
            
            # 计算时间差（以秒为单位）
            time_diff = (create_time - insert_time).total_seconds()
            
            # 返回结果
            return key, time_diff
        except ValueError as e:
            logger.error(f"Error processing key {key}: {e}")
    return None

# 使用线程池处理每个键
with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
    futures = [executor.submit(process_key, key) for key in keys]
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        if result:
            time_diff_list.append(result)

# 按时间差倒序排序并取前3条
sorted_time_diff_list = sorted(time_diff_list, key=lambda x: x[1], reverse=True)[:3]

# 打印前3条记录
print("延迟最长前3条:")
for key, diff in sorted_time_diff_list:
    print(f"键: {key}, 延迟: {diff} 秒")

# 计算所有时间差的平均值
if time_diff_list:
    avg_time_diff = sum(diff for _, diff in time_diff_list) / len(time_diff_list)
    print(f"平均延迟: {avg_time_diff} 秒")
else:
    print("No valid records found.")
