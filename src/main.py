#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025/2/21 10:11
# comment:

import redis
import json
import os
import configparser
from loguru import logger
from datetime import datetime, timezone

# 数据库连接定义
config = configparser.ConfigParser()
config.read("../conf/db.cnf")

redis_host = config.get("espd_pro", "host")
redis_database = int(config.get("espd_pro", "database"))
redis_user = config.get("espd_pro", "user")
redis_password = config.get("espd_pro", "password")
redis_port = int(config.get("espd_pro", "port"))

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

# 确保输出文件目录存在
outputDir = "../files"
if not os.path.exists(outputDir):
    os.makedirs(outputDir)
outputFile = os.path.join(outputDir, "output_keys.txt")

try:
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_database,
                    password=redis_user+':'+redis_password, decode_responses=True)
    logger.info("成功连接到 Redis 数据库")
except Exception as e:
    logger.info(f"连接 Redis 数据库失败：{e}")
    exit()

key_pattern = "CWInfoIncompleteTag_*"
json_node_path = "FirstLoginTime"
target_date = datetime(2025, 2, 20, 0, 0, 0, tzinfo=timezone.utc)

keys = r.keys(key_pattern)
if not keys:
    logger.info("未找到匹配的 key")
    exit()

logger.info(f"找到 {len(keys)} 个匹配的 key，正在处理...")

# 打开文件用于写入
with open(outputFile, "w") as f:
    for key in keys:
        value = r.get(key)
        if not value:
            logger.info(f"Key '{key}' 的值为空，跳过")
            continue

        try:
            json_data = json.loads(value)
        except json.JSONDecodeError:
            logger.info(f"Key '{key}' 的值不是有效的 JSON 格式，跳过")
            continue

        node_value = json_data
        for node in json_node_path.split("."):
            if isinstance(node_value, dict) and node in node_value:
                node_value = node_value[node]
            else:
                logger.info(f"Key '{key}' 中不存在路径 '{json_node_path}'，跳过")
                break
        else:
            logger.info(f"Key: {key}, JSON Node Value: {node_value}")
            node_value = node_value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(node_value)
            target_date = target_date.astimezone(dt.tzinfo)
            if dt < target_date:
                f.write(f"DEL '{key}'\n")
                logger.info(f"符合条件的 key '{key}' 已写入文件")

logger.info("处理完成")
