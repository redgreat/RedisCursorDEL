#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# @author by wangcw @ 2025
# @generate at 2025/2/21 10:11
# comment: 批量删除以指定前缀开头的Redis KEY

import redis
import os
import configparser
from loguru import logger
import sys

def connect_redis():
    """
    连接Redis数据库
    """
    config = configparser.ConfigParser()
    config.read("../conf/db.cnf")
    
    redis_host = config.get("gjf_pro", "host")
    redis_database = int(config.get("gjf_pro", "database"))
    redis_user = config.get("gjf_pro", "user")
    redis_password = config.get("gjf_pro", "password")
    redis_port = int(config.get("gjf_pro", "port"))
    
    try:
        r = redis.Redis(host=redis_host, port=redis_port, db=redis_database,
                        password=redis_user+':'+redis_password, decode_responses=True)
        r.ping()
        logger.info("成功连接到 Redis 数据库")
        return r
    except Exception as e:
        logger.error(f"连接 Redis 数据库失败：{e}")
        return None

def setup_logger():
    """
    配置日志系统
    """
    logDir = os.path.expanduser("../log/")
    if not os.path.exists(logDir):
        os.mkdir(logDir)
    logFile = os.path.join(logDir, "redis_delete.log")
    
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

def delete_keys_by_pattern(redis_client, pattern_str, batch_size=3000, dry_run=True):
    """
    批量删除匹配指定模式的Redis KEY
    
    Args:
        redis_client: Redis客户端实例
        pattern_str: KEY匹配模式字符串
        batch_size: 批处理大小
        dry_run: 是否为试运行模式（不实际删除）
    """
    pattern = f"*{pattern_str}*"
    cursor = 0
    total_deleted = 0
    
    logger.info(f"开始{'试运行' if dry_run else '删除'}匹配模式 '{pattern}' 的 KEY")
    
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=batch_size)
        
        if keys:
            logger.info(f"找到 {len(keys)} 个匹配的 KEY")
            
            if dry_run:
                for key in keys:
                    logger.info(f"[试运行] 将删除 KEY: {key}")
                total_deleted += len(keys)
            else:
                try:
                    deleted_count = redis_client.delete(*keys)
                    total_deleted += deleted_count
                    logger.info(f"成功删除 {deleted_count} 个 KEY")
                    
                    for key in keys:
                        logger.info(f"已删除 KEY: {key}")
                except Exception as e:
                    logger.error(f"删除 KEY 时发生错误: {e}")
        
        if cursor == 0:
            break
    
    logger.info(f"{'试运行' if dry_run else '删除'}完成，共处理 {total_deleted} 个 KEY")
    return total_deleted

def main():
    """
    主函数
    """
    setup_logger()
    
    if len(sys.argv) < 2:
        logger.error("请提供要匹配的 KEY 模式")
        print("使用方法: python delete_prifix_key_batch.py <模式字符串> [--execute]")
        print("示例: python delete_prifix_key_batch.py insured_medical_idNumberCheck")
        print("      python delete_prifix_key_batch.py insured_medical_idNumberCheck --execute")
        sys.exit(1)
    
    pattern_str = sys.argv[1]
    dry_run = "--execute" not in sys.argv
    
    if dry_run:
        logger.info("运行在试运行模式，不会实际删除 KEY")
        logger.info("如需实际删除，请添加 --execute 参数")
    else:
        logger.warning("运行在执行模式，将实际删除匹配的 KEY")
        response = input(f"确认要删除所有包含 '{pattern_str}' 的 KEY 吗？(yes/no): ")
        if response.lower() != 'yes':
            logger.info("操作已取消")
            sys.exit(0)
    
    redis_client = connect_redis()
    if not redis_client:
        sys.exit(1)
    
    try:
        total_count = delete_keys_by_pattern(redis_client, pattern_str, dry_run=dry_run)
        
        if dry_run:
            logger.info(f"试运行完成，共找到 {total_count} 个匹配的 KEY")
        else:
            logger.info(f"删除操作完成，共删除 {total_count} 个 KEY")
            
    except Exception as e:
        logger.error(f"执行过程中发生错误: {e}")
    finally:
        redis_client.close()
        logger.info("Redis 连接已关闭")

if __name__ == "__main__":
    main()