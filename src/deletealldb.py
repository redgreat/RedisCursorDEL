import redis
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
    logger.error(f"连接 Redis 数据库失败：{e}")
    exit()

# 确认是否清空数据库
confirm = input(f"确认是否清空数据库 {redis_database}？(y/n): ")
if confirm.lower() != 'y':
    logger.info("操作已取消")
    exit()

# 使用SCAN命令分批删除键
try:
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match='*', count=1000)
        if keys:
            r.delete(*keys)
        if cursor == 0:
            break
    logger.info(f"数据库 {redis_database} 已成功清空")
    print(f"数据库 {redis_database} 已成功清空")
except Exception as e:
    logger.error(f"清空数据库 {redis_database} 时出错：{e}")
    print(f"清空数据库 {redis_database} 时出错：{e}")
