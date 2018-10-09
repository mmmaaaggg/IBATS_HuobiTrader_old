# -*- coding: utf-8 -*-
"""
Created on 2017/6/9
@author: MG
"""
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger()


class ConfigBase:
    # 交易所名称
    MARKET_NAME = 'huobi'

    # api configuration
    EXCHANGE_ACCESS_KEY = ""
    EXCHANGE_SECRET_KEY = ""

    # mysql db info
    DB_SCHEMA_ABAT = 'abat'
    DB_SCHEMA_MD = 'bc_md'
    DB_URL_DIC = {
        DB_SCHEMA_MD: 'mysql://mg:****10.0.3.66/' + DB_SCHEMA_MD,
        DB_SCHEMA_ABAT: 'mysql://mg:****@10.0.3.66/' + DB_SCHEMA_ABAT,
    }

    # redis info
    REDIS_INFO_DIC = {'REDIS_HOST': '192.168.239.131',
                      'REDIS_PORT': '6379',
                      }

    # evn configuration
    LOG_FORMAT = '%(asctime)s %(levelname)s %(name)s %(filename)s.%(funcName)s:%(lineno)d|%(message)s'

    # 每一次实务均产生数据库插入或更新动作（默认：否）
    UPDATE_OR_INSERT_PER_ACTION = False


class ConfigProduct(ConfigBase):
    # 测试子账户 key
    EXCHANGE_ACCESS_KEY = '***'
    EXCHANGE_SECRET_KEY = '***'

    DB_URL_DIC = {
        ConfigBase.DB_SCHEMA_MD: 'mysql://mg:***@10.0.3.66/' + ConfigBase.DB_SCHEMA_MD,
        ConfigBase.DB_SCHEMA_ABAT: 'mysql://mg:***@10.0.3.66/' + ConfigBase.DB_SCHEMA_ABAT,
    }


# 开发配置（SIMNOW MD + Trade）
# Config = ConfigBase()
# 测试配置（测试行情库）
# Config = ConfigTest()
# 生产配置
Config = ConfigProduct()

# 设定默认日志格式
logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
# 设置rest调用日志输出级别
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)
logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
logging.getLogger('EventAgent').setLevel(logging.INFO)
logging.getLogger('StgBase').setLevel(logging.INFO)

# 配置文件日至
Rthandler = RotatingFileHandler('log.log', maxBytes=10 * 1024 * 1024, backupCount=5)
Rthandler.setLevel(logging.INFO)
formatter = logging.Formatter(Config.LOG_FORMAT)
Rthandler.setFormatter(formatter)
logging.getLogger('').addHandler(Rthandler)
