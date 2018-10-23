#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/20 19:53
@File    : md_agent.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import json
import time
from queue import Queue
from abat.md import MdAgentBase, register_backtest_md_agent, register_realtime_md_agent
from abat.utils.fh_utils import bytes_2_str
from abat.common import PeriodType
from backend import engine_md
from abat.utils.redis import get_redis, get_channel
from backend.orm import MDMin1
from abat.utils.db_utils import with_db_session
import pandas as pd
from config import Config


class MdAgentPub(MdAgentBase):

    def load_history(self, date_from=None, date_to=None, load_md_count=None)->(pd.DataFrame, dict):
        """
        从mysql中加载历史数据
        实时行情推送时进行合并后供数据分析使用
        :param date_from: None代表沿用类的 init_md_date_from 属性
        :param date_to: None代表沿用类的 init_md_date_from 属性
        :param load_md_count: 0 代表不限制，None代表沿用类的 init_load_md_count 属性，其他数字代表相应的最大加载条数
        :return: md_df 或者
         ret_data {
            'md_df': md_df, 'datetime_key': 'ts_start',
            'date_key': **, 'time_key': **, 'microseconds_key': **
            }
        """
        # 如果 init_md_date_from 以及 init_md_date_to 为空，则不加载历史数据
        if self.init_md_date_from is None and self.init_md_date_to is None:
            ret_data = {'md_df': None, 'datetime_key': 'ts_start'}
            return ret_data

        if self.md_period == PeriodType.Tick:
            # sql_str = """SELECT * FROM md_tick
            #             WHERE InstrumentID IN (%s) %s
            #             ORDER BY ActionDay DESC, ActionTime DESC, ActionMillisec DESC %s"""
            raise ValueError("暂不支持 tick 级回测")
        elif self.md_period == PeriodType.Min1:
            # 将sql 语句形势改成由 sqlalchemy 进行sql 拼装方式
            # sql_str = """select * from md_min_1
            #     where InstrumentID in ('j1801') and tradingday>='2017-08-14'
            #     order by ActionDay, ActionTime, ActionMillisec limit 200"""
            # sql_str = """SELECT * FROM md_min_1
            # WHERE InstrumentID IN (%s) %s
            # ORDER BY ActionDay DESC, ActionTime DESC %s"""
            with with_db_session(engine_md) as session:
                query = session.query(
                    MDMin1.symbol.label('pair'), MDMin1.ts_start.label('ts_start'),
                    MDMin1.open.label('open'), MDMin1.high.label('high'),
                    MDMin1.low.label('low'), MDMin1.close.label('close'),
                    MDMin1.vol.label('vol'), MDMin1.amount.label('amount'), MDMin1.count.label('count')
                ).filter(
                    MDMin1.symbol.in_(self.instrument_id_set)
                ).order_by(MDMin1.ts_start.desc())
                # 设置参数
                params = list(self.instrument_id_set)
                # date_from 起始日期
                if date_from is None:
                    date_from = self.init_md_date_from
                if date_from is not None:
                    # qry_str_date_from = " and tradingday>='%s'" % date_from
                    query = query.filter(MDMin1.ts_start >= date_from)
                    params.append(date_from)
                # date_to 截止日期
                if date_to is None:
                    date_to = self.init_md_date_to
                if date_to is not None:
                    # qry_str_date_to = " and tradingday<='%s'" % date_to
                    query = query.filter(MDMin1.ts_start <= date_to)
                    params.append(date_to)

                # load_limit 最大记录数
                if load_md_count is None:
                    load_md_count = self.init_load_md_count
                if load_md_count is not None and load_md_count > 0:
                    qry_str_limit = " limit %d" % load_md_count
                    query = query.limite(load_md_count)
                    params.append(load_md_count)

                sql_str = str(query)
        else:
            raise ValueError('%s error' % self.md_period)

        # 合约列表
        # qry_str_inst_list = "'" + "', '".join(self.instrument_id_set) + "'"
        # 拼接sql
        # qry_sql_str = sql_str % (qry_str_inst_list, qry_str_date_from + qry_str_date_to, qry_str_limit)

        # 加载历史数据
        md_df = pd.read_sql(sql_str, engine_md, params=params)
        # self.md_df = md_df
        ret_data = {'md_df': md_df, 'datetime_key': 'ts_start'}
        return ret_data


@register_realtime_md_agent
class MdAgentRealtime(MdAgentPub):

    def __init__(self, instrument_id_set, md_period: PeriodType, name=None, init_load_md_count=None,
                 init_md_date_from=None, init_md_date_to=None, **kwargs):
        super().__init__(instrument_id_set, md_period, name=name, init_load_md_count=init_load_md_count,
                         init_md_date_from=init_md_date_from, init_md_date_to=init_md_date_to, **kwargs)
        self.pub_sub = None
        self.md_queue = Queue()

    def connect(self):
        """链接redis、初始化历史数据"""
        redis_client = get_redis()
        self.pub_sub = redis_client.pubsub()

    def release(self):
        """释放channel资源"""
        self.pub_sub.close()

    def subscribe(self, instrument_id_set=None):
        """订阅合约"""
        super().subscribe(instrument_id_set)
        if instrument_id_set is None:
            instrument_id_set = self.instrument_id_set
        # channel_head = Config.REDIS_CHANNEL[self.md_period]
        # channel_list = [channel_head + instrument_id for instrument_id in instrument_id_set]
        channel_list = [get_channel(Config.MARKET_NAME, self.md_period, instrument_id)
                        for instrument_id in instrument_id_set]
        self.pub_sub.psubscribe(*channel_list)

    def run(self):
        """启动多线程获取MD"""
        if not self.keep_running:
            self.keep_running = True
            for item in self.pub_sub.listen():
                if self.keep_running:
                    if item['type'] == 'pmessage':
                        # self.logger.debug("pmessage:", item)
                        md_dic_str = bytes_2_str(item['data'])
                        md_dic = json.loads(md_dic_str)
                        self.md_queue.put(md_dic)
                    else:
                        self.logger.debug("%s response: %s", self.name, item)
                else:
                    break

    def unsubscribe(self, instrument_id_set):
        """退订合约"""
        if instrument_id_set is None:
            tmp_set = self.instrument_id_set
            super().unsubscribe(instrument_id_set)
            instrument_id_set = tmp_set
        else:
            super().unsubscribe(instrument_id_set)

        # channel_head = Config.REDIS_CHANNEL[self.md_period]
        # channel_list = [channel_head + instrument_id for instrument_id in instrument_id_set]
        channel_list = [get_channel(Config.MARKET_NAME, self.md_period, instrument_id)
                        for instrument_id in instrument_id_set]
        if self.pub_sub is not None:  # 在回测模式下有可能不进行 connect 调用以及 subscribe 订阅，因此，没有 pub_sub 实例
            self.pub_sub.punsubscribe(*channel_list)

    def pull(self, timeout=None):
        """阻塞方式提取合约数据"""
        md = self.md_queue.get(block=True, timeout=timeout)
        self.md_queue.task_done()
        return md


@register_backtest_md_agent
class MdAgentBacktest(MdAgentPub):

    def __init__(self, instrument_id_set, md_period: PeriodType, name=None, init_load_md_count=None,
                 init_md_date_from=None, init_md_date_to=None, **kwargs):
        super().__init__(instrument_id_set, md_period, name=name, init_load_md_count=init_load_md_count,
                         init_md_date_from=init_md_date_from, init_md_date_to=init_md_date_to, **kwargs)
        self.timeout = 1

    def connect(self):
        """链接redis、初始化历史数据"""
        pass

    def release(self):
        """释放channel资源"""
        pass

    def run(self):
        """启动多线程获取MD"""
        if not self.keep_running:
            self.keep_running = True
            while self.keep_running:
                time.sleep(self.timeout)
            else:
                self.logger.info('%s job finished', self.name)