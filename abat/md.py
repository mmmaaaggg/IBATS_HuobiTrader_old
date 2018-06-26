#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/20 15:12
@File    : md.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from config import Config
from abat.common import PeriodType, RunMode
from threading import Thread
import time
import pandas as pd
import logging
from abc import ABC, abstractmethod
logger = logging.getLogger(__package__)


class MdAgentBase(Thread, ABC):

    @staticmethod
    def factory(run_mode: RunMode, instrument_id_list, md_period: PeriodType, name=None, **kwargs):
        # if run_mode == RunMode.Backtest:
        #     md_agent = MdAgentBacktest(instrument_id_list, md_period, name, **kwargs)
        # elif run_mode == RunMode.Realtime:
        #     md_agent = MdAgentRealtime(instrument_id_list, md_period, name, **kwargs)
        # else:
        #     raise ValueError("run_mode:%s exception", run_mode)
        md_agent_class = md_agent_class_dic[run_mode]
        md_agent = md_agent_class(instrument_id_list, md_period, name, **kwargs)
        return md_agent

    def __init__(self, instrument_id_set, md_period: PeriodType, name=None,
                 init_load_md_count=None, init_md_date_from=None, init_md_date_to=None, **kwargs):
        if name is None:
            name = md_period
        super().__init__(name=name, daemon=True)
        self.md_period = md_period
        self.keep_running = None
        self.instrument_id_set = instrument_id_set
        self.init_load_md_count = init_load_md_count
        self.init_md_date_from = init_md_date_from
        self.init_md_date_to = init_md_date_to
        self.logger = logging.getLogger()

    @abstractmethod
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

    @abstractmethod
    def connect(self):
        """链接redis、初始化历史数据"""

    @abstractmethod
    def release(self):
        """释放channel资源"""

    def subscribe(self, instrument_id_set=None):
        """订阅合约"""
        if instrument_id_set is None:
            return
        self.instrument_id_set |= instrument_id_set

    def unsubscribe(self, instrument_id_set):
        """退订合约"""
        if instrument_id_set is None:
            self.instrument_id_set = set()
        else:
            self.instrument_id_set -= instrument_id_set


md_agent_class_dic = {RunMode.Backtest: MdAgentBase, RunMode.Realtime: MdAgentBase}


def register_realtime_md_agent(agent: MdAgentBase) -> MdAgentBase:
    md_agent_class_dic[RunMode.Realtime] = agent
    logger.info('设置 realtime md agent:%s', agent.__class__.__name__)
    return agent


def register_backtest_md_agent(agent: MdAgentBase) -> MdAgentBase:
    md_agent_class_dic[RunMode.Backtest] = agent
    logger.info('设置 backtest md agent:%s', agent.__class__.__name__)
    return agent


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)

    instrument_id_list = set(['jm1711', 'rb1712', 'pb1801', 'IF1710'])
    md_agent = MdAgentBase.factory(RunMode.Realtime, instrument_id_list, md_period=PeriodType.Min1,
                                   init_load_md_count=100)
    md_df = md_agent.load_history()
    print(md_df.shape)
    md_agent.connect()
    md_agent.subscribe(instrument_id_list)
    md_agent.start()
    for n in range(120):
        time.sleep(1)
    md_agent.keep_running = False
    md_agent.join()
    md_agent.release()
    print("all finished")
