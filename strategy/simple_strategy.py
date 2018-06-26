#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/21 11:26
@File    : simple_strategy.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import logging
import time
from abat.common import BacktestTradeMode, PeriodType, RunMode, ContextKey, Direction
from abat.strategy import StgBase
import agent.md_agent
import agent.td_agent
from abat.strategy import StgHandlerBase
from config import Config
logger = logging.getLogger()


class MACroseStg(StgBase):

    def __init__(self, unit=1):
        super().__init__()
        self.ma5 = []
        self.ma10 = []
        self.unit = unit

    def on_prepare_min1(self, md_df, context):
        if md_df is not None:
            self.ma5 = list(md_df['close'].rolling(5, 5).mean())[10:]
            self.ma10 = list(md_df['close'].rolling(10, 10).mean())[10:]

    def on_min1(self, md_df, context):
        close = md_df['close'].iloc[-1]
        self.ma5.append(md_df['close'].iloc[-5:].mean())
        self.ma10.append(md_df['close'].iloc[-10:].mean())
        instrument_id = context[ContextKey.instrument_id_list][0]
        if self.ma5[-2] < self.ma10[-2] and self.ma5[-1] > self.ma10[-1]:
            position_date_pos_info_dic = self.get_position(instrument_id)
            no_target_position = True
            if position_date_pos_info_dic is not None:
                for position_date, pos_info in position_date_pos_info_dic.items():
                    direction = pos_info.direction
                    if direction == Direction.Short:
                        self.close_short(instrument_id, close, pos_info.position)
                    elif direction == Direction.Long:
                        no_target_position = False
            if no_target_position:
                self.open_long(instrument_id, close, self.unit)
        elif self.ma5[-2] > self.ma10[-2] and self.ma5[-1] < self.ma10[-1]:
            position_date_pos_info_dic = self.get_position(instrument_id)
            no_target_position = True
            if position_date_pos_info_dic is not None:
                for position_date, pos_info in position_date_pos_info_dic.items():
                    direction = pos_info.direction
                    if direction == Direction.Long:
                        self.close_long(instrument_id, close, pos_info.position)
                    elif direction == Direction.Short:
                        no_target_position = False
            if no_target_position:
                self.open_short(instrument_id, close, self.unit)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
    # 参数设置
    strategy_params = {'unit': 100000}
    md_agent_params_list = [{
        'name': 'min1',
        'md_period': PeriodType.Min1,
        'instrument_id_list': ['ethbtc'],  # ['jm1711', 'rb1712', 'pb1801', 'IF1710'],
        'init_md_date_to': '2018-6-17',
        'init_md_date_to': '2018-6-19',
    }]
    run_mode_realtime_params = {
        'run_mode': RunMode.Realtime,
    }
    run_mode_backtest_params = {
        'run_mode': RunMode.Backtest,
        'date_from': '2018-6-18',
        'date_to': '2018-6-19',
        'init_cash': 1000000,
        'trade_mode': BacktestTradeMode.Order_2_Deal
    }
    # run_mode = RunMode.BackTest
    # 初始化策略处理器
    stghandler = StgHandlerBase.factory(stg_class_obj=MACroseStg,
                                        strategy_params=strategy_params,
                                        md_agent_params_list=md_agent_params_list,
                                        **run_mode_backtest_params)
    stghandler.start()
    time.sleep(10)
    stghandler.keep_running = False
    stghandler.join()
    logging.info("执行结束")
