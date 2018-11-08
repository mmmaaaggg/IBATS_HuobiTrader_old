#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/9/28 8:42
@File    : run.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import click
import logging
import time
from abat.common import PeriodType, RunMode, BacktestTradeMode
from abat.strategy import StgHandlerBase
from strategy.bs_against_files.csv_orders_with_feedback import ReadFileStg
from strategy.simple_strategy import MACroseStg

logger = logging.getLogger()

strategy_list = [ReadFileStg]  # , MACroseStg
promt_str = '输入对应数字选择执行策略：\n' + \
            '\n'.join(['%d) %s' % (num, foo.__name__) for num, foo in enumerate(strategy_list)]) + '\n'


@click.command()
@click.option('--num', type=click.IntRange(0, len(strategy_list) - 1), prompt=promt_str)
@click.option('--init', type=click.BOOL, default=False)
def main(num, init):
    if init:
        from abat.backend.orm import init
        init()

    stg_func = strategy_list[num]

    DEBUG = False
    # 参数设置
    strategy_params = {}
    md_agent_params_list = [
        # {
        #     'name': 'min1',
        #     'md_period': PeriodType.Min1,
        #     'instrument_id_list': ['rb1805', 'i1801'],  # ['jm1711', 'rb1712', 'pb1801', 'IF1710'],
        #     'init_md_date_to': '2017-9-1',
        #     'dict_or_df_as_param': dict
        # },
        {
            'name': 'tick',
            'md_period': PeriodType.Tick,
            'instrument_id_list': ['ethusdt', 'eosusdt'],  #
        }]
    run_mode_realtime_params = {
        'run_mode': RunMode.Realtime,
        'enable_timer_thread': True,
        'seconds_of_timer_interval': 15,
    }
    run_mode_backtest_params = {
        'run_mode': RunMode.Backtest,
        'date_from': '2017-9-4',
        'date_to': '2017-9-27',
        'init_cash': 1000000,
        'trade_mode': BacktestTradeMode.Order_2_Deal
    }
    # run_mode = RunMode.BackTest
    # 初始化策略处理器
    stghandler = StgHandlerBase.factory(
        stg_class_obj=stg_func,
        strategy_params=strategy_params,
        md_agent_params_list=md_agent_params_list,
        **run_mode_realtime_params)

    if DEBUG:
        stghandler.run()
    else:
        # 开始执行策略
        stghandler.start()
        try:
            while True:
                # 策略执行 2 分钟后关闭
                time.sleep(2)
        except KeyboardInterrupt:
            logger.warning('程序中断中...')

        stghandler.keep_running = False
        stghandler.join(timeout=2)

    logger.info("执行结束")


if __name__ == "__main__":

    main(standalone_mode=False)
