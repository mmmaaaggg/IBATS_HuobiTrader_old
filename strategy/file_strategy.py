# -*- coding: utf-8 -*-
"""
Created on 2017/11/18
@author: MG
"""
import threading
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from abat.strategy import StgBase, StgHandlerBase
from config import Config
from abat.common import PeriodType, RunMode, BacktestTradeMode, Direction, PositionDateType
import os
# 下面代码是必要的引用
# md_agent md_agent 并没有“显式”的被使用，但是在被引用期间，已经将相应的 agent 类注册到了相应的列表中
import agent.md_agent
import agent.td_agent


class ReadFileStg(StgBase):
    _folder_path = os.path.abspath(r'.\file_order')

    def __init__(self):
        super().__init__()
        self._mutex = threading.Lock()
        self._last_check_datetime = datetime.now() - timedelta(minutes=1)
        self.interval_timedelta = timedelta(seconds=15)
        self.target_position_dic = {}
        # 设定相应周期的事件驱动句柄 接收的参数类型
        self._on_period_event_dic[PeriodType.Tick].param_type = dict
        # 记录合约最近一次执行操作的时间
        self.symbol_last_deal_datetime = {}
        # 记录合约最近一个发送买卖请求的时间
        self.instrument_lastest_order_datetime_dic = {}
        # 目前由于交易是异步执行，在尚未记录每一笔订单的情况下，时间太短可能会导致仓位与请求但出现不同步现象，导致下单过多的问题
        self.timedelta_between_deal = timedelta(seconds=3)

    def fetch_pos_by_file(self):
        # 检查最近一次文件检查的时间，避免重复查询
        if self._last_check_datetime + self.interval_timedelta > datetime.now():
            return
        # 获取文件列表
        file_name_list = os.listdir(self._folder_path)
        if file_name_list is None:
            # self.logger.info('No file')
            return
        # 读取所有 csv 文件
        position_df = None
        for file_name in file_name_list:
            file_base_name, file_extension = os.path.splitext(file_name)
            if file_extension.lower() != '.csv':
                continue
            file_path = os.path.join(self._folder_path, file_name)
            position_df_tmp = pd.read_csv(file_path)
            if position_df is None:
                position_df = position_df_tmp
            else:
                position_df = position_df.append(position_df_tmp)

            # 文件备份
            backup_file_name = file_base_name + datetime.now().strftime(
                '%Y-%m-%d %H_%M_%S') + file_extension + '.bak'
            # 调试阶段暂时不重命名备份，不影响程序使用
            os.rename(file_path, os.path.join(self._folder_path, backup_file_name))

        return position_df

    def on_timer(self):
        """
        每15秒进行一次文件检查
        文件格式(csv xls)，每个symbol一行，不可重复，例：卖出eth 买入eos， 不考虑套利的情况（套利需要单独开发其他策略）
        currency   target_vol   symbol     price
        eth       0          ethusdt   800
        eos       200        eosusdt   40
        :param md_df: 
        :param context: 
        :return: 
        """
        with self._mutex:
            position_df = self.fetch_pos_by_file()
            if position_df is None or position_df.shape[0] == 0:
                return
            # 检查目标仓位与当前持仓是否相符，不符，则执行相应交易

            long_pos_df = position_df.set_index('currency').dropna()
            self.logger.debug('仓位调整方案：\n%s', long_pos_df)
            target_position_dic = {}
            for num, (currency, (target_vol, symbol, price)) in enumerate(long_pos_df.T.items()):
                # 检查当前持仓是否与目标持仓一致，如果一致则清空 self.target_position
                position_cur = 0
                position_date_pos_info_dic = self.get_position(symbol)
                if position_date_pos_info_dic is not None and len(position_date_pos_info_dic) > 0:
                    is_fit = False
                    for position_date_type, pos_info in position_date_pos_info_dic.items():
                        if pos_info['balance'] == target_vol:
                            is_fit = True
                            break
                        else:
                            position_cur += pos_info['balance']

                    if is_fit:
                        continue

                position_gap = target_vol - position_cur
                if position_gap == 0:
                    continue
                # 当前合约累计持仓与目标持仓不一致，则添加目标持仓任务
                # 多头目标持仓
                target_position_dic[symbol] = (Direction.Long, currency, target_vol, symbol, price)

            self.target_position_dic = target_position_dic
            if len(target_position_dic) > 0:
                self.logger.info('发现新的目标持仓指令\n%s', target_position_dic)

    def do_order(self, md_dic, instrument_id, position, price=None, direction=Direction.Long, msg=""):
        # if True:
        #     self.logger.info("%s %s %f 价格 %f [%s]",
        #                      instrument_id, '买入' if position > 0 else '卖出', position, price, msg)
        #     return
        # position == 0 则代表无需操作
        # 执行交易
        if direction == Direction.Long:
            if position == 0:
                return
            elif position > 0:
                if price is None:
                    price = md_dic['close']
                    # TODO: 稍后按盘口卖一档价格挂单

                self.open_long(instrument_id, price, position)
                self.logger.info("%s %s -> 开多 %d %.0f", instrument_id, msg, position, price)
            elif position < 0:
                if price is None:
                    price = md_dic['close']
                    # TODO: 稍后按盘口卖一档价格挂单
                position_net = -position
                self.close_long(instrument_id, price, position_net)
                self.logger.info("%s %s -> 平多 %d %.0f", instrument_id, msg, position_net, price)
        else:
            raise ValueError('目前不支持做空')
        self.instrument_lastest_order_datetime_dic[instrument_id] = datetime.now()

    def on_tick(self, md_dic, context):
        """
        tick级数据进行交易操作
        :param md_dic: 
        :param context: 
        :return: 
        """
        # self.logger.debug('get tick data: %s', md_dic)
        if self.target_position_dic is None or len(self.target_position_dic) == 0:
            return
        if self.datetime_last_update_position is None:
            logging.debug("尚未获取持仓数据，跳过")
            return

        # self.logger.debug('target_position_dic: %s', self.target_position_dic)
        symbol = md_dic['symbol']
        if symbol not in self.target_position_dic:
            return
        currency = self.trade_agent.get_currency(symbol)
        # self.logger.debug('target_position_dic[%s]: %s', symbol, self.target_position_dic[symbol])
        # 如果的当前合约近期存在交易回报，则交易回报时间一定要小于查询持仓时间：
        # 防止出现以及成交单持仓信息未及时更新导致的数据不同步问题
        if symbol in self.datetime_last_rtn_trade_dic:
            if currency not in self.datetime_last_update_position_dic:
                logging.debug("持仓数据中没有包含当前合约，最近一次成交回报时间：%s，跳过",
                              self.datetime_last_rtn_trade_dic[symbol])
                self.get_position(symbol, force_refresh=True)
                return
            if self.datetime_last_rtn_trade_dic[symbol] > self.datetime_last_update_position_dic[currency]:
                logging.debug("持仓数据尚未更新完成，最近一次成交回报时间：%s，最近一次持仓更新时间：%s",
                              self.datetime_last_rtn_trade_dic[symbol],
                              self.datetime_last_update_position_dic[currency])
                self.get_position(symbol, force_refresh=True)
                return

        # 过于密集执行可能会导致重复下单的问题
        if symbol in self.symbol_last_deal_datetime:
            last_deal_datetime = self.symbol_last_deal_datetime[symbol]
            if last_deal_datetime + self.timedelta_between_deal > datetime.now():
                # logging.debug("最近一次交易时间：%s，防止交易密度过大，跳过", last_deal_datetime)
                return

        with self._mutex:

            # 撤销所有相关订单
            self.cancel_order(symbol)

            # 计算目标仓位方向及交易数量
            position_date_pos_info_dic = self.get_position(symbol)
            if position_date_pos_info_dic is None:
                # 如果当前无持仓，直接按照目标仓位进行开仓动作
                if symbol not in self.target_position_dic:
                    # 当前无持仓，目标仓位也没有
                    pass
                else:
                    # 当前无持仓，直接按照目标仓位进行开仓动作
                    direction, currency, target_vol, symbol, price = self.target_position_dic[symbol]
                    self.do_order(md_dic, symbol, target_vol, price,
                                  msg='当前无持仓')
            else:
                # 如果当前有持仓
                # 比较当前持仓总量与目标仓位是否一致
                if symbol not in self.target_position_dic:
                    currency = self.trade_agent.get_currency(symbol)
                    # 如果当前有持仓，目标仓位为空，则当前持仓无论多空全部平仓
                    for position_date_type, pos_info_dic in position_date_pos_info_dic.items():
                        position = pos_info_dic['balance']
                        self.do_order(md_dic, symbol, -position,
                                      msg='目标仓位0，全部平仓')
                else:
                    # 如果当前有持仓，目标仓位也有持仓，则需要进一步比对
                    direction_target, currency_target, vol_target, symbol, price = self.target_position_dic[symbol]
                    # 汇总全部同方向持仓，如果不够目标仓位，则加仓
                    # 对全部的反方向持仓进行平仓
                    position_holding = 0
                    for position_date_type, pos_info_dic in position_date_pos_info_dic.items():
                        direction = Direction.Long
                        position = pos_info_dic['balance']
                        if direction != direction_target:
                            self.do_order(md_dic, symbol, -position, price,
                                          msg="目标仓位反向 %d，平仓" % position)
                            continue
                        else:
                            position_holding += position

                    # 如果持仓超过目标仓位，则平仓多出的部分，如果不足则补充多的部分
                    position_gap = vol_target - position_holding
                    if position_gap > 0:
                        # 如果不足则补充多的部分
                        self.do_order(md_dic, symbol, position_gap, price,
                                      msg="补充仓位")
                    elif position_gap < 0:
                        # 如果持仓超过目标仓位，则平仓多出的部分
                        self.do_order(md_dic, symbol, position_gap, price,
                                      msg="持仓超量，平仓 %d" % position_gap)

        # 更新最近执行时间
        self.symbol_last_deal_datetime[symbol] = datetime.now()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
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
            'instrument_id_list': ['ethusdt', 'eosusdt'],  # ['jm1711', 'rb1712', 'pb1801', 'IF1710'],
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
    stghandler = StgHandlerBase.factory(stg_class_obj=ReadFileStg,
                                        strategy_params=strategy_params,
                                        md_agent_params_list=md_agent_params_list,
                                        **run_mode_realtime_params)
    stghandler.start()
    time.sleep(120)
    stghandler.keep_running = False
    stghandler.join()
    logging.info("执行结束")
    # print(os.path.abspath(r'..\file_order'))
