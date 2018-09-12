# -*- coding: utf-8 -*-
"""
@author  : MG
@Time    : 2017/11/18
@author  : MG
@desc    : 接受文件订单
追踪tick级行情实时成交（仅适合小资金）
追踪tick级行情固定点位止损
目前仅支持做多，不支持做空

每15秒进行一次文件检查
文件格式(csv xls)，每个symbol一行，不可重复，例：卖出eth 买入eos， 不考虑套利的情况（套利需要单独开发其他策略）
显示如下（非文件格式，csv文件以‘,’为分隔符，这个近为视觉好看，以表格形式显示）：
currency   symbol     weight  stop_loss_price
eth        ethusdt    0.5        2.5
eos        eosusdt    0.5        200
"""
import threading
import time
import logging
import pandas as pd
from datetime import datetime, timedelta
from abat.strategy import StgBase, StgHandlerBase
from config import Config
from abat.common import PeriodType, RunMode, BacktestTradeMode, Direction, PositionDateType
from collections import defaultdict
import os
# 下面代码是必要的引用
# md_agent md_agent 并没有“显式”的被使用，但是在被引用期间，已经将相应的 agent 类注册到了相应的列表中
import agent.md_agent
import agent.td_agent

DEBUG = False


class TargetPosition:

    def __init__(self, direction, currency, position, symbol,
                 price=None, stop_loss_price=None, has_stop_loss=False, gap_threshold_vol=None):
        self.direction = direction
        self.currency = currency
        self.position = position
        self.symbol = symbol
        self.price = price
        self.stop_loss_price = stop_loss_price
        self.has_stop_loss = has_stop_loss
        self.gap_threshold_vol = gap_threshold_vol

    def check_stop_loss(self, close):
        """
        根据当前价格计算是否已经到达止损点位
        如果此前已经到达过止损点位则不再比较，也不需重置状态
        :param close:
        :return:
        """
        # 如果此前已经到达过止损点位则不再比较，也不需重置状态
        if self.stop_loss_price is None or self.has_stop_loss:
            return
        self.has_stop_loss = (self.direction == Direction.Long and close < self.stop_loss_price) or (
                self.direction == Direction.Short and close > self.stop_loss_price)
        if self.has_stop_loss:
            logging.warning('%s 处于止损状态。止损价格 %f 当前价格 %f', self.symbol, self.stop_loss_price, close)

    def get_target_position(self):
        return self.direction, self.currency, self.position, self.symbol, \
               self.price, self.stop_loss_price, self.has_stop_loss, \
               self.gap_threshold_vol


class ReadFileStg(StgBase):
    _folder_path = os.path.abspath(r'.\file_order')

    def __init__(self):
        super().__init__()
        self._mutex = threading.Lock()
        self._last_check_datetime = datetime.now() - timedelta(minutes=1)
        self.interval_timedelta = timedelta(seconds=15)
        self.symbol_target_position_dic = {}
        # 设定相应周期的事件驱动句柄 接收的参数类型
        self._on_period_event_dic[PeriodType.Tick].param_type = dict
        # 记录合约最近一次执行操作的时间
        self.symbol_last_deal_datetime = {}
        # 记录合约最近一个发送买卖请求的时间
        self.instrument_lastest_order_datetime_dic = {}
        # 目前由于交易是异步执行，在尚未记录每一笔订单的情况下，时间太短可能会导致仓位与请求但出现不同步现象，导致下单过多的问题
        self.timedelta_between_deal = timedelta(seconds=3)
        self.min_order_vol = 0.1
        self.symbol_latest_price_dic = defaultdict(float)

    def fetch_pos_by_file(self):
        """读取仓位配置csv文件，返回目标仓位DataFrame"""
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
        file_path_list = []
        for file_name in file_name_list:
            file_base_name, file_extension = os.path.splitext(file_name)
            if file_extension.lower() != '.csv':
                continue
            file_path = os.path.join(self._folder_path, file_name)
            file_path_list.append(file_path)
            position_df_tmp = pd.read_csv(file_path)
            if position_df is None:
                position_df = position_df_tmp
            else:
                is_ok = True
                for col_name in ('currency', 'symbol', 'weight', 'stop_loss_price'):
                    if col_name not in position_df_tmp.columns:
                        is_ok = False
                        self.logger.error('%s 文件格式不正确，缺少 %s 列数据', file_name, col_name)
                        break

                if not is_ok:
                    continue
                position_df = position_df.append(position_df_tmp)

            # 调试阶段暂时不重命名备份，不影响程序使用
            if not DEBUG:
                # 文件备份
                backup_file_name = file_base_name + datetime.now().strftime(
                    '%Y-%m-%d %H_%M_%S') + file_extension + '.bak'
                os.rename(file_path, os.path.join(self._folder_path, backup_file_name))

        return position_df, file_path_list

    def on_timer(self):
        """
        每15秒进行一次文件检查
        获得目标持仓currency， 权重，止损点位
        生成相应交易指令
        :param md_df:
        :param context: 
        :return: 
        """
        with self._mutex:
            position_df, file_path_list = self.fetch_pos_by_file()
            if position_df is None or position_df.shape[0] == 0:
                return
            self.logger.debug('仓位调整目标：\n%s', position_df)
            target_holding_dic = position_df.set_index('currency').dropna().to_dict('index')
            if len(self.symbol_latest_price_dic) == 0:
                self.logger.warning('当前程序没有缓存到有效的最新价格数据，交易指令暂缓执行')
                return

            # {currency: (Direction, currency, target_position, symbol, target_price, stop_loss_price)
            symbol_target_position_dic = {}
            # 检查目标仓位与当前持仓是否相符，否则执行相应交易
            target_currency_set = set(list(position_df['currency']))
            holding_currency_dic = self.get_holding_currency()
            # 检查是否所有持仓符合目标配置文件要求
            is_all_fit_target = True

            # 如果当前 currency 不在目标持仓列表里面，则卖出
            for num, (currency, balance_dic) in enumerate(holding_currency_dic.items(), start=1):
                # currency 在目标持仓中，无需清仓
                if currency in target_currency_set:
                    continue
                # hc 为 货币交易所的一种手续费代币工具，不做交易使用
                if currency == 'hc':
                    continue
                # 若持仓余额 小于 0.0001 则放弃清仓
                tot_balance = 0
                for _, dic in balance_dic.items():
                    tot_balance += dic['balance']
                if tot_balance < 0.0001:
                    continue

                symbol = self.get_symbol_by_currency(currency)
                symbol_target_position_dic[symbol] = TargetPosition(Direction.Long, currency, 0, symbol)
                is_all_fit_target = False

            # 生成目标持仓列表买入指令
            for num, (currency, position_dic) in enumerate(target_holding_dic.items()):
                weight = position_dic['weight']
                stop_loss_price = position_dic['stop_loss_price']
                symbol = self.get_symbol_by_currency(currency)
                target_vol, gap_threshold_vol = self.calc_vol(symbol, weight)
                if target_vol is None:
                    self.logger.warning('%s 持仓权重 %.2f %% 无法计算目标持仓量', currency, weight * 100)
                    continue
                # 检查当前持仓是否与目标持仓一致，如果一致则跳过
                # position_date_pos_info_dic = self.get_position(symbol)
                # if position_date_pos_info_dic is not None and len(position_date_pos_info_dic) > 0:
                #     # 有持仓，比较是否满足目标仓位，否则下指令
                #     position_cur = sum([pos_info['balance'] for pos_info in position_date_pos_info_dic.values()])
                #     position_gap = target_vol - position_cur
                #     # 实盘情况下，很少绝对一致，在一定区间内即可
                #     if position_gap > gap_threshold_vol:
                #         # 当前合约累计持仓与目标持仓不一致，则添加目标持仓任务
                #         is_all_fit_target = False
                # else:
                #     is_all_fit_target = False
                # 无论仓位是否存在，均生成交易指令，待交易执行阶段进行比较（以上代码不影响是否生产建仓指令）

                # 多头目标持仓
                symbol_target_position_dic[symbol] = TargetPosition(Direction.Long, currency, target_vol, symbol,
                                                                    None, stop_loss_price,
                                                                    gap_threshold_vol=gap_threshold_vol)

            # if is_all_fit_target:
            #     # 文件备份 file_path_list
            #     for file_path in file_path_list:
            #         file_base_name_with_path, file_extension = os.path.split(file_path)
            #         backup_file_path = file_base_name_with_path + datetime.now().strftime(
            #             '%Y-%m-%d %H_%M_%S') + file_extension + '.bak'
            #         # 调试阶段暂时不重命名备份，不影响程序使用
            #         os.rename(file_path, backup_file_path)
            #         self.logger.info('备份仓位配置文件：%s -> %s', file_path, backup_file_path)
            # el
            if len(symbol_target_position_dic) > 0:
                self.symbol_target_position_dic = symbol_target_position_dic
                self.logger.info('发现新的目标持仓指令\n%s', symbol_target_position_dic)
            else:
                self.symbol_target_position_dic = None
                self.logger.debug('无仓位调整指令')

    def do_order(self, md_dic, instrument_id, order_vol, price=None, direction=Direction.Long, stop_loss_price=0,
                 msg=""):
        # if True:
        #     self.logger.info("%s %s %f 价格 %f [%s]",
        #                      instrument_id, '买入' if position > 0 else '卖出', position, price, msg)
        #     return
        # position == 0 则代表无需操作
        # 执行交易
        if direction == Direction.Long:
            if order_vol == 0:
                return
            elif order_vol > 0:
                if price is None or price == 0:
                    price = md_dic['close']
                    # TODO: 稍后按盘口卖一档价格挂单

                if DEBUG:
                    # debug 模式下，价格不要真实成交，只要看一下有委托单就可以了
                    price /= 2

                if stop_loss_price is not None and stop_loss_price > 0 and price <= stop_loss_price:
                    self.logger.warning('已经出发止损价 %.6f 停止买入操作', stop_loss_price)
                    return

                self.open_long(instrument_id, price, order_vol)
                self.logger.info("%s %s -> 开多 %.4f 价格：%.4f", instrument_id, msg, order_vol, price)
            elif order_vol < 0:
                if price is None or price == 0:
                    price = md_dic['close']
                    # TODO: 稍后按盘口卖一档价格挂单

                if DEBUG:
                    # debug 模式下，价格不要真实成交，只要看一下有委托单就可以了
                    price += price

                order_vol_net = -order_vol
                self.close_long(instrument_id, price, order_vol_net)
                self.logger.info("%s %s -> 平多 %.4f 价格：%.4f", instrument_id, msg, order_vol_net, price)
        else:
            raise ValueError('目前不支持做空')
        self.instrument_lastest_order_datetime_dic[instrument_id] = datetime.now()

    def on_tick(self, md_dic, context):
        """
        tick 级数据进行交易操作
        :param md_dic: 
        :param context: 
        :return: 
        """
        # self.logger.debug('get tick data: %s', md_dic)
        symbol = md_dic['symbol']
        # 更新最新价格
        close_cur = md_dic['close']
        self.symbol_latest_price_dic[symbol] = close_cur
        # 计算是否需要进行调仓操作
        if self.symbol_target_position_dic is None or symbol not in self.symbol_target_position_dic:
            return
        if self.datetime_last_update_position is None:
            logging.debug("尚未获取持仓数据，跳过")
            return

        target_currency = self.trade_agent.get_currency(symbol)
        # self.logger.debug('target_position_dic[%s]: %s', symbol, self.target_position_dic[symbol])
        # 如果的当前合约近期存在交易回报，则交易回报时间一定要小于查询持仓时间：
        # 防止出现以及成交单持仓信息未及时更新导致的数据不同步问题
        if symbol in self.datetime_last_rtn_trade_dic:
            if target_currency not in self.datetime_last_update_position_dic:
                logging.debug("持仓数据中没有包含当前合约，最近一次成交回报时间：%s，跳过",
                              self.datetime_last_rtn_trade_dic[symbol])
                self.get_position(symbol, force_refresh=True)
                return
            if self.datetime_last_rtn_trade_dic[symbol] > self.datetime_last_update_position_dic[target_currency]:
                logging.debug("持仓数据尚未更新完成，最近一次成交回报时间：%s 晚于 最近一次持仓更新时间：%s",
                              self.datetime_last_rtn_trade_dic[symbol],
                              self.datetime_last_update_position_dic[target_currency])
                self.get_position(symbol, force_refresh=True)
                return

        # 过于密集执行可能会导致重复下单的问题
        if symbol in self.symbol_last_deal_datetime:
            last_deal_datetime = self.symbol_last_deal_datetime[symbol]
            if last_deal_datetime + self.timedelta_between_deal > datetime.now():
                # logging.debug("最近一次交易时间：%s，防止交易密度过大，跳过", last_deal_datetime)
                return

        with self._mutex:
            target_position = self.symbol_target_position_dic[symbol]
            target_position.check_stop_loss(close_cur)

            # 撤销所有相关订单
            self.cancel_order(symbol)

            # 计算目标仓位方向及交易数量
            position_date_pos_info_dic = self.get_position(symbol)
            if position_date_pos_info_dic is None:
                # 无当前持仓，有目标仓位，直接按照目标仓位进行开仓动作
                # target_direction, target_currency, target_position, symbol, target_price, \
                # stop_loss_price, has_stop_loss, gap_threshold_vol = self.get_target_position(symbol)
                if not target_position.has_stop_loss:
                    self.do_order(md_dic, symbol, target_position.position, target_position.price,
                                  target_position.direction, target_position.stop_loss_price, msg='当前无持仓')
            else:
                # 如果当前有持仓，执行两类动作：
                # 1）若 当前持仓与目标持仓不匹配，则进行相应的调仓操作
                # 2）若 当前持仓价格超出止损价位，则进行清仓操作

                position_holding = sum(
                    [pos_info_dic['balance'] for pos_info_dic in position_date_pos_info_dic.values()])
                self.logger.debug('当前 %s 持仓 %f', target_position.currency, position_holding)
                # 比较当前持仓总量与目标仓位是否一致
                # 如果当前有持仓，目标仓位也有持仓，则需要进一步比对
                # target_direction, target_currency, target_position, symbol, target_price, \
                # stop_loss_price, has_stop_loss, gap_threshold_vol = self.get_target_position(symbol)
                if target_position.has_stop_loss:
                    # 已经触发止损，如果依然有持仓，则进行持续清仓操作
                    self.do_order(md_dic, symbol, -position_holding, None,
                                  target_position.direction, msg="止损")
                else:
                    # 汇总全部同方向持仓，如果不够目标仓位，则加仓
                    # 对全部的反方向持仓进行平仓

                    # 如果持仓超过目标仓位，则平仓多出的部分，如果不足则补充多的部分
                    position_gap = target_position.position - position_holding
                    if position_gap > target_position.gap_threshold_vol:
                        if position_holding < target_position.gap_threshold_vol:
                            msg = '建仓'
                        else:
                            msg = "补充仓位"
                        # 如果不足则补充多的部分
                        self.do_order(md_dic, symbol, position_gap, target_position.price,
                                      target_position.direction, target_position.stop_loss_price, msg=msg)
                    elif position_gap < - target_position.gap_threshold_vol:
                        if target_position.position == 0:
                            msg = '清仓'
                        else:
                            msg = "持仓超过目标仓位，减仓 %.4f" % position_gap
                        # 如果持仓超过目标仓位，则平仓多出的部分
                        self.do_order(md_dic, symbol, position_gap, target_position.price,
                                      target_position.direction, target_position.stop_loss_price, msg=msg)
                    else:
                        self.logger.debug('当前持仓 %f 与目标持仓%f 差距 %f 过小，忽略此调整',
                                          position_holding, target_position.position, position_gap)

        # 更新最近执行时间
        self.symbol_last_deal_datetime[symbol] = datetime.now()

    def get_symbol_by_currency(self, currency):
        """目前暂时仅支持currency 与 usdt 之间转换"""
        return currency + 'usdt'

    def calc_vol(self, symbol, weight, gap_threshold_precision=0.01):
        """
        根据权重及当前账号总市值，计算当前 symbol 对应多少 vol
        :param symbol:
        :param weight:
        :return:
        """
        holding_currency_dic = self.get_holding_currency(exclude_usdt=False)
        # tot_value = sum([dic['balance'] * self.symbol_latest_price_dic[self.get_symbol_by_currency(currency)]
        #                  for currency, dic in holding_currency_dic.items()])
        if symbol not in self.symbol_latest_price_dic or self.symbol_latest_price_dic[symbol] == 0:
            self.logger.error('%s 没有找到有效的最新价格', symbol)
            weight_vol = None
            gap_threshold_vol = None
        else:
            tot_value = 0
            for currency, dic in holding_currency_dic.items():
                for pos_date_type, dic_sub in dic.items():
                    if currency == 'usdt':
                        tot_value += dic_sub['balance']
                    else:
                        tot_value += dic_sub['balance'] * self.symbol_latest_price_dic[
                            self.get_symbol_by_currency(currency)]

            weight_vol = tot_value * weight / self.symbol_latest_price_dic[symbol]
            gap_threshold_vol = tot_value * gap_threshold_precision / self.symbol_latest_price_dic[symbol]

        return weight_vol, gap_threshold_vol

    def get_target_position(self, symbol):
        dic = self.symbol_target_position_dic[symbol]
        return dic['direction'], dic['currency'], dic['position'], dic['symbol'], \
               dic['price'], dic['stop_loss_price'], dic.setdefault('has_stop_loss', False), \
               dic.setdefault('gap_threshold_vol', None)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
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
        stg_class_obj=ReadFileStg,
        strategy_params=strategy_params,
        md_agent_params_list=md_agent_params_list,
        **run_mode_realtime_params)
    if DEBUG:
        stghandler.run()
    else:
        # 开始执行策略
        stghandler.start()
        # 策略执行 2 分钟后关闭
        time.sleep(120)
        stghandler.keep_running = False
        stghandler.join()

    logging.info("执行结束")
    # print(os.path.abspath(r'..\file_order'))
