# -*- coding: utf-8 -*-
"""
@author  : MG
@Time    : 2017/11/18
@author  : MG
@desc    : 监控csv文件（每15秒）
对比当前日期与回测csv文件中最新记录是否匹配，存在最新交易请求，生成交易请求（order*.csv文件）
追踪tick级行情实时成交（仅适合小资金）
追踪tick级行情本金买入点 * N % 止损
目前仅支持做多，不支持做空

每15秒进行一次文件检查
文件格式(csv xls)，每个symbol一行，不可重复，例：卖出eth 买入eos， 不考虑套利的情况（套利需要单独开发其他策略）
显示如下（非文件格式，csv文件以‘,’为分隔符，这个近为视觉好看，以表格形式显示）：
currency   symbol     weight  stop_loss_rate
eth        ethusdt    0.5        0.3
eos        eosusdt    0.5        0.4
"""
import re
import threading
import time
import logging
import pandas as pd
from datetime import datetime, timedelta, date
from abat.strategy import StgBase, StgHandlerBase
from abat.utils.fh_utils import str_2_date, get_folder_path
from config import Config
from abat.common import PeriodType, RunMode, BacktestTradeMode, Direction, PositionDateType
from collections import defaultdict
from backend import engine_md
from backend.orm import SymbolPair
from abat.utils.db_utils import with_db_session, get_db_session
from sqlalchemy import func
import os
import json
# 下面代码是必要的引用
# md_agent md_agent 并没有“显式”的被使用，但是在被引用期间，已经将相应的 agent 类注册到了相应的列表中
import agent.md_agent
import agent.td_agent

DEBUG = False
# "TradeBookCryptoCurrency2018-10-08.csv"
PATTERN_BACKTEST_FILE_NAME = re.compile(r'(?<=^TradeBookCryptoCurrency)[\d\-]{8,10}(?=.csv$)')
PATTERN_ORDER_FILE_NAME = re.compile(r'^order.*\.csv$')
PATTERN_FEEDBACK_FILE_NAME = re.compile(r'^feedback.*\.json$')


class TargetPosition:

    def __init__(self, direction: Direction, currency, symbol, weight=None, position=None, price=None,
                 stop_loss_rate=None, stop_loss_price=None, has_stop_loss=False, gap_threshold_vol=0.01):
        self.direction = direction
        self.currency = currency
        self.position = position
        self.symbol = symbol
        self.price = price
        self.stop_loss_rate = stop_loss_rate
        self.stop_loss_price = stop_loss_price
        self.has_stop_loss = has_stop_loss
        self.gap_threshold_vol = gap_threshold_vol
        self.weight = weight

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

    def to_dict(self):
        return {attr: getattr(self, attr) for attr in dir(self)
                if attr.find('_') != 0 and not callable(getattr(self, attr))}

    def __repr__(self):
        return f'<TargetPosition(symbol={self.symbol}, direction={int(self.direction)}, weight={self.weight},' \
               f'position={self.position}, price={self.price}, stop_loss_price={self.stop_loss_price}, ' \
               f'has_stop_loss={self.has_stop_loss}, gap_threshold_vol={self.gap_threshold_vol})>'


class ReadFileStg(StgBase):
    _folder_path = get_folder_path(r'file_order')

    def __init__(self, symbol_list=None):
        super().__init__()
        self.symbol_list = symbol_list
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
        self.weight = 1 if not DEBUG else 0.2  # 默认仓位权重
        self.stop_loss_rate = -0.03
        # 初始化 symbol 基本信息
        with with_db_session(engine_md) as session:
            symbol_info_list = session.query(SymbolPair).filter(
                func.concat(SymbolPair.base_currency, SymbolPair.quote_currency).in_(symbol_list)).all()
            self.symbol_info_dic = {symbol.base_currency+symbol.quote_currency: symbol for symbol in symbol_info_list}
        self.logger.info('接受订单文件目录：%s', self._folder_path)
        self.load_feedback_file()

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
            # 仅处理 order*.csv文件
            if PATTERN_ORDER_FILE_NAME.search(file_name) is None:
                continue
            self.logger.debug('处理文件 order 文件： %s', file_name)
            file_base_name, file_extension = os.path.splitext(file_name)
            file_path = os.path.join(self._folder_path, file_name)
            file_path_list.append(file_path)
            position_df_tmp = pd.read_csv(file_path)
            if position_df is None:
                position_df = position_df_tmp
            else:
                is_ok = True
                for col_name in ('currency', 'symbol', 'weight', 'stop_loss_rate'):
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
                backup_file_name = f"{file_base_name} {datetime.now().strftime('%Y-%m-%d %H_%M_%S')}" \
                                   f"{file_extension}.bak"
                os.rename(file_path, os.path.join(self._folder_path, backup_file_name))
                self.logger.info('备份 order 文件 %s -> %s', file_name, backup_file_name)

        return position_df, file_path_list

    def handle_backtest_file(self):
        """
        处理王淳的回测文件，生成相应的交易指令文件
        :return:
        """
        with self._mutex:
            # 获取文件列表
            file_name_list = os.listdir(self._folder_path)
            if file_name_list is None:
                # self.logger.info('No file')
                return
            # 读取所有 csv 文件
            for file_name in file_name_list:
                file_base_name, file_extension = os.path.splitext(file_name)
                # 仅处理 order*.csv文件
                m = PATTERN_BACKTEST_FILE_NAME.search(file_name)
                if m is None:
                    continue
                file_date_str = m.group()
                file_date = str_2_date(file_date_str)
                if file_date != date.today():
                    self.logger.warning('文件：%s 日期与当前系统日期 %s 不匹配，不予处理', file_name, date.today())
                    continue
                self.logger.debug('处理文件 %s 文件日期：%s', file_name, file_date_str)
                file_path = os.path.join(self._folder_path, file_name)
                data_df = pd.read_csv(file_path)
                if data_df is None or data_df.shape[0] == 0:
                    continue
                if str_2_date(data_df.iloc[-1]['Date']) != file_date:
                    self.logger.warning('文件：%s 回测记录中最新日期与当前文件日期 %s 不匹配，不予处理', file_name, file_date)
                    continue

                # 生成交易指令文件
                currency = data_df.iloc[-1]['InstruLong'].lower()
                order_dic = {
                    'currency': [currency],
                    'symbol': [f'{currency}usdt'],
                    'weight': [self.weight],
                    'stop_loss_rate': [self.stop_loss_rate],
                }
                order_file_name = f'order_{file_date_str}.csv'
                order_file_path = os.path.join(self._folder_path, order_file_name)
                order_df = pd.DataFrame(order_dic)
                order_df.to_csv(order_file_path)
                # 调试阶段暂时不重命名备份，不影响程序使用
                if not DEBUG:
                    # 文件备份
                    backup_file_name = f"{file_base_name} {datetime.now().strftime('%Y-%m-%d %H_%M_%S')}" \
                                       f"{file_extension}.bak"
                    os.rename(file_path, os.path.join(self._folder_path, backup_file_name))

    def handle_order_file(self):
        """
        获得目标持仓currency， 权重，止损点位
        生成相应交易指令
        另外，如果发现新的交易order文件，则将所有的 feedback 文件备份（根据新的order进行下单，生成新的feedback文件）
        :return:
        """
        with self._mutex:
            position_df, file_path_list = self.fetch_pos_by_file()
            if position_df is None or position_df.shape[0] == 0:
                return
            if len(self.symbol_latest_price_dic) == 0:
                self.logger.warning('当前程序没有缓存到有效的最新价格数据，交易指令暂缓执行')
                return
            # 如果存在新的 order 指令，则将所有的 feedback 文件备份（根据新的order进行下单，生成新的feedback文件）
            self.backup_feedback_files()
            self.logger.debug('仓位调整目标：\n%s', position_df)
            target_holding_dic = position_df.set_index('currency').dropna().to_dict('index')

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
                # if currency == 'hc':
                #     continue
                # 若持仓余额 小于 0.0001 则放弃清仓
                tot_balance = 0
                for _, dic in balance_dic.items():
                    tot_balance += dic['balance']
                if tot_balance < 0.0001:
                    continue

                symbol = self.get_symbol_by_currency(currency)
                if self.symbol_list is not None and symbol not in self.symbol_list:
                    self.logger.warning('%s 持仓： %.6f 不在当前订阅列表中，也不在目标持仓中，该持仓将不会被操作',
                                        symbol, tot_balance)
                    continue
                self.logger.info('计划卖出 %s', symbol)
                # TODO: 最小下单量在数据库中有对应信息，有待改进
                weight = 0
                target_vol, gap_threshold_vol, stop_loss_price = self.calc_vol_and_stop_loss_price(
                    symbol, weight, gap_threshold_precision=None)
                symbol_target_position_dic[symbol] = TargetPosition(
                    Direction.Long, currency, symbol,
                    weight=weight, position=0, gap_threshold_vol=gap_threshold_vol)
                is_all_fit_target = False

            # 生成目标持仓列表买入指令
            for num, (currency, position_dic) in enumerate(target_holding_dic.items()):
                weight = position_dic['weight']
                stop_loss_rate = position_dic['stop_loss_rate']
                # stop_loss_price = position_dic['stop_loss_rate']
                symbol = self.get_symbol_by_currency(currency)
                target_vol, gap_threshold_vol, stop_loss_price = self.calc_vol_and_stop_loss_price(
                    symbol, weight, stop_loss_rate)
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
                self.logger.info('计划买入 %s 目标仓位：%f 止损价：%f', symbol, target_vol, stop_loss_price)
                symbol_target_position_dic[symbol] = TargetPosition(
                    Direction.Long, currency, symbol,
                    weight=weight, position=target_vol, price=None,
                    stop_loss_rate=stop_loss_rate, stop_loss_price=stop_loss_price, gap_threshold_vol=gap_threshold_vol)

            symbol_target_position_dic_len = len(symbol_target_position_dic)
            if symbol_target_position_dic_len > 0:
                self.symbol_target_position_dic = symbol_target_position_dic
                self.logger.info('发现新的目标持仓指令：')
                self.logger_symbol_target_position_dic()
                # 生成 feedback 文件
                self.create_feedback_file()
            else:
                self.symbol_target_position_dic = None
                self.logger.debug('无仓位调整指令')

    def logger_symbol_target_position_dic(self):
        """
        展示当前目标持仓信息
        :return:
        """
        symbol_target_position_dic_len = len(self.symbol_target_position_dic)
        for num, (key, val) in enumerate(self.symbol_target_position_dic.items()):
            self.logger.info('%d/%d) %s, %r', num, symbol_target_position_dic_len, key, val)

    def on_timer(self):
        """
        每15秒进行一次文件检查
        1）检查王淳的回测文件，匹配最新日期 "TradeBookCryptoCurrency2018-10-08.csv" 中的日期是否与系统日期一致，如果一致则处理，生成“交易指令文件”
        2）生成相应目标仓位文件 order_2018-10-08.csv
        :param md_df:
        :param context: 
        :return: 
        """
        self.get_balance()
        self.handle_backtest_file()
        self.handle_order_file()

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

                # if DEBUG:
                #     # debug 模式下，价格不要真实成交，只要看一下有委托单就可以了
                #     price /= 2

                if stop_loss_price is not None and stop_loss_price > 0 and price <= stop_loss_price:
                    self.logger.warning('%s 当前价格 %.6f 已经触发止损价 %.6f 停止买入操作',
                                        instrument_id, price, stop_loss_price)
                    return

                self.open_long(instrument_id, price, order_vol)
                self.logger.info("%s %s -> 开多 %.4f 价格：%.4f", instrument_id, msg, order_vol, price)
            elif order_vol < 0:
                if price is None or price == 0:
                    price = md_dic['close']
                    # TODO: 稍后按盘口卖一档价格挂单

                # if DEBUG:
                #     # debug 模式下，价格不要真实成交，只要看一下有委托单就可以了
                #     price += price

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
            # self.logger.debug("当前 symbol='%s' 无操作", symbol)
            return
        if self.datetime_last_update_position is None:
            self.logger.debug("尚未获取持仓数据，跳过")
            return

        target_position = self.symbol_target_position_dic[symbol]
        # 权重为空，或者清仓的情况下，无需重新计算仓位
        if target_position.weight is not None and not(target_position.weight == 0 and target_position.position == 0):
            # target_position 为交易指令生产是产生的止损价格，无需浮动，否则永远无法止损了
            target_vol, gap_threshold_vol, stop_loss_price = self.calc_vol_and_stop_loss_price(
                symbol, target_position.weight, target_position.stop_loss_rate)
            target_position.position = target_vol
            target_position.gap_threshold_vol = gap_threshold_vol

        # target_currency = self.trade_agent.get_currency(symbol)
        target_currency = target_position.currency
        # self.logger.debug('target_position_dic[%s]: %s', symbol, self.target_position_dic[symbol])
        # 如果的当前合约近期存在交易回报，则交易回报时间一定要小于查询持仓时间：
        # 防止出现以及成交单持仓信息未及时更新导致的数据不同步问题
        if symbol in self.datetime_last_rtn_trade_dic:
            if target_currency not in self.datetime_last_update_position_dic:
                self.logger.debug("%s 持仓数据中没有包含当前合约，最近一次成交回报时间：%s，跳过",
                                  target_currency, self.datetime_last_rtn_trade_dic[symbol])
                self.get_position(symbol, force_refresh=True)
                # 此处可以不 return 因为当前火币交易所接口是同步返回持仓结果的
                # 不过为了兼容其他交易所，因此统一使用这种方式
                return
            if self.datetime_last_rtn_trade_dic[symbol] > self.datetime_last_update_position_dic[target_currency]:
                self.logger.debug("%s 持仓数据尚未更新完成，最近一次成交回报时间：%s > 最近一次持仓更新时间：%s",
                                  target_currency,
                                  self.datetime_last_rtn_trade_dic[symbol],
                                  self.datetime_last_update_position_dic[target_currency])
                self.get_position(symbol, force_refresh=True)
                # 此处可以不 return 因为当前火币交易所接口是同步返回持仓结果的
                # 不过为了兼容其他交易所，因此统一使用这种方式
                return

        # 过于密集执行可能会导致重复下单的问题
        if symbol in self.symbol_last_deal_datetime:
            last_deal_datetime = self.symbol_last_deal_datetime[symbol]
            if last_deal_datetime + self.timedelta_between_deal > datetime.now():
                # logging.debug("最近一次交易时间：%s，防止交易密度过大，跳过", last_deal_datetime)
                return

        with self._mutex:
            target_position.check_stop_loss(close_cur)
            # self.logger.debug("当前持仓目标：%r", target_position)
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
                # 比较当前持仓总量与目标仓位是否一致
                # 如果当前有持仓，目标仓位也有持仓，则需要进一步比对
                # target_direction, target_currency, target_position, symbol, target_price, \
                # stop_loss_price, has_stop_loss, gap_threshold_vol = self.get_target_position(symbol)
                if target_position.has_stop_loss:
                    if position_holding <= target_position.gap_threshold_vol:
                        self.logger.debug('当前 %s 持仓 %f -> %f 价格 %.6f 已处于止损状态，剩余仓位低于阀值 %f 无需进一步清仓',
                                          target_currency, position_holding, target_position.position, close_cur,
                                          target_position.gap_threshold_vol)
                    else:
                        self.logger.debug('当前 %s 持仓 %f -> %f 价格 %.6f 已处于止损状态',
                                          target_currency, position_holding, target_position.position, close_cur)
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
                        self.logger.debug('当前 %s 持仓 %f -> %f 价格 %.6f %s',
                                          target_currency, position_holding, target_position.position, close_cur, msg)
                        self.do_order(md_dic, symbol, position_gap, target_position.price,
                                      target_position.direction, target_position.stop_loss_price, msg=msg)
                    elif position_gap < - target_position.gap_threshold_vol:
                        if target_position.position == 0:
                            msg = '清仓'
                        else:
                            msg = "持仓超过目标仓位，减仓 %.4f" % position_gap
                        # 如果持仓超过目标仓位，则平仓多出的部分
                        self.logger.debug('当前 %s 持仓 %f -> %f 价格 %.6f %s',
                                          target_currency, position_holding, target_position.position, close_cur, msg)
                        self.do_order(md_dic, symbol, position_gap, target_position.price,
                                      target_position.direction, target_position.stop_loss_price, msg=msg)
                    else:
                        self.logger.debug('当前 %s 持仓 %f -> %f 差距 %f 小于最小调整范围 %f，忽略此调整',
                                          target_currency, position_holding, target_position.position, position_gap,
                                          target_position.gap_threshold_vol)

        # 更新最近执行时间
        self.symbol_last_deal_datetime[symbol] = datetime.now()

    def get_symbol_by_currency(self, currency):
        """目前暂时仅支持currency 与 usdt 之间转换"""
        return currency + 'usdt'

    def calc_vol_and_stop_loss_price(self, symbol, weight, stop_loss_rate=None, gap_threshold_precision: (int, None)=2):
        """
        根据权重及当前账号总市值，计算当前 symbol 对应多少 vol, 根据 stop_loss_rate 计算止损价格(目前仅考虑做多的情况)
        :param symbol:
        :param weight:
        :param stop_loss_rate: 为空则不计算
        :param gap_threshold_precision: 为了避免反复调整，造成手续费摊高，设置最小调整精度, None 则使用当前Symbol默认值
        :return:
        """
        if gap_threshold_precision is None:
            gap_threshold_precision = self.symbol_info_dic[symbol].amount_precision
        holding_currency_dic = self.get_holding_currency(exclude_usdt=False)
        # tot_value = sum([dic['balance'] * self.symbol_latest_price_dic[self.get_symbol_by_currency(currency)]
        #                  for currency, dic in holding_currency_dic.items()])
        if symbol not in self.symbol_latest_price_dic or self.symbol_latest_price_dic[symbol] == 0:
            self.logger.error('%s 没有找到有效的最新价格', symbol)
            price_latest = None
            weight_vol = None
            gap_threshold_vol = None
            stop_loss_price = None
        else:
            tot_value = 0
            for currency, dic in holding_currency_dic.items():
                for pos_date_type, dic_sub in dic.items():
                    if currency == 'usdt':
                        tot_value += dic_sub['balance']
                    else:
                        tot_value += dic_sub['balance'] * self.symbol_latest_price_dic[
                            self.get_symbol_by_currency(currency)]

            price_latest = self.symbol_latest_price_dic[symbol]
            weight_vol = tot_value * weight / price_latest
            gap_threshold_vol = None if gap_threshold_precision is None else \
                tot_value * (0.1 ** gap_threshold_precision) / price_latest
            stop_loss_price = None if stop_loss_rate is None else \
                price_latest * (1 + stop_loss_rate)

        self.logger.debug('%s price_latest=%.4f, weight_vol=%f [%.1f%%], gap_threshold_vol=%.4f, stop_loss_price=%.4f',
                          symbol,
                          0 if price_latest is None else price_latest,
                          0 if weight_vol is None else weight_vol,
                          0 if weight is None else (weight * 100),
                          0 if gap_threshold_vol is None else gap_threshold_vol,
                          0 if stop_loss_price is None else stop_loss_price,
                          )
        return weight_vol, gap_threshold_vol, stop_loss_price

    def get_target_position(self, symbol):
        dic = self.symbol_target_position_dic[symbol]
        return dic['direction'], dic['currency'], dic['position'], dic['symbol'], \
               dic['price'], dic['stop_loss_price'], dic.setdefault('has_stop_loss', False), \
               dic.setdefault('gap_threshold_vol', None)

    def backup_feedback_files(self):
        """
        将所有的 feedback 文件备份
        :return:
        """
        # 获取文件列表
        file_name_list = os.listdir(self._folder_path)
        if file_name_list is None:
            # self.logger.info('No file')
            return

        for file_name in file_name_list:
            # 仅处理 feedback*.csv文件
            if PATTERN_FEEDBACK_FILE_NAME.search(file_name) is None:
                continue
            file_base_name, file_extension = os.path.splitext(file_name)
            file_path = os.path.join(self._folder_path, file_name)
            # 文件备份
            backup_file_name = f"{file_base_name} {datetime.now().strftime('%Y-%m-%d %H_%M_%S')}" \
                               f"{file_extension}.bak"
            os.rename(file_path, os.path.join(self._folder_path, backup_file_name))
            self.logger.info('备份 Feedback 文件 %s -> %s', file_name, backup_file_name)

    def create_feedback_file(self):
        """
        根据 symbol_target_position_dic 创建 feedback 文件
        :return:
        """
        symbol_target_position_dic = self.symbol_target_position_dic
        data_dic = {}
        for key, val in symbol_target_position_dic.items():
            val_dic = val.to_dict()
            val_dic['direction'] = int(val_dic['direction'])
            data_dic[key] = val_dic

        file_name = f"feedback_{datetime.now().strftime('%Y-%m-%d %H_%M_%S')}.json"
        file_path = os.path.join(self._folder_path, file_name)
        with open(file_path, 'w') as file:
            json.dump(data_dic, file)
        self.logger.info('生产 feedback 文件：%s', file_name)
        return file_path

    def load_feedback_file(self):
        """
        加载 feedback 文件，更新 self.symbol_target_position_dic
        :return:
        """
        # 获取文件列表
        file_name_list = os.listdir(self._folder_path)
        if file_name_list is None or len(file_name_list) == 0:
            # self.logger.info('No file')
            return
        # 读取所有 csv 文件
        for file_name in file_name_list:
            # 仅处理 order*.csv文件
            if PATTERN_FEEDBACK_FILE_NAME.search(file_name) is None:
                continue
            self.logger.debug('处理文件 feedback文件： %s', file_name)
            file_path = os.path.join(self._folder_path, file_name)

            with open(file_path) as file:
                data_dic = json.load(file)
            # 构建 symbol_target_position_dic 对象
            symbol_target_position_dic = {}
            for key, val in data_dic.items():
                val['direction'] = Direction(val['direction'])
                symbol_target_position_dic[key] = TargetPosition(**val)

            self.symbol_target_position_dic = symbol_target_position_dic
            self.logger.info('加载 feedback 文件：%s', file_name)
            self.logger_symbol_target_position_dic()
            break
        else:
            logging.info('没有可用的 feedback 文件可加载')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
    DEBUG = False
    symbol_list = ['ethusdt', 'eosusdt']
    # 参数设置
    strategy_params = {'symbol_list': symbol_list}
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
            'instrument_id_list': symbol_list,  #
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
        time.sleep(180)
        stghandler.keep_running = False
        stghandler.join()

    logging.info("执行结束")
    # print(os.path.abspath(r'..\file_order'))
