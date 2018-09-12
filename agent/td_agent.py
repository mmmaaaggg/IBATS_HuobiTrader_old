# -*- coding: utf-8 -*-
"""
Created on 2017/10/3
@author: MG
"""
import logging
from collections import OrderedDict
from datetime import datetime, timedelta, date
from huobitrade import setKey
from abat.backend.orm import OrderInfo, TradeInfo, PosStatusInfo, AccountStatusInfo
from config import Config
from abat.utils.db_utils import with_db_session
from abat.backend import engine_abat
from abat.common import Direction, Action, BacktestTradeMode, PositionDateType
from abat.utils.fh_utils import try_n_times, ceil, floor
from abat.trade import TraderAgent, register_backtest_trader_agent, register_realtime_trader_agent
from huobitrade.service import HBRestAPI
from backend import engine_md
from backend.orm import SymbolPair
from collections import defaultdict
from enum import Enum

logger = logging.getLogger()
# 设置秘钥
setKey(Config.EXCHANGE_ACCESS_KEY, Config.EXCHANGE_SECRET_KEY)


class OrderType(Enum):
    """
    buy-market：市价买, sell-market：市价卖, buy-limit：限价买, sell-limit：限价卖, buy-ioc：IOC买单, sell-ioc：IOC卖单
    """
    buy_market = 'buy-market'
    sell_market = 'sell-market'
    buy_limit = 'buy-limit'
    sell_limit = 'sell-limit'
    buy_ioc = 'buy-ioc'
    sell_ioc = 'sell-ioc'


@register_backtest_trader_agent
class BacktestTraderAgent(TraderAgent):
    """
    供调用模拟交易接口使用
    """

    def __init__(self, stg_run_id, run_mode_params: dict):
        super().__init__(stg_run_id, run_mode_params)
        # 标示 order 成交模式
        self.trade_mode = run_mode_params.setdefault('trade_mode', BacktestTradeMode.Order_2_Deal)
        # 账户初始资金
        self.init_cash = run_mode_params['init_cash']
        # 用来标示当前md，一般执行买卖交易是，对时间，价格等信息进行记录
        self.curr_md_period_type = None
        self.curr_md = None
        # 用来保存历史的 order_info trade_info pos_status_info account_info
        self.order_info_list = []
        self.trade_info_list = []
        self.pos_status_info_dic = OrderedDict()
        self.account_info_list = []
        # 持仓信息 初始化持仓状态字典，key为 instrument_id
        self._pos_status_info_dic = {}
        self._order_info_dic = {}
        # 账户信息
        self._account_status_info = None

    def set_curr_md(self, period_type, md):
        self.curr_md_period_type = period_type
        self.curr_md = md

    def connect(self):
        pass

    def _save_order_info(self, instrument_id, price: float, vol: int, direction: Direction, action: Action):
        order_date = self.curr_md['ts_start'].date()
        order_info = OrderInfo(stg_run_id=self.stg_run_id,
                               order_date=order_date,
                               order_time=self.curr_md['ts_start'].time(),
                               order_millisec=0,
                               direction=int(direction),
                               action=int(action),
                               instrument_id=instrument_id,
                               order_price=float(price),
                               order_vol=int(vol)
                               )
        if False:  # 暂时不用
            with with_db_session(engine_abat, expire_on_commit=False) as session:
                session.add(order_info)
                session.commit()
        self.order_info_list.append(order_info)
        self._order_info_dic.setdefault(instrument_id, []).append(order_info)
        # 更新成交信息
        # Order_2_Deal 模式：下单即成交
        if self.trade_mode == BacktestTradeMode.Order_2_Deal:
            self._save_trade_info(order_info)

    def _save_trade_info(self, order_info: OrderInfo):
        """
        根据订单信息保存成交结果
        :param order_info: 
        :return: 
        """
        trade_info = TradeInfo.create_by_order_info(order_info)
        self.trade_info_list.append(trade_info)
        # 更新持仓信息
        self._save_pos_status_info(trade_info)

    def _save_pos_status_info(self, trade_info: TradeInfo) -> AccountStatusInfo:
        """
        根据成交信息保存最新持仓信息
        :param trade_info: 
        :return: 
        """
        instrument_id = trade_info.instrument_id
        if instrument_id in self._pos_status_info_dic:
            pos_status_info_last = self._pos_status_info_dic[instrument_id]
            pos_status_info = pos_status_info_last.update_by_trade_info(trade_info)
        else:
            pos_status_info = PosStatusInfo.create_by_trade_info(trade_info)
        # 更新
        trade_date, trade_time, trade_millisec = \
            pos_status_info.trade_date, pos_status_info.trade_time, pos_status_info.trade_millisec
        self.pos_status_info_dic[(trade_date, trade_time, trade_millisec)] = pos_status_info
        self._pos_status_info_dic[instrument_id] = pos_status_info
        # self.c_save_acount_info(pos_status_info)

    def _create_account_status_info(self) -> AccountStatusInfo:
        stg_run_id, init_cash, md = self.stg_run_id, self.init_cash, self.curr_md
        trade_date = md['ts_start'].date()
        trade_time = md['ts_start'].time()
        trade_millisec = 0
        # trade_price = float(self.curr_md['close'])
        acc_status_info = AccountStatusInfo(stg_run_id=stg_run_id,
                                            trade_date=trade_date,
                                            trade_time=trade_time,
                                            trade_millisec=trade_millisec,
                                            available_cash=init_cash,
                                            balance_tot=init_cash,
                                            )
        if Config.UPDATE_OR_INSERT_PER_ACTION:
            # 更新最新持仓纪录
            with with_db_session(engine_abat, expire_on_commit=False) as session:
                session.add(acc_status_info)
                session.commit()
        return acc_status_info

    def _update_by_pos_status_info(self) -> AccountStatusInfo:
        """根据 持仓列表更新账户信息"""

        pos_status_info_dic, md = self._pos_status_info_dic, self.curr_md

        account_status_info = self._account_status_info.create_by_self()
        # 上一次更新日期、时间
        # trade_date_last, trade_time_last, trade_millisec_last = \
        #     account_status_info.trade_date, account_status_info.trade_time, account_status_info.trade_millisec
        # 更新日期、时间
        trade_date = md['ts_start'].date()
        trade_time = md['ts_start'].time()
        trade_millisec = 0

        available_cash_chg = 0
        curr_margin = 0
        close_profit = 0
        position_profit = 0
        floating_pl_chg = 0
        margin_chg = 0
        floating_pl_cum = 0
        for instrument_id, pos_status_info in pos_status_info_dic.items():
            curr_margin += pos_status_info.margin
            if pos_status_info.position == 0:
                close_profit += pos_status_info.floating_pl
            else:
                position_profit += pos_status_info.floating_pl
            floating_pl_chg += pos_status_info.floating_pl_chg
            margin_chg += pos_status_info.margin_chg
            floating_pl_cum += pos_status_info.floating_pl_cum

        available_cash_chg = floating_pl_chg - margin_chg
        account_status_info.curr_margin = curr_margin
        # # 对于同一时间，平仓后又开仓的情况，不能将close_profit重置为0
        # if trade_date == trade_date_last and trade_time == trade_time_last and trade_millisec == trade_millisec_last:
        #     account_status_info.close_profit += close_profit
        # else:
        # 一个单位时段只允许一次，不需要考虑上面的情况
        account_status_info.close_profit = close_profit

        account_status_info.position_profit = position_profit
        account_status_info.available_cash += available_cash_chg
        account_status_info.floating_pl_cum = floating_pl_cum
        account_status_info.balance_tot = account_status_info.available_cash + curr_margin

        account_status_info.trade_date = trade_date
        account_status_info.trade_time = trade_time
        account_status_info.trade_millisec = trade_millisec
        if Config.UPDATE_OR_INSERT_PER_ACTION:
            # 更新最新持仓纪录
            with with_db_session(engine_abat, expire_on_commit=False) as session:
                session.add(account_status_info)
                session.commit()
        return account_status_info

    def _update_pos_status_info_by_md(self, pos_status_info_last) -> PosStatusInfo:
        """创建新的对象，根据 trade_info 更新相关信息"""
        md = self.curr_md
        trade_date = md['ts_start'].date()
        trade_time = md['ts_start'].time()
        trade_millisec = 0
        trade_price = float(md['close'])
        instrument_id = md['pair']

        pos_status_info = pos_status_info_last.create_by_self()
        pos_status_info.cur_price = trade_price
        pos_status_info.trade_date = trade_date
        pos_status_info.trade_time = trade_time
        pos_status_info.trade_millisec = trade_millisec

        # 计算 floating_pl margin
        # instrument_info = Config.instrument_info_dic[instrument_id]
        # multiple = instrument_info['VolumeMultiple']
        # margin_ratio = instrument_info['LongMarginRatio']
        multiple, margin_ratio = 1, 1
        position = pos_status_info.position
        cur_price = pos_status_info.cur_price
        avg_price = pos_status_info.avg_price
        pos_status_info.margin = position * cur_price * multiple * margin_ratio
        pos_status_info.margin_chg = pos_status_info.margin - pos_status_info_last.margin
        if pos_status_info.direction == Direction.Long:
            pos_status_info.floating_pl = (cur_price - avg_price) * position * multiple
        else:
            pos_status_info.floating_pl = (avg_price - cur_price) * position * multiple
        pos_status_info.floating_pl_chg = pos_status_info.floating_pl - pos_status_info_last.floating_pl
        pos_status_info.floating_pl_cum += pos_status_info.floating_pl_chg

        if Config.UPDATE_OR_INSERT_PER_ACTION:
            # 更新最新持仓纪录
            with with_db_session(engine_abat, expire_on_commit=False) as session:
                session.add(pos_status_info)
                session.commit()
        return pos_status_info

    def update_account_info(self):
        """
        更新 持仓盈亏数据 汇总统计当前周期账户盈利情况
        :return: 
        """
        if self.curr_md is None:
            return
        if self._account_status_info is None:
            # self._account_status_info = AccountStatusInfo.create(self.stg_run_id, self.init_cash, self.curr_md)
            self._account_status_info = self._create_account_status_info()
            self.account_info_list.append(self._account_status_info)

        instrument_id = self.curr_md['pair']
        if instrument_id in self._pos_status_info_dic:
            pos_status_info_last = self._pos_status_info_dic[instrument_id]
            trade_date = pos_status_info_last.trade_date
            trade_time = pos_status_info_last.trade_time
            # 如果当前K线以及更新则不需再次更新。如果当前K线以及有交易产生，则 pos_info 将会在 _save_pos_status_info 函数中被更新，因此无需再次更新
            if trade_date == self.curr_md['ts_start'].date() and trade_time == self.curr_md['ts_start'].time():
                return
            # 说明上一根K线位置已经平仓，下一根K先位置将记录清除
            if pos_status_info_last.position == 0:
                del self._pos_status_info_dic[instrument_id]
            # 根据 md 数据更新 仓位信息
            # pos_status_info = pos_status_info_last.update_by_md(self.curr_md)
            pos_status_info = self._update_pos_status_info_by_md(pos_status_info_last)
            self._pos_status_info_dic[instrument_id] = pos_status_info

        # 统计账户信息，更新账户信息
        # account_status_info = self._account_status_info.update_by_pos_status_info(
        #     self._pos_status_info_dic, self.curr_md)
        account_status_info = self._update_by_pos_status_info()
        self._account_status_info = account_status_info
        self.account_info_list.append(self._account_status_info)

    def open_long(self, instrument_id, price, vol):
        self._save_order_info(instrument_id, price, vol, Direction.Long, Action.Open)

    def close_long(self, instrument_id, price, vol):
        self._save_order_info(instrument_id, price, vol, Direction.Long, Action.Close)

    def open_short(self, instrument_id, price, vol):
        self._save_order_info(instrument_id, price, vol, Direction.Short, Action.Open)

    def close_short(self, instrument_id, price, vol):
        self._save_order_info(instrument_id, price, vol, Direction.Short, Action.Close)

    def get_position(self, instrument_id, **kwargs) -> dict:
        if instrument_id in self._pos_status_info_dic:
            pos_status_info = self._pos_status_info_dic[instrument_id]
            position_date_pos_info_dic = {PositionDateType.History: pos_status_info}
        else:
            position_date_pos_info_dic = None
        return position_date_pos_info_dic

    @property
    def datetime_last_update_position(self) -> datetime:
        return datetime.now()

    @property
    def datetime_last_rtn_trade_dic(self) -> dict:
        raise NotImplementedError()

    @property
    def datetime_last_update_position_dic(self) -> dict:
        raise NotImplementedError()

    @property
    def datetime_last_send_order_dic(self) -> dict:
        raise NotImplementedError()

    def release(self):
        try:
            with with_db_session(engine_abat) as session:
                session.add_all(self.order_info_list)
                session.add_all(self.trade_info_list)
                session.add_all(self.pos_status_info_dic.values())
                session.add_all(self.account_info_list)
                session.commit()
        except:
            self.logger.exception("release exception")

    def get_order(self, instrument_id) -> OrderInfo:
        if instrument_id in self._order_info_dic:
            return self._order_info_dic[instrument_id]
        else:
            return None


@register_realtime_trader_agent
class RealTimeTraderAgent(TraderAgent):
    """
    供调用实时交易接口使用
    """

    def __init__(self, stg_run_id, run_mode_params: dict):
        super().__init__(stg_run_id, run_mode_params)
        self.trader_api = HBRestAPI()
        self.currency_balance_dic = {}
        self.currency_balance_last_get_datetime = None
        self.symbol_currency_dic = None
        self.symbol_precision_dic = None
        self._datetime_last_rtn_trade_dic = {}
        self._datetime_last_update_position_dic = {}

    def connect(self):
        with with_db_session(engine_md) as session:
            data = session.query(SymbolPair).all()
            self.symbol_currency_dic = {
                f'{sym.base_currency}{sym.quote_currency}': sym.base_currency
                for sym in data}
            self.symbol_precision_dic = {
                f'{sym.base_currency}{sym.quote_currency}': (int(sym.price_precision), int(sym.amount_precision))
                for sym in data}

    # @try_n_times(times=3, sleep_time=2, logger=logger)
    def open_long(self, symbol, price, vol):
        """买入多头"""
        price_precision, amount_precision = self.symbol_precision_dic[symbol]
        if isinstance(price, float):
            price = format(price, f'.{price_precision}f')
        if isinstance(vol, float):
            if vol < 10 ** -amount_precision:
                logger.warning('%s 订单量 %f 太小，忽略')
                return
            vol = format(floor(vol, amount_precision), f'.{amount_precision}f')
        self.trader_api.send_order(vol, symbol, OrderType.buy_limit.value, price)
        self._datetime_last_rtn_trade_dic[symbol] = datetime.now()

    def close_long(self, symbol, price, vol):
        """卖出多头"""
        price_precision, amount_precision = self.symbol_precision_dic[symbol]
        if isinstance(price, float):
            price = format(price, f'.{price_precision}f')
        if isinstance(vol, float):
            if vol < 10 ** -amount_precision:
                logger.warning('%s 订单量 %f 太小，忽略')
                return
            vol = format(floor(vol, amount_precision), f'.{amount_precision}f')
        self.trader_api.send_order(vol, symbol, OrderType.sell_limit.value, price)
        self._datetime_last_rtn_trade_dic[symbol] = datetime.now()

    def open_short(self, instrument_id, price, vol):
        # self.trader_api.open_short(instrument_id, price, vol)
        raise NotImplementedError()

    def close_short(self, instrument_id, price, vol):
        # self.trader_api.close_short(instrument_id, price, vol)
        raise NotImplementedError()

    def get_position(self, instrument_id, force_refresh=False) -> dict:
        """
        instrument_id（相当于 symbol )
        symbol ethusdt, btcusdt
        currency eth, btc
        :param instrument_id:
        :param force_refresh:
        :return:
        """
        symbol = instrument_id
        currency = self.get_currency(symbol)
        # currency = instrument_id
        # self.logger.debug('symbol:%s force_refresh=%s', symbol, force_refresh)
        position_date_inv_pos_dic = self.get_balance(currency=currency, force_refresh=force_refresh)
        return position_date_inv_pos_dic

    def get_currency(self, symbol):
        """
        根据 symbol 找到对应的 currency
        symbol: ethusdt, btcusdt
        currency: eth, btc
        :param symbol:
        :return:
        """
        return self.symbol_currency_dic[symbol]

    def get_balance(self, non_zero_only=False, trade_type_only=True, currency=None, force_refresh=False):
        """
        调用接口 查询 各个币种仓位
        :param non_zero_only: 只保留非零币种
        :param trade_type_only: 只保留 trade 类型币种，frozen 类型的不保存
        :param currency: 只返回制定币种 usdt eth 等
        :param force_refresh: 强制刷新，默认没30秒允许重新查询一次
        :return: {'usdt': {<PositionDateType.History: 2>: {'currency': 'usdt', 'type': 'trade', 'balance': 144.09238}}}
        """
        if force_refresh or self.currency_balance_last_get_datetime is None or \
                self.currency_balance_last_get_datetime < datetime.now() - timedelta(seconds=30):
            ret_data = self.trader_api.get_balance()
            acc_balance = ret_data['data']['list']
            acc_balance_new_dic = defaultdict(dict)
            for balance_dic in acc_balance:
                currency_curr = balance_dic['currency']
                self._datetime_last_update_position_dic[currency_curr] = datetime.now()

                if non_zero_only and balance_dic['balance'] == '0':
                    continue

                if trade_type_only and balance_dic['type'] != 'trade':
                    continue
                balance_val = float(balance_dic['balance'])
                balance_dic['balance'] = float(balance_dic['balance'])
                if PositionDateType.History in acc_balance_new_dic[currency_curr]:
                    balance_dic_old = acc_balance_new_dic[currency_curr][PositionDateType.History]
                    balance_dic_old['balance'] += balance_dic['balance']
                    # TODO: 日后可以考虑将 PositionDateType.History 替换为 type
                    acc_balance_new_dic[currency_curr][PositionDateType.History] = balance_dic
                else:
                    acc_balance_new_dic[currency_curr] = {PositionDateType.History: balance_dic}

            self.currency_balance_dic = acc_balance_new_dic
            self.currency_balance_last_get_datetime = datetime.now()

        if currency is not None:
            if currency in self.currency_balance_dic:
                ret_data = self.currency_balance_dic[currency]
                # for position_date_type, data in self.currency_balance_dic[currency].items():
                #     if data['currency'] == currency:
                #         ret_data = data
                #         break
            else:
                ret_data = None
        else:
            ret_data = self.currency_balance_dic
        return ret_data

    @property
    def datetime_last_update_position(self) -> datetime:
        return self.currency_balance_last_get_datetime

    @property
    def datetime_last_rtn_trade_dic(self) -> dict:
        return self._datetime_last_rtn_trade_dic

    @property
    def datetime_last_update_position_dic(self) -> dict:
        return self._datetime_last_update_position_dic

    @property
    def datetime_last_send_order_dic(self) -> dict:
        raise NotImplementedError()

    def get_order(self, instrument_id, states='submitted') -> list:
        """

        :param instrument_id:
        :param states:
        :return: 格式如下：
        [{'id': 603164274, 'symbol': 'ethusdt', 'account-id': 909325, 'amount': '4.134700000000000000',
'price': '983.150000000000000000', 'created-at': 1515166787246, 'type': 'buy-limit',
'field-amount': '4.134700000000000000', 'field-cash-amount': '4065.030305000000000000',
'field-fees': '0.008269400000000000', 'finished-at': 1515166795669, 'source': 'web',
'state': 'filled', 'canceled-at': 0},
 ... ]
        """
        symbol = instrument_id
        ret_data = self.trader_api.get_orders_info(symbol=symbol, states=states)
        return ret_data['data']

    def cancel_order(self, instrument_id):
        symbol = instrument_id
        order_list = self.get_order(symbol)
        order_id_list = [data['id'] for data in order_list]
        return self.trader_api.batchcancel_order(order_id_list)

    def release(self):
        pass


if __name__ == "__main__":
    import time

    # 测试交易 下单接口及撤单接口
    # symbol, vol, price = 'ocnusdt', 1, 0.00004611  # OCN/USDT
    symbol, vol, price = 'eosusdt', 1.0251, 4.1234  # OCN/USDT

    td = RealTimeTraderAgent(stg_run_id=1, run_mode_params={})
    td.open_long(symbol=symbol, price=price, vol=vol)
    order_dic_list = td.get_order(instrument_id=symbol)
    print('after open_long', order_dic_list)
    assert len(order_dic_list) == 1
    td.cancel_order(instrument_id=symbol)
    time.sleep(1)
    order_dic_list = td.get_order(instrument_id=symbol)
    print('after cancel', order_dic_list)
    assert len(order_dic_list) == 0
