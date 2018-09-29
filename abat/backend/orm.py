# -*- coding: utf-8 -*-
"""
Created on 2017/10/9
@author: MG
"""
from datetime import datetime, timedelta
from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, Float, Boolean, SmallInteger, Date, Time
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.orm import mapper
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from pandas import Timedelta
from config import Config
from abat.backend import engine_abat
from abat.utils.db_utils import with_db_session
from abat.common import Action, Direction
from abat.utils.fh_utils import str_2_date, pd_timedelta_2_timedelta
import logging

BaseModel = declarative_base()
# 每一次实务均产生数据库插入或更新动作（默认：否）
UPDATE_OR_INSERT_PER_ACTION = False


class StgRunInfo(BaseModel):
    """策略运行信息"""

    __tablename__ = 'stg_run_info'
    stg_run_id = Column(Integer, autoincrement=True, primary_key=True)
    stg_name = Column(String(200))
    dt_from = Column(DateTime())
    dt_to = Column(DateTime())
    stg_params = Column(String(2000))
    md_agent_params_list = Column(String(2000))
    run_mode = Column(SmallInteger)
    run_mode_params = Column(String(2000))


class OrderInfo(BaseModel):
    """订单信息"""

    __tablename__ = 'order_info'
    order_id = Column(Integer, primary_key=True, autoincrement=True)
    stg_run_id = Column(Integer)
    # order_dt = Column(DateTime, server_default=func.now())
    order_date = Column(Date)  # 对应行情数据中 ActionDate
    order_time = Column(Time)  # 对应行情数据中 ActionTime
    order_millisec = Column(Integer)  # 对应行情数据中 ActionMillisec
    direction = Column(Boolean)  # 0：空；1：多
    action = Column(Integer)  # 0：关：1：开
    instrument_id = Column(String(30))
    order_price = Column(DOUBLE)
    order_vol = Column(DOUBLE)  # 订单量
    traded_vol = Column(DOUBLE, server_default='0')  # 保证金 , comment="成交数量"
    margin = Column(DOUBLE, server_default='0')  # 保证金 , comment="占用保证金"

    # 每一次实务均产生数据库插入或更新动作（默认：否）

    def __repr__(self):
        return "<OrderInfo(id='{0.order_id}', direction='{0.direction}', action='{0.action}', instrument_id='{0.instrument_id}', order_price='{0.order_price}', order_vol='{0.order_vol}')>".format(
            self)

    @staticmethod
    def remove_order_info(stg_run_id: int):
        """
        仅作为调试工具使用，删除指定 stg_run_id 相关的 order_info
        :param stg_run_id: 
        :return: 
        """
        with with_db_session(engine_abat) as session:
            session.execute('DELETE FROM order_info WHERE stg_run_id=:stg_run_id',
                            {'stg_run_id': stg_run_id})
            session.commit()

    @staticmethod
    def create_by_dic(order_dic):
        order_info = OrderInfo()
        order_info.order_date = order_dic['TradingDay']
        order_info.order_time = pd_timedelta_2_timedelta(order_dic['InsertTime'])
        order_info.order_millisec = 0
        order_info.direction = Direction.create_by_direction(order_dic['Direction'])
        order_info.action = Action.create_by_offsetflag(order_dic['CombOffsetFlag'])
        order_info.instrument_id = order_dic['InstrumentID']
        order_info.order_price = order_dic['LimitPrice']
        order_info.order_vol = order_dic['VolumeTotalOriginal']
        order_info.traded_vol = order_dic['VolumeTraded']
        order_info.margin = 0
        return order_info


class TradeInfo(BaseModel):
    """记录成交信息"""
    __tablename__ = 'trade_info'
    trade_id = Column(Integer, primary_key=True, autoincrement=True)  # , comment="成交id"
    stg_run_id = Column(Integer)
    order_id = Column(Integer)  # , comment="对应订单id"
    # order_dt = Column(DateTime, server_default=func.now())
    order_price = Column(DOUBLE)  # , comment="原订单价格"
    order_vol = Column(DOUBLE)  # 订单量 , comment="原订单数量"
    trade_date = Column(Date)  # 对应行情数据中 ActionDate
    trade_time = Column(Time)  # 对应行情数据中 ActionTime
    trade_millisec = Column(Integer)  # 对应行情数据中 ActionMillisec
    direction = Column(Boolean)  # 0：空；1：多
    action = Column(Integer)  # 0：关：1：开
    instrument_id = Column(String(30))
    trade_price = Column(DOUBLE)  # , comment="成交价格"
    trade_vol = Column(DOUBLE)  # 订单量 , comment="成交数量"
    margin = Column(DOUBLE, server_default='0')  # 保证金 , comment="占用保证金"
    commission = Column(DOUBLE, server_default='0')  # 佣金、手续费 , comment="佣金、手续费"

    def set_trade_time(self, value):
        if isinstance(value, Timedelta):
            # print(value, 'parse to timedelta')
            self.trade_time = timedelta(seconds=value.seconds)
        else:
            self.trade_time = value

    @staticmethod
    def remove_trade_info(stg_run_id: int):
        """
        仅作为调试工具使用，删除指定 stg_run_id 相关的 trade_info
        :param stg_run_id: 
        :return: 
        """
        with with_db_session(engine_abat) as session:
            session.execute('DELETE FROM trade_info WHERE stg_run_id=:stg_run_id',
                            {'stg_run_id': stg_run_id})
            session.commit()

    @staticmethod
    def create_by_order_info(order_info: OrderInfo):
        direction, action, instrument_id = order_info.direction, order_info.action, order_info.instrument_id
        order_price, order_vol, order_id = order_info.order_price, order_info.order_vol, order_info.order_id
        order_date, order_time, order_millisec = order_info.order_date, order_info.order_time, order_info.order_millisec
        stg_run_id = order_info.stg_run_id

        # TODO: 以后还可以增加滑点，成交比例等
        # instrument_info = Config.instrument_info_dic[instrument_id]
        # multiple = instrument_info['VolumeMultiple']
        # margin_ratio = instrument_info['LongMarginRatio']
        multiple, margin_ratio = 1, 1
        margin = order_vol * order_price * multiple * margin_ratio
        commission = 0
        trade_info = TradeInfo(stg_run_id=stg_run_id,
                               order_id=order_id,
                               trade_date=order_date,
                               trade_time=order_time,
                               trade_millisec=order_millisec,
                               direction=direction,
                               action=action,
                               instrument_id=instrument_id,
                               order_price=order_price,
                               order_vol=order_vol,
                               trade_price=order_price,
                               trade_vol=order_vol,
                               margin=margin,
                               commission=commission
                               )
        if UPDATE_OR_INSERT_PER_ACTION:
            with with_db_session(engine_abat, expire_on_commit=False) as session:
                session.add(trade_info)
                session.commit()
        return trade_info


class PosStatusInfo(BaseModel):
    """
    持仓状态数据
    当持仓状态从有仓位到清仓时（position>0 --> position==0），计算清仓前的浮动收益，并设置到 floating_pl 字段最为当前状态的浮动收益
    在调用 create_by_self 时，则需要处理一下，当 position==0 时，floating_pl 直接设置为 0，避免引起后续计算上的混淆
    """
    __tablename__ = 'pos_status_info'
    pos_status_info_id = Column(Integer, primary_key=True, autoincrement=True)
    stg_run_id = Column(Integer)  # 对应回测了策略 StgRunID 此数据与 AccSumID 对应数据相同
    trade_id = Column(Integer)  # , comment="最新的成交id"
    # update_dt = Column(DateTime)  # 每个订单变化生成一条记录 此数据与 AccSumID 对应数据相同
    trade_date = Column(Date)  # 对应行情数据中 ActionDate
    trade_time = Column(Time)  # 对应行情数据中 ActionTime
    trade_millisec = Column(Integer)  # 对应行情数据中 ActionMillisec
    direction = Column(Integer)
    instrument_id = Column(String(30))
    position = Column(DOUBLE, default=0.0)
    avg_price = Column(DOUBLE, default=0.0)  # 所持投资品种上一交易日所有交易的加权平均价
    cur_price = Column(DOUBLE, default=0.0)
    floating_pl = Column(DOUBLE, default=0.0)
    floating_pl_chg = Column(DOUBLE, default=0.0)
    floating_pl_cum = Column(DOUBLE, default=0.0)
    margin = Column(DOUBLE, default=0.0)
    margin_chg = Column(DOUBLE, default=0.0)
    position_date = Column(Integer, default=0)
    logger = logging.getLogger(__tablename__)

    def __repr__(self):
        return "<PosStatusInfo(id='{0.pos_status_info_id}', update_dt='{0.update_dt}', instrument_id='{0.instrument_id}', instrument_id='{0.instrument_id}', direction='{0.direction}', position='{0.position}')>".format(
            self)

    @staticmethod
    def create_by_trade_info(trade_info: TradeInfo):
        direction, action, instrument_id = trade_info.direction, trade_info.action, trade_info.instrument_id
        trade_price, trade_vol, trade_id = trade_info.trade_price, trade_info.trade_vol, trade_info.trade_id
        trade_date, trade_time, trade_millisec = trade_info.trade_date, trade_info.trade_time, trade_info.trade_millisec
        stg_run_id = trade_info.stg_run_id
        if action == int(Action.Close):
            raise ValueError('trade_info.action 不能为 close')
        pos_status_info = PosStatusInfo(stg_run_id=stg_run_id,
                                        trade_id=trade_id,
                                        trade_date=trade_date,
                                        trade_time=trade_time,
                                        trade_millisec=trade_millisec,
                                        direction=direction,
                                        instrument_id=instrument_id,
                                        position=trade_vol,
                                        avg_price=trade_price,
                                        cur_price=trade_price,
                                        margin=0,
                                        margin_chg=0,
                                        floating_pl=0,
                                        floating_pl_chg=0,
                                        floating_pl_cum=0,
                                        )
        if UPDATE_OR_INSERT_PER_ACTION:
            # 更新最新持仓纪录
            with with_db_session(engine_abat, expire_on_commit=False) as session:
                session.add(pos_status_info)
                session.commit()
        return pos_status_info

    def update_by_trade_info(self, trade_info: TradeInfo):
        """
        创建新的对象，根据 trade_info 更新相关信息
        :param trade_info: 
        :return: 
        """
        # 复制前一个持仓状态
        pos_status_info = self.create_by_self()
        direction, action, instrument_id = trade_info.direction, trade_info.action, trade_info.instrument_id
        trade_price, trade_vol, trade_id = trade_info.trade_price, trade_info.trade_vol, trade_info.trade_id
        trade_date, trade_time, trade_millisec = trade_info.trade_date, trade_info.trade_time, trade_info.trade_millisec

        # 获取合约信息
        # instrument_info = Config.instrument_info_dic[instrument_id]
        # multiple = instrument_info['VolumeMultiple']
        # margin_ratio = instrument_info['LongMarginRatio']
        multiple, margin_ratio = 1, 1

        # 计算仓位、方向、平均价格
        pos_direction, position, avg_price = pos_status_info.direction, pos_status_info.position, pos_status_info.avg_price
        if pos_direction == direction:
            if action == Action.Open:
                # 方向相同：开仓：加仓；
                pos_status_info.avg_price = (position * avg_price + trade_price * trade_vol) / (position + trade_vol)
                pos_status_info.position = position + trade_vol
            else:
                # 方向相同：关仓：减仓；
                if trade_vol > position:
                    raise ValueError("当前持仓%d，平仓%d，错误" % (position, trade_vol))
                elif trade_vol == position:
                    # 清仓前计算浮动收益
                    # 未清仓的情况将在下面的代码中统一计算浮动收益
                    if pos_status_info.direction == Direction.Long:
                        pos_status_info.floating_pl = (trade_price - avg_price) * position * multiple
                    else:
                        pos_status_info.floating_pl = (avg_price - trade_price) * position * multiple

                    pos_status_info.avg_price = 0
                    pos_status_info.position = 0

                else:
                    pos_status_info.avg_price = (position * avg_price - trade_price * trade_vol) / (
                                position - trade_vol)
                    pos_status_info.position = position - trade_vol
        elif position == 0:
            pos_status_info.avg_price = trade_price
            pos_status_info.position = trade_vol
            pos_status_info.direction = direction
        else:
            # 方向相反
            raise ValueError("当前仓位：%s %d手，目标操作：%s %d手，请先平仓在开仓" % (
                "多头" if pos_direction == Direction.Long else "空头", position,
                "多头" if direction == Direction.Long else "空头", trade_vol,
            ))
            # if position == trade_vol:
            #     # 方向相反，量相同：清仓
            #     pos_status_info.avg_price = 0
            #     pos_status_info.position = 0
            # else:
            #     holding_amount = position * avg_price
            #     trade_amount = trade_price * trade_vol
            #     position_rest = position - trade_vol
            #     avg_price = (holding_amount - trade_amount) / position_rest
            #     if position > trade_vol:
            #         # 减仓
            #         pos_status_info.avg_price = avg_price
            #         pos_status_info.position = position_rest
            #     else:
            #         # 多空反手
            #         self.logger.warning("%s 持%s：%d -> %d 多空反手", self.instrument_id,
            #                             '多' if direction == int(Direction.Long) else '空', position, position_rest)
            #         pos_status_info.avg_price = avg_price
            #         pos_status_info.position = position_rest
            #         pos_status_info.direction = Direction.Short if direction == int(Direction.Short) else Direction.Long

        # 设置其他属性
        pos_status_info.cur_price = trade_price
        pos_status_info.trade_date = trade_date
        pos_status_info.trade_time = trade_time
        pos_status_info.trade_millisec = trade_millisec

        # 计算 floating_pl margin
        position = pos_status_info.position
        # cur_price = pos_status_info.cur_price
        avg_price = pos_status_info.avg_price
        pos_status_info.margin = position * trade_price * multiple * margin_ratio
        # 如果当前仓位不为 0 则计算浮动收益
        if position > 0:
            if pos_status_info.direction == Direction.Long:
                pos_status_info.floating_pl = (trade_price - avg_price) * position * multiple
            else:
                pos_status_info.floating_pl = (avg_price - trade_price) * position * multiple
        # 如果前一状态仓位为 0 则不进行差值计算
        if self.position == 0:
            pos_status_info.margin_chg = pos_status_info.margin
            pos_status_info.floating_pl_chg = pos_status_info.floating_pl
        else:
            pos_status_info.margin_chg = pos_status_info.margin - self.margin
            pos_status_info.floating_pl_chg = pos_status_info.floating_pl - self.floating_pl

        pos_status_info.floating_pl_cum += pos_status_info.floating_pl_chg

        if UPDATE_OR_INSERT_PER_ACTION:
            # 更新最新持仓纪录
            with with_db_session(engine_abat, expire_on_commit=False) as session:
                session.add(pos_status_info)
                session.commit()
        return pos_status_info

    def update_by_md(self, md: dict):
        """
        创建新的对象，根据 trade_info 更新相关信息
        :param md: 
        :return: 
        """
        trade_date = md['ActionDay']
        trade_time = pd_timedelta_2_timedelta(md['ActionTime'])
        trade_millisec = int(md.setdefault('ActionMillisec', 0))
        trade_price = float(md['close'])
        instrument_id = md['InstrumentID']
        pos_status_info = self.create_by_self()
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
        pos_status_info.margin_chg = pos_status_info.margin - self.margin
        if pos_status_info.direction == Direction.Long:
            pos_status_info.floating_pl = (cur_price - avg_price) * position * multiple
        else:
            pos_status_info.floating_pl = (avg_price - cur_price) * position * multiple
        pos_status_info.floating_pl_chg = pos_status_info.floating_pl - self.floating_pl
        pos_status_info.floating_pl_cum += pos_status_info.floating_pl_chg

        if UPDATE_OR_INSERT_PER_ACTION:
            # 更新最新持仓纪录
            with with_db_session(engine_abat, expire_on_commit=False) as session:
                session.add(pos_status_info)
                session.commit()
        return pos_status_info

    def create_by_self(self):
        """
        创建新的对象
        若当前对象持仓为0（position==0），则 浮动收益部分设置为0
        :return: 
        """
        position = self.position
        pos_status_info = PosStatusInfo(stg_run_id=self.stg_run_id,
                                        trade_id=self.trade_id,
                                        trade_date=self.trade_date,
                                        trade_time=self.trade_time,
                                        trade_millisec=self.trade_millisec,
                                        direction=self.direction,
                                        instrument_id=self.instrument_id,
                                        position=position,
                                        avg_price=self.avg_price,
                                        cur_price=self.cur_price,
                                        floating_pl=self.floating_pl if position > 0 else 0,
                                        floating_pl_cum=self.floating_pl_cum,
                                        margin=self.margin)
        return pos_status_info

    @staticmethod
    def create_by_dic(position_date_inv_pos_dic: dict) -> dict:
        if position_date_inv_pos_dic is None:
            return None
        position_date_pos_info_dic = {}
        for position_date, pos_dic in position_date_inv_pos_dic.items():
            pos_info = PosStatusInfo()
            pos_info.trade_date = pos_dic['TradingDay']
            pos_info.trade_time = None
            pos_info.direction = Direction.create_by_posi_direction(pos_dic['PosiDirection'])
            pos_info.instrument_id = pos_dic['InstrumentID']
            pos_info.position = pos_dic['Position']
            pos_info.avg_price = pos_dic['PositionCost']
            pos_info.cur_price = 0
            pos_info.floating_pl = pos_dic['PositionProfit']
            pos_info.floating_pl_chg = 0
            pos_info.margin = pos_dic['UseMargin']
            pos_info.margin_chg = 0
            pos_info.position_date = int(position_date)
            position_date_pos_info_dic[position_date] = pos_info
        return position_date_pos_info_dic

    @staticmethod
    def remove_pos_status_info(stg_run_id: int):
        """
        仅作为调试工具使用，删除指定 stg_run_id 相关的 pos_status_info
        :param stg_run_id: 
        :return: 
        """
        with with_db_session(engine_abat) as session:
            session.execute('DELETE FROM pos_status_info WHERE stg_run_id=:stg_run_id',
                            {'stg_run_id': stg_run_id})
            session.commit()


class AccountStatusInfo(BaseModel):
    """持仓状态数据"""
    __tablename__ = 'account_status_info'
    account_status_info_id = Column(Integer, primary_key=True, autoincrement=True)
    stg_run_id = Column(Integer)  # 对应回测了策略 StgRunID 此数据与 AccSumID 对应数据相同
    trade_date = Column(Date)  # 对应行情数据中 ActionDate
    trade_time = Column(Time)  # 对应行情数据中 ActionTime
    trade_millisec = Column(Integer)  # 对应行情数据中 ActionMillisec
    available_cash = Column(DOUBLE, default=0.0)  # 可用资金, double
    curr_margin = Column(DOUBLE, default=0.0)  # 当前保证金总额, double
    close_profit = Column(DOUBLE, default=0.0)
    position_profit = Column(DOUBLE, default=0.0)
    floating_pl_cum = Column(DOUBLE, default=0.0)
    fee_tot = Column(DOUBLE, default=0.0)
    balance_tot = Column(DOUBLE, default=0.0)

    @staticmethod
    def create(stg_run_id, init_cash: int, md: dict):
        """
        根据 md 及 初始化资金 创建对象，默认日期为当前md数据-1天
        :param stg_run_id: 
        :param init_cash: 
        :param md: 
        :return: 
        """
        trade_date = str_2_date(md['ActionDay']) - timedelta(days=1)
        trade_time = pd_timedelta_2_timedelta(md['ActionTime'])
        trade_millisec = int(md.setdefault('ActionMillisec', 0))
        trade_price = float(md['close'])
        acc_status_info = AccountStatusInfo(stg_run_id=stg_run_id,
                                            trade_date=trade_date,
                                            trade_time=trade_time,
                                            trade_millisec=trade_millisec,
                                            available_cash=init_cash,
                                            balance_tot=init_cash,
                                            )
        if UPDATE_OR_INSERT_PER_ACTION:
            # 更新最新持仓纪录
            with with_db_session(engine_abat, expire_on_commit=False) as session:
                session.add(acc_status_info)
                session.commit()
        return acc_status_info

    def create_by_self(self):
        """
        创建新的对象，默认前一日持仓信息的最新价，等于下一交易日的结算价（即 AvePrice）
        :return: 
        """
        account_status_info = AccountStatusInfo(stg_run_id=self.stg_run_id,
                                                trade_date=self.trade_date,
                                                trade_time=self.trade_time,
                                                trade_millisec=self.trade_millisec,
                                                available_cash=self.available_cash,
                                                curr_margin=self.curr_margin,
                                                close_profit=self.close_profit,
                                                position_profit=self.position_profit,
                                                floating_pl_cum=self.floating_pl_cum,
                                                fee_tot=self.fee_tot,
                                                balance_tot=self.balance_tot
                                                )
        return account_status_info

    def update_by_pos_status_info(self, pos_status_info_dic, md: dict):
        """
        根据 持仓列表更新账户信息
        :param pos_status_info_dic: 
        :return: 
        """
        account_status_info = self.create_by_self()
        # 上一次更新日期、时间
        # trade_date_last, trade_time_last, trade_millisec_last = \
        #     account_status_info.trade_date, account_status_info.trade_time, account_status_info.trade_millisec
        # 更新日期、时间
        trade_date = md['ActionDay']
        trade_time = pd_timedelta_2_timedelta(md['ActionTime'])
        trade_millisec = int(md.setdefault('ActionMillisec', 0))

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
        if UPDATE_OR_INSERT_PER_ACTION:
            # 更新最新持仓纪录
            with with_db_session(engine_abat, expire_on_commit=False) as session:
                session.add(account_status_info)
                session.commit()
        return account_status_info


def init():
    from abat.backend import engine_abat

    BaseModel.metadata.create_all(engine_abat)
    with with_db_session(engine_abat) as session:
        for table_name, _ in BaseModel.metadata.tables.items():
            sql_str = "ALTER TABLE %s ENGINE = MyISAM" % table_name
            session.execute(sql_str)
    print("所有表结构建立完成")


if __name__ == "__main__":
    init()
    # 创建user表，继承metadata类
    # Engine使用Schama Type创建一个特定的结构对象
    # stg_info_table = Table("stg_info", metadata, autoload=True)
