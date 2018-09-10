#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/20 15:12
@File    : trade.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import logging
from abc import abstractmethod, ABC
from datetime import datetime
from abat.common import RunMode
from abat.backend.orm import OrderInfo

logger = logging.getLogger(__package__)


class TraderAgent(ABC):
    """
    交易代理（抽象类），回测交易代理，实盘交易代理的父类
    """

    def __init__(self, stg_run_id, run_mode_params: dict):
        """
        stg_run_id 作为每一次独立的执行策略过程的唯一标识
        :param stg_run_id:
        """
        self.stg_run_id = stg_run_id
        self.run_mode_params = run_mode_params
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def connect(self):
        raise NotImplementedError()

    @abstractmethod
    def open_long(self, instrument_id, price, vol):
        raise NotImplementedError()

    @abstractmethod
    def close_long(self, instrument_id, price, vol):
        raise NotImplementedError()

    @abstractmethod
    def open_short(self, instrument_id, price, vol):
        raise NotImplementedError()

    @abstractmethod
    def close_short(self, instrument_id, price, vol):
        raise NotImplementedError()

    @abstractmethod
    def get_position(self, instrument_id) -> dict:
        raise NotImplementedError()

    @abstractmethod
    def get_order(self, instrument_id) -> OrderInfo:
        raise NotImplementedError()

    @abstractmethod
    def release(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def datetime_last_update_position(self) -> datetime:
        raise NotImplementedError()

    @property
    @abstractmethod
    def datetime_last_rtn_trade_dic(self) -> dict:
        raise NotImplementedError()

    @property
    @abstractmethod
    def datetime_last_update_position_dic(self) -> dict:
        raise NotImplementedError()

    @property
    @abstractmethod
    def datetime_last_send_order_dic(self) -> dict:
        raise NotImplementedError()

    @property
    @abstractmethod
    def get_balance(self) -> dict:
        raise NotImplementedError()


trader_agent_class_dic = {RunMode.Backtest: TraderAgent, RunMode.Realtime: TraderAgent}


def register_realtime_trader_agent(agent: TraderAgent) -> TraderAgent:
    trader_agent_class_dic[RunMode.Realtime] = agent
    logger.info('设置 realtime trade agent:%s', agent.__class__.__name__)
    return agent


def register_backtest_trader_agent(agent: TraderAgent) -> TraderAgent:
    trader_agent_class_dic[RunMode.Backtest] = agent
    logger.info('设置 backtest trade agent:%s', agent.__class__.__name__)
    return agent
