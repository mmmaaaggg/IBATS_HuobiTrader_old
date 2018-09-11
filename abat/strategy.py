# -*- coding: utf-8 -*-
"""
Created on 2017/9/2
@author: MG
"""
from threading import Thread
import warnings
import json
import numpy as np
import pandas as pd
from config import Config
import logging
from queue import Empty
import time
from datetime import date, datetime, timedelta
from abc import ABC
from abat.backend import engine_abat
from abat.utils.db_utils import with_db_session
from abat.md import MdAgentBase
from abat.common import PeriodType, RunMode, ContextKey, Direction, BacktestTradeMode
from abat.utils.fh_utils import try_2_date
from abat.backend.orm import StgRunInfo
from abat.trade import trader_agent_class_dic


class StgBase:

    def __init__(self):
        # 记录各个周期 md 数据
        self._md_period_df_dic = {}
        # 记录在行情推送过程中最新的一笔md数据
        # self._period_curr_md_dic = {}
        # 记录各个周期 md 列信息
        self._md_period_df_col_name_list_dic = {}
        self.trade_agent = None
        self.logger = logging.getLogger(self.__class__.__name__)
        self._on_period_event_dic = {
            PeriodType.Tick: EventHandlersRelation(PeriodType.Tick,
                                                   self.on_prepare_tick, self.on_tick, pd.DataFrame),
            PeriodType.Min1: EventHandlersRelation(PeriodType.Min1,
                                                   self.on_prepare_min1, self.on_min1, pd.DataFrame),
            PeriodType.Hour1: EventHandlersRelation(PeriodType.Hour1,
                                                    self.on_prepare_hour1, self.on_hour1, pd.DataFrame),
            PeriodType.Day1: EventHandlersRelation(PeriodType.Day1,
                                                   self.on_prepare_day1, self.on_day1, pd.DataFrame),
            PeriodType.Week1: EventHandlersRelation(PeriodType.Week1,
                                                    self.on_prepare_week1, self.on_week1, pd.DataFrame),
            PeriodType.Mon1: EventHandlersRelation(PeriodType.Mon1,
                                                   self.on_prepare_month1, self.on_month1, pd.DataFrame),
        }
        self._period_context_dic = {}

    def on_timer(self):
        pass

    def load_md_period_df(self, period, md_df, context):
        """初始化加载 md 数据"""
        self._md_period_df_dic[period] = md_df
        self._md_period_df_col_name_list_dic[period] = list(md_df.columns) if isinstance(md_df, pd.DataFrame) else None
        self._period_context_dic[period] = context
        # prepare_event_handler = self._on_period_prepare_event_dic[period]
        prepare_event_handler = self._on_period_event_dic[period].prepare_event
        prepare_event_handler(md_df, context)

    def init(self):
        """
        加载历史数据后，启动周期策略执行函数之前
        执行初始化动作，连接 trade_agent
        以后还可以放出其他初始化动作
        :return:
        """
        self.trade_agent.connect()

    def release(self):
        """

        :return:
        """
        self.trade_agent.release()

    def _on_period_md_append(self, period, md):
        """
        （仅供 on_period_md_handler 调用使用）
        用于整理接收到的各个周期行情数据
        :param period:
        :param md:
        :return:
        """
        # self._period_curr_md_dic[period] = md
        md_df = pd.DataFrame([md])
        if period in self._md_period_df_dic:
            col_name_list = self._md_period_df_col_name_list_dic[period]
            md_df_his = self._md_period_df_dic[period].append(md_df[col_name_list])
            self._md_period_df_dic[period] = md_df_his
        else:
            md_df_his = md_df
            self._md_period_df_dic[period] = md_df_his
            self._md_period_df_col_name_list_dic[period] = \
                list(md_df.columns) if isinstance(md_df, pd.DataFrame) else None
            # self.logger.debug('%s -> %s', period, md)
        return md_df_his

    def _on_period_md_event(self, period, md_df_his):
        """
        （仅供 on_period_md_handler 调用使用）
        用于将各个周期数据传入对应周期事件处理函数
        :param period:
        :param md_df_his:
        :return:
        """
        # event_handler = self._on_period_md_event_dic[period]
        event_handler = self._on_period_event_dic[period].md_event
        context = self._period_context_dic[period]
        # self._trade_agent.curr_md = md
        event_handler(md_df_his, context)

    def on_period_md_handler(self, period, md):
        """响应 period 数据"""
        # 本机测试，延时0.155秒，从分钟K线合成到交易策略端收到数据
        self.logger.debug("%s -> %s", PeriodType(period), md)
        # self._on_period_md_event(period, md_df_his)
        period_event_relation = self._on_period_event_dic[period]
        event_handler = period_event_relation.md_event
        param_type = period_event_relation.param_type
        context = self._period_context_dic[period]
        # TODO 由于每一次进入都需要进行判断，增加不必要的计算，考虑通过优化提高运行效率
        if param_type is dict:
            param = md
        elif param_type is pd.DataFrame:
            param = self._on_period_md_append(period, md)
        else:
            raise ValueError("不支持 %s 类型作为 %s 的事件参数" % (param_type, period))
        event_handler(param, context)

    def on_prepare_tick(self, md_df, context):
        """Tick 历史数据加载执行语句"""
        pass

    def on_prepare_min1(self, md_df, context):
        """1分钟线 历史数据加载执行语句"""
        pass

    def on_prepare_hour1(self, md_df, context):
        """1小时线 历史数据加载执行语句"""
        pass

    def on_prepare_day1(self, md_df, context):
        """1日线 历史数据加载执行语句"""
        pass

    def on_prepare_week1(self, md_df, context):
        """1周线 历史数据加载执行语句"""
        pass

    def on_prepare_month1(self, md_df, context):
        """1月线 历史数据加载执行语句"""
        pass

    def on_tick(self, md_df, context):
        """Tick策略执行语句，需要相应策略实现具体的策略算法"""
        pass

    def on_min1(self, md_df, context):
        """1分钟线策略执行语句，需要相应策略实现具体的策略算法"""
        pass

    def on_hour1(self, md_df, context):
        """1小时线策略执行语句，需要相应策略实现具体的策略算法"""
        pass

    def on_day1(self, md_df, context):
        """1日线策略执行语句，需要相应策略实现具体的策略算法"""
        pass

    def on_week1(self, md_df, context):
        """1周线策略执行语句，需要相应策略实现具体的策略算法"""
        pass

    def on_month1(self, md_df, context):
        """1月线策略执行语句，需要相应策略实现具体的策略算法"""
        pass

    def open_long(self, instrument_id, price, vol):
        self.trade_agent.open_long(instrument_id, price, vol)

    def close_long(self, instrument_id, price, vol):
        self.trade_agent.close_long(instrument_id, price, vol)

    def open_short(self, instrument_id, price, vol):
        self.trade_agent.open_short(instrument_id, price, vol)

    def close_short(self, instrument_id, price, vol):
        self.trade_agent.close_short(instrument_id, price, vol)

    def get_position(self, instrument_id, **kwargs) -> dict:
        """
        position_date 作为key， PosStatusInfo 为 val
        返回 position_date_pos_info_dic
        :param instrument_id:
        :return:
        """
        return self.trade_agent.get_position(instrument_id, **kwargs)

    def get_order(self, instrument_id) -> list:
        return self.trade_agent.get_order(instrument_id)

    def cancel_order(self, instrument_id):
        return self.trade_agent.cancel_order(instrument_id)

    @property
    def datetime_last_update_position(self):
        return self.trade_agent.datetime_last_update_position

    @property
    def datetime_last_rtn_trade_dic(self):
        return self.trade_agent.datetime_last_rtn_trade_dic

    @property
    def datetime_last_update_position_dic(self):
        return self.trade_agent.datetime_last_update_position_dic

    @property
    def datetime_last_send_order_dic(self):
        return self.trade_agent.datetime_last_send_order_dic

    def get_balance(self, non_zero_only=True, trade_type_only=True, currency=None, force_refresh=False) -> dict:
        """
        调用接口 查询 各个币种仓位
        :param non_zero_only: 只保留非零币种
        :param trade_type_only: 只保留 trade 类型币种，frozen 类型的不保存
        :param currency: 只返回制定币种 usdt eth 等
        :param force_refresh: 强制刷新，默认没30秒允许重新查询一次
        :return: {'usdt': {<PositionDateType.History: 2>: {'currency': 'usdt', 'type': 'trade', 'balance': 144.09238}}}
        """
        return self.trade_agent.get_balance(non_zero_only, trade_type_only, currency, force_refresh)

    def get_holding_currency(self, force_refresh=False, exclude_usdt=True) -> dict:
        """
        持仓情况dict（非usdt）,仅包含交易状态 type = 'trade' 的记录
        :param force_refresh:
        :param exclude_usdt: 默认为True，剔除 usdt
        :return:
         {'eos': {<PositionDateType.History: 2>: {'currency': 'eos', 'type': 'trade', 'balance': 144.09238}}}
        """
        cur_balance_dic = self.get_balance(non_zero_only=True, force_refresh=force_refresh)
        balance_dic = {}
        for currency, dic in cur_balance_dic.items():
            if exclude_usdt and currency == 'usdt':
                continue
            for pos_date_type, dic_sub in dic.items():
                if dic_sub['type'] != 'trade':
                    continue
                balance_dic.setdefault(currency, {})[pos_date_type] = dic_sub
        return balance_dic


class StgHandlerBase(Thread, ABC):
    logger = logging.getLogger("StgHandlerBase")

    @staticmethod
    def factory(stg_class_obj: StgBase.__class__, strategy_params, md_agent_params_list, run_mode: RunMode, **run_mode_params):
        """
        建立策略对象
        建立数据库相应记录信息
        根据运行模式（实时、回测）：选择相应的md_agent以及trade_agent
        :param stg_class_obj: 策略类型 StgBase 的子类
        :param strategy_params: 策略参数
        :param md_agent_params_list: 行情代理（md_agent）参数，支持同时订阅多周期、多品种，
        例如：同时订阅 [ethusdt, eosusdt] 1min 行情、[btcusdt, ethbtc] tick 行情
        :param run_mode: 运行模式 RunMode.Realtime  或 RunMode.Backtest
        :param run_mode_params: 运行参数，回测模式下：运行起止时间，实时行情下：加载定时器等设置
        :return: 策略执行对象实力
        """
        stg_run_info = StgRunInfo(stg_name=stg_class_obj.__name__,  # '{.__name__}'.format(stg_class_obj)
                                  dt_from=datetime.now(),
                                  dt_to=None,
                                  stg_params=json.dumps(strategy_params),
                                  md_agent_params_list=json.dumps(md_agent_params_list),
                                  run_mode=int(run_mode),
                                  run_mode_params=json.dumps(run_mode_params))
        with with_db_session(engine_abat) as session:
            session.add(stg_run_info)
            session.commit()
            stg_run_id = stg_run_info.stg_run_id
        # 设置运行模式：回测模式，实时模式。初始化交易接口
        # if run_mode == RunMode.Backtest:
        #     trade_agent = BacktestTraderAgent(stg_run_id, run_mode_params)
        # elif run_mode == RunMode.Realtime:
        #     trade_agent = RealTimeTraderAgent(stg_run_id, run_mode_params)
        # else:
        #     raise ValueError('run_mode %d error' % run_mode)
        trade_agent_class = trader_agent_class_dic[run_mode]
        # 初始化策略实体，传入参数
        stg_base = stg_class_obj(**strategy_params)
        # 设置策略交易接口 trade_agent，这里不适用参数传递的方式而使用属性赋值，
        # 因为stg子类被继承后，参数主要用于设置策略所需各种参数使用
        stg_base.trade_agent = trade_agent_class(stg_run_id, run_mode_params)
        # 对不同周期设置相应的md_agent
        # 初始化各个周期的 md_agent
        md_period_agent_dic = {}
        for md_agent_param in md_agent_params_list:
            period = md_agent_param['md_period']
            md_agent = MdAgentBase.factory(run_mode, **md_agent_param)
            md_period_agent_dic[period] = md_agent
            # 对各个周期分别加载历史数据，设置对应 handler
            # 通过 md_agent 加载各个周期的历史数据
            md_df = md_agent.load_history()
            if md_df is None:
                StgHandlerBase.logger.warning('加载 %s 历史数据为 None', period)
                continue
            if isinstance(md_df, dict):
                his_df_dic = md_df
                md_df = his_df_dic['md_df']
            else:
                warnings.warn('load_history 返回 df 数据格式即将废弃，请更新成 dict', DeprecationWarning)

            context = {ContextKey.instrument_id_list: list(md_agent.instrument_id_set)}
            stg_base.load_md_period_df(period, md_df, context)
            StgHandlerBase.logger.debug('加载 %s 历史数据 %s 条', period, 'None' if md_df is None else str(md_df.shape[0]))
        # 初始化 StgHandlerBase 实例
        if run_mode == RunMode.Realtime:
            stg_handler = StgHandlerRealtime(stg_run_id=stg_run_id, stg_base=stg_base,
                                             md_period_agent_dic=md_period_agent_dic, **run_mode_params)
        elif run_mode == RunMode.Backtest:
            stg_handler = StgHandlerBacktest(stg_run_id=stg_run_id, stg_base=stg_base,
                                             md_period_agent_dic=md_period_agent_dic, **run_mode_params)
        else:
            raise ValueError('run_mode %d error' % run_mode)
        StgHandlerBase.logger.debug('初始化 %r 完成', stg_handler)
        return stg_handler

    def __init__(self, stg_run_id, stg_base: StgBase, run_mode, md_period_agent_dic):
        super().__init__(daemon=True)
        self.stg_run_id = stg_run_id
        self.run_mode = run_mode
        # 初始化策略实体，传入参数
        self.stg_base = stg_base
        # 设置工作状态
        self.keep_running = None
        # 日志
        self.logger = logging.getLogger()
        # 对不同周期设置相应的md_agent
        self.md_period_agent_dic = md_period_agent_dic

    def stg_run_ending(self):
        """
        处理策略结束相关事项
        释放策略资源
        更新策略执行信息
        :return: 
        """
        self.stg_base.release()
        # 更新数据库 td_to 字段
        with with_db_session(engine_abat) as session:
            session.query(StgRunInfo).filter(StgRunInfo.stg_run_id == self.stg_run_id).update(
                {StgRunInfo.dt_to: datetime.now()})
            # sql_str = StgRunInfo.update().where(
            # StgRunInfo.c.stg_run_id == self.stg_run_id).values(dt_to=datetime.now())
            # session.execute(sql_str)
            session.commit()

    def __repr__(self):
        return '<{0.__class__.__name__}:{0.stg_run_id} {0.run_mode}>'.format(self)


class StgHandlerRealtime(StgHandlerBase):

    def __init__(self, stg_run_id, stg_base: StgBase, md_period_agent_dic, **kwargs):
        super().__init__(stg_run_id=stg_run_id, stg_base=stg_base, run_mode=RunMode.Realtime,
                         md_period_agent_dic=md_period_agent_dic)
        # 对不同周期设置相应的md_agent
        self.md_period_agent_dic = md_period_agent_dic
        # 设置线程池
        self.running_thread = {}
        # 日志
        self.logger = logging.getLogger()
        # 设置推送超时时间
        self.timeout_pull = 60
        # 设置独立的时间线程
        self.enable_timer_thread = kwargs.setdefault('enable_timer_thread', False)
        self.seconds_of_timer_interval = kwargs.setdefault('seconds_of_timer_interval', 9999)

    def run(self):

        # TODO: 以后再加锁，防止多线程，目前只是为了防止误操作导致的重复执行
        if self.keep_running:
            return
        else:
            self.keep_running = True

        try:
            # 策略初始化
            self.stg_base.init()
            # 对各个周期分别设置对应 handler
            for period, md_agent in self.md_period_agent_dic.items():
                # 获取对应事件响应函数
                on_period_md_handler = self.stg_base.on_period_md_handler
                # 异步运行：每一个周期及其对应的 handler 作为一个线程独立运行
                thread_name = 'run_md_agent %s' % md_agent.name
                run_md_agent_thread = Thread(target=self.run_md_agent, name=thread_name,
                                             args=(md_agent, on_period_md_handler), daemon=True)
                self.running_thread[period] = run_md_agent_thread
                self.logger.info("加载 %s 线程", thread_name)
                run_md_agent_thread.start()

            if self.enable_timer_thread:
                thread_name = 'run_timer'
                timer_thread = Thread(target=self.run_timer, name=thread_name, daemon=True)
                self.logger.info("加载 %s 线程", thread_name)
                timer_thread.start()

            # 各个线程分别join等待结束信号
            for period, run_md_agent_thread in self.running_thread.items():
                run_md_agent_thread.join()
                self.logger.info('%s period %s finished', run_md_agent_thread.name, period)
        finally:
            self.keep_running = False
            self.stg_run_ending()

    def run_timer(self):
        """
        负责定时运行策略对象的 on_timer 方法
        :return: 
        """
        while self.keep_running:
            try:
                self.stg_base.on_timer()
            except:
                self.logger.exception('on_timer 函数运行异常')
            finally:
                time.sleep(self.seconds_of_timer_interval)

    def run_md_agent(self, md_agent, handler):
        """
        md_agent pull 方法的事件驱动处理函数
        :param md_agent: 
        :param handler:  self.stgbase对象的响应 md_agent 的梳理函数：根据不同的 md_period 可能是 on_tick、 on_min、 on_day、 on_week、 on_month 等其中一个
        :return: 
        """
        period = md_agent.md_period
        self.logger.info('启动 %s 行情监听线程', period)
        md_agent.connect()
        md_agent.subscribe()  # 参数为空相当于 md_agent.subscribe(md_agent.instrument_id_list)
        md_agent.start()
        while self.keep_running:
            try:
                if not self.keep_running:
                    break
                # 加载数据，是设置超时时间，防止长时间阻塞
                md_dic = md_agent.pull(self.timeout_pull)
                handler(period, md_dic)
            except Empty:
                # 工作状态检查
                pass
            except Exception:
                self.logger.exception('%s 事件处理句柄执行异常，对应行情数据md_dic:\n%s',
                                      period, md_dic)
                # time.sleep(1)
        md_agent.release()
        self.logger.info('period:%s finished', period)


class StgHandlerBacktest(StgHandlerBase):

    def __init__(self, stg_run_id, stg_base: StgBase, md_period_agent_dic, date_from, date_to, **kwargs):
        super().__init__(stg_run_id=stg_run_id, stg_base=stg_base, run_mode=RunMode.Backtest,
                         md_period_agent_dic=md_period_agent_dic)
        # 回测 ID 每一次测试生成一个新的ID，在数据库中作为本次测试的唯一标识
        # TODO: 这一ID需要从数据库生成
        # self.backtest_id = 1
        # self.stg_base._trade_agent.backtest_id = self.backtest_id
        # 设置回测时间区间
        self.date_from = try_2_date(date_from)
        self.date_to = try_2_date(date_to)
        if not isinstance(self.date_from, date):
            raise ValueError("date_from: %s", date_from)
        if not isinstance(self.date_to, date):
            raise ValueError("date_from: %s", date_to)
        # 初始资金账户金额
        self.init_cash = kwargs['init_cash']
        # 载入回测时间段各个周期的历史数据，供回测使用
        # 对各个周期分别进行处理
        self.backtest_his_df_dic = {}
        for period, md_agent in self.md_period_agent_dic.items():
            md_df = md_agent.load_history(date_from, date_to, load_md_count=0)
            if md_df is None:
                continue
            if isinstance(md_df, pd.DataFrame):
                # 对于 CTP 老程序接口直接返回的是 df，因此补充相关的 key 数据
                # TODO: 未来这部分代码将逐步给更替
                warnings.warn('load_history 需要返回 dict 类型数据， 对 DataFame 的数据处理即将废弃', DeprecationWarning)
                if period == PeriodType.Tick:
                    his_df_dic = {'md_df': md_df,
                                  'date_key': 'ActionDay', 'time_key': 'ActionTime',
                                  'microseconds_key': 'ActionMillisec'}
                else:
                    his_df_dic = {'md_df': md_df,
                                  'date_key': 'ActionDay', 'time_key': 'ActionTime'}
                self.backtest_his_df_dic[period] = his_df_dic
                self.logger.debug('加载 %s 回测数据 %d 条记录', period, md_df.shape[0])
            else:
                self.backtest_his_df_dic[period] = his_df_dic = md_df
                self.logger.debug('加载 %s 回测数据 %d 条记录', period, his_df_dic['md_df'].shape[0])

    def run(self):
        """
        执行回测
        :return: 
        """
        # TODO: 以后再加锁，防止多线程，目前只是为了防止误操作导致的重复执行
        if self.keep_running:
            self.logger.warning('当前任务正在执行中..，避免重复执行')
            return
        else:
            self.keep_running = True
        self.logger.info('执行回测任务【%s - %s】开始', self.date_from, self.date_to)
        try:
            # 策略初始化
            self.stg_base.init()
            # 对每一个周期构建时间轴及对应记录的数组下标
            period_dt_idx_dic = {}
            for period, his_df_dic in self.backtest_his_df_dic.items():
                his_df = his_df_dic['md_df']
                datetime_s = his_df[his_df_dic['datetime_key']] if 'datetime_key' in his_df_dic else None
                date_s = his_df[his_df_dic['date_key']] if 'date_key' in his_df_dic else None
                time_s = his_df[his_df_dic['time_key']] if 'time_key' in his_df_dic else None
                microseconds_s = his_df[his_df_dic['microseconds_key']] if 'microseconds_key' in his_df_dic else None
                df_len = his_df.shape[0]
                # 整理日期轴
                dt_idx_dic = {}
                if datetime_s is not None:
                    for idx in range(df_len):
                        if microseconds_s:
                            dt = datetime_s[idx] + timedelta(microseconds=int(microseconds_s[idx]))
                        else:
                            dt = datetime_s[idx]

                        if dt in dt_idx_dic:
                            dt_idx_dic[dt].append(idx)
                        else:
                            dt_idx_dic[dt] = [idx]
                elif date_s is not None and time_s is not None:
                    for idx in range(df_len):
                        # action_date = date_s[idx]
                        # dt = datetime(action_date.year, action_date.month, action_date.day) + time_s[
                        #     idx] + timedelta(microseconds=int(microseconds_s[idx]))
                        if microseconds_s:
                            dt = datetime.combine(date_s[idx], time_s[idx]) + timedelta(
                                microseconds=int(microseconds_s[idx]))
                        else:
                            dt = datetime.combine(date_s[idx], time_s[idx])

                        if dt in dt_idx_dic:
                            dt_idx_dic[dt].append(idx)
                        else:
                            dt_idx_dic[dt] = [idx]

                # action_day_s = his_df['ActionDay']
                # action_time_s = his_df['ActionTime']
                # # Tick 数据 存在 ActionMillisec 记录秒以下级别数据
                # if period == PeriodType.Tick:
                #     action_milsec_s = his_df['ActionMillisec']
                #     dt_idx_dic = {}
                #     for idx in range(df_len):
                #         action_date = action_day_s[idx]
                #         dt = datetime(action_date.year, action_date.month, action_date.day) + action_time_s[
                #             idx] + timedelta(microseconds=int(action_milsec_s[idx]))
                #         if dt in dt_idx_dic:
                #             dt_idx_dic[dt].append(idx)
                #         else:
                #             dt_idx_dic[dt] = [idx]
                # else:
                #     dt_idx_dic = {}
                #     for idx in range(df_len):
                #         action_date = action_day_s[idx]
                #         dt = datetime(action_date.year, action_date.month, action_date.day) + action_time_s[
                #             idx]
                #         if dt in dt_idx_dic:
                #             dt_idx_dic[dt].append(idx)
                #         else:
                #             dt_idx_dic[dt] = [idx]
                # 记录各个周期时间戳
                period_dt_idx_dic[period] = dt_idx_dic

            # 按照时间顺序将各个周期数据依次推入对应 handler
            period_idx_df = pd.DataFrame(period_dt_idx_dic).sort_index()
            for row_num in range(period_idx_df.shape[0]):
                period_idx_s = period_idx_df.ix[row_num, :]
                for period, idx_list in period_idx_s.items():
                    if all(np.isnan(idx_list)):
                        continue
                    his_df = self.backtest_his_df_dic[period]['md_df']
                    for idx_row in idx_list:
                        # TODO: 这里存在着性能优化空间 DataFrame -> Series -> dict 效率太低
                        md = his_df.ix[idx_row].to_dict()
                        # 在回测阶段，需要对 trade_agent 设置最新的md数据，一遍交易接口确认相应的k线日期
                        self.stg_base.trade_agent.set_curr_md(period, md)
                        # 执行策略相应的事件响应函数
                        self.stg_base.on_period_md_handler(period, md)
                        # 根据最新的 md 及 持仓信息 更新 账户信息
                        self.stg_base.trade_agent.update_account_info()
            self.logger.info('执行回测任务【%s - %s】完成', self.date_from, self.date_to)
        finally:
            self.keep_running = False
            self.stg_run_ending()


class EventHandlersRelation:
    """
    用于记录事件类型与其对应的各种相关事件句柄之间的关系
    """

    def __init__(self, period_type, prepare_event, md_event, param_type):
        self.period_type = period_type
        self.prepare_event = prepare_event
        self.md_event = md_event
        self.param_type = param_type


class MACroseStg(StgBase):

    def __init__(self):
        super().__init__()
        self.ma5 = []
        self.ma10 = []

    def on_prepare_min1(self, md_df, context):
        if md_df:
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
                self.open_long(instrument_id, close, 1)
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
                self.open_short(instrument_id, close, 1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format=Config.LOG_FORMAT)
    # 参数设置
    strategy_params = {}
    md_agent_params_list = [{
        'name': 'min1',
        'md_period': PeriodType.Min1,
        'instrument_id_list': ['ethbtc'],  # ['jm1711', 'rb1712', 'pb1801', 'IF1710'],
        'init_md_date_to': '2017-9-1',
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
