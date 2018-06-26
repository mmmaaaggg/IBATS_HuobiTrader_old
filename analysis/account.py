# -*- coding: utf-8 -*-
"""
Created on 2018/1/14
@author: MG
"""
import matplotlib.pyplot as plt
from abat.utils.fh_utils import return_risk_analysis
import pandas as pd
import numpy as np
import logging
from abat.backend.orm import AccountStatusInfo, StgRunInfo
from abat.backend import engine_abat
from abat.utils.db_utils import with_db_session
from sqlalchemy import func, or_, and_, column, not_

logger = logging.getLogger()


def get_account_balance(stg_run_id):
    """
    获取 account_info 账户走势数据
    :param stg_run_id: 
    :return: 
    """
    with with_db_session(engine_abat) as session:
        sql_str = str(
            session.query(
                func.concat(AccountStatusInfo.trade_date, ' ', AccountStatusInfo.trade_time).label('trade_datetime'),
                AccountStatusInfo.available_cash.label('available_cash'),
                AccountStatusInfo.curr_margin.label('curr_margin'),
                AccountStatusInfo.balance_tot.label('balance_tot')
            ).filter(AccountStatusInfo.stg_run_id == stg_run_id).order_by(
                AccountStatusInfo.trade_date, AccountStatusInfo.trade_time
            )
        )
    # sql_str = """SELECT concat(trade_date, " ", trade_time) trade_datetime, available_cash, curr_margin, balance_tot
    #   FROM account_status_info where stg_run_id=%s order by trade_date, trade_time"""
    data_df = pd.read_sql(sql_str, engine_abat, params=[' ', stg_run_id])
    data_df["return_rate"] = (data_df["balance_tot"].pct_change().fillna(0) + 1).cumprod()
    data_df = data_df.set_index("trade_datetime")
    return data_df


if __name__ == "__main__":
    with with_db_session(engine_abat) as session:
        # stg_run_id = session.execute("select max(stg_run_id) from stg_run_info").fetchone()[0]
        stg_run_id = session.query(func.max(StgRunInfo.stg_run_id)).scalar()
    # stg_run_id = 2
    data_df = get_account_balance(stg_run_id)

    logger.info("\n%s", data_df)
    data_df.plot(ylim=[min(data_df["available_cash"]), max(data_df["balance_tot"])])
    data_df.plot(ylim=[min(data_df["curr_margin"]), max(data_df["curr_margin"])])
    stat_df = return_risk_analysis(data_df[['return_rate']], freq=None)
    logger.info("\n%s", stat_df)
    plt.show()
