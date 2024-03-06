import pandas as pd
import numpy as np
import rqdatac as rq
from datetime import datetime
from datetime import timedelta
from datetime import date
import glob
import warnings
warnings.filterwarnings('ignore')

# rqdatac API
rq.init(xxx)

def get_deal(date):
    # Stock Deal
    try:
        deal_stock = pd.read_csv(r'Z:\tca\data\{}\Stock\Deal.csv'.format(date), encoding='gbk')
        deal_stock = deal_stock[
            ['资金账号', '账号名称', '证券代码', '证券名称', '成交价格', '成交数量', '成交金额', '成交日期', '成交时间',
             '操作']]
        deal_stock['证券代码'] = deal_stock['证券代码'].apply(lambda x: str(x).zfill(6))
        deal_stock['成交日期'] = deal_stock['成交日期'].apply(
            lambda x: datetime.strptime(str(x), '%Y%m%d').strftime('%Y-%m-%d'))
    except:
        print('Problem occcurred when processing Z:\tca\data\{}\Stock\Deal.csv'.format(date))
        pass

    # Credit Deal
    try:
        deal_credit = pd.read_csv(r'Z:\tca\data\{}\Credit\Deal.csv'.format(date), encoding='gbk')
        deal_credit = deal_credit[
            ['资金账号', '账号名称', '证券代码', '证券名称', '成交价格', '成交数量', '成交金额', '成交日期', '成交时间',
             '操作']]
        deal_credit['证券代码'] = deal_credit['证券代码'].apply(lambda x: str(x).zfill(6))
        deal_credit['成交日期'] = deal_credit['成交日期'].apply(
            lambda x: datetime.strptime(str(x), '%Y%m%d').strftime('%Y-%m-%d'))
    except:
        print('Problem occcurred when processing Z:\tca\data\{}\Credit\Deal.csv'.format(date))
        pass

    # matic 普通
    try:
        path_matic_pt = glob.glob(r'Z:\tca\data\{}\matic_output\普通交易_成交报表*.csv'.format(date))[0]
        deal_matic_pt = pd.read_csv(path_matic_pt, encoding='gbk')
        deal_matic_pt = deal_matic_pt[
            ['资产账户', '账户名称', '证券代码', '证券名称', '成交价格', '成交数量', '成交金额', '成交时间',
             '买卖方向']]
        deal_matic_pt['证券代码'] = deal_matic_pt['证券代码'].apply(lambda x: str(x).zfill(6))
        deal_matic_pt['成交日期'] = deal_matic_pt['成交时间'].apply(
            lambda x: datetime.strptime(x, '%Y/%m/%d %H:%M:%S').strftime('%Y-%m-%d'))
        deal_matic_pt['成交时间'] = deal_matic_pt['成交时间'].apply(
            lambda x: datetime.strptime(x, '%Y/%m/%d %H:%M:%S').strftime('%H:%M:%S'))
        deal_matic_pt['买卖方向'] = deal_matic_pt['买卖方向'].apply(lambda x: x[2:])
        temp_pt = deal_matic_pt.pop('成交日期')
        deal_matic_pt.insert(loc=7, column='成交日期', value=temp_pt)
        deal_matic_pt.columns = ['资金账号', '账号名称', '证券代码', '证券名称', '成交价格', '成交数量', '成交金额',
                                 '成交日期', '成交时间', '操作']
    except:
        print('Problem occcurred when processing Z:\tca\data\{}\matic_output\普通交易_成交报表*.csv'.format(date))
        pass

    try:
        # matic 信用
        path_matic_xy = glob.glob(r'Z:\tca\data\{}\matic_output\信用交易_成交报表*.csv'.format(date))[0]
        deal_matic_xy = pd.read_csv(path_matic_xy, encoding='gbk')
        deal_matic_xy = deal_matic_xy[
            ['资产账户', '账户名称', '证券代码', '证券名称', '成交价格', '成交数量', '成交金额', '成交时间',
             '买卖方向']]
        deal_matic_xy['证券代码'] = deal_matic_xy['证券代码'].apply(lambda x: str(x).zfill(6))
        deal_matic_xy['成交日期'] = deal_matic_xy['成交时间'].apply(
            lambda x: datetime.strptime(x, '%Y/%m/%d %H:%M:%S').strftime('%Y-%m-%d'))
        deal_matic_xy['成交时间'] = deal_matic_xy['成交时间'].apply(
            lambda x: datetime.strptime(x, '%Y/%m/%d %H:%M:%S').strftime('%H:%M:%S'))
        deal_matic_xy['买卖方向'] = deal_matic_xy['买卖方向'].apply(lambda x: x[3:])
        temp_xy = deal_matic_xy.pop('成交日期')
        deal_matic_xy.insert(loc=7, column='成交日期', value=temp_xy)
        deal_matic_xy.columns = ['资金账号', '账号名称', '证券代码', '证券名称', '成交价格', '成交数量', '成交金额',
                                 '成交日期', '成交时间', '操作']
    except:
        print('Problem occcurred when processing Z:\tca\data\{}\matic_output\信用交易_成交报表*.csv'.format(date))
        pass

    # 合并
    deal = pd.concat([deal_stock, deal_credit, deal_matic_pt, deal_matic_xy])
    deal.reset_index(drop=True, inplace=True)

    return deal


def get_mkt_vwap(deal):
    # get market data from rq
    date_list = sorted([*set(deal['成交日期'].tolist())])
    stocks_id = rq.all_instruments('CS')['order_book_id'].tolist()
    data = rq.get_price(stocks_id, start_date=date_list[0], end_date=date_list[-1], fields=['total_turnover', 'volume'])

    VWAP_mkt = data.reset_index()
    VWAP_mkt.columns = ['证券代码', '成交日期', '成交金额', '成交数量']
    VWAP_mkt['成交日期'] = VWAP_mkt['成交日期'].apply(lambda x: x.strftime('%Y-%m-%d'))
    VWAP_mkt['证券代码'] = VWAP_mkt['证券代码'].apply(lambda x: x.split('.')[0])
    VWAP_mkt['VWAP_mkt'] = VWAP_mkt['成交金额'] / VWAP_mkt['成交数量']
    VWAP_mkt = VWAP_mkt[['成交日期', '证券代码', 'VWAP_mkt']]
    VWAP_mkt = VWAP_mkt.rename(columns={'证券代码': '代码'})
    VWAP_mkt = VWAP_mkt.set_index(['成交日期', '代码'])

    return VWAP_mkt

def get_mkt_value(date):
    # stock market value
    mkt_value_stock = pd.read_csv(r'Z:\tca\data\{}\Stock\Account.csv'.format(date), encoding='gbk')
    mkt_value_stock = mkt_value_stock[['交易日', '资金账号', '账号名称', '总市值']]
    mkt_value_stock['交易日'] = mkt_value_stock['交易日'].apply(
        lambda x: datetime.strptime(str(x), '%Y%m%d').strftime('%Y-%m-%d'))
    mkt_value_stock.rename(columns={'交易日': '成交日期'}, inplace=True)

    # credit market value
    mkt_value_credit = pd.read_csv(r'Z:\tca\data\{}\Credit\Account.csv'.format(date), encoding='gbk')
    mkt_value_credit = mkt_value_credit[['交易日', '资金账号', '账号名称', '总市值']]
    mkt_value_credit['交易日'] = mkt_value_credit['交易日'].apply(
        lambda x: datetime.strptime(str(x), '%Y%m%d').strftime('%Y-%m-%d'))
    mkt_value_credit.rename(columns={'交易日': '成交日期'}, inplace=True)

    # matic 普通
    path_matic_pt = glob.glob(r'Z:\tca\data\{}\matic_output\普通交易_资产报表*.csv'.format(date))[0]
    mkt_value_matic_pt = pd.read_csv(path_matic_pt, encoding='gbk')
    mkt_value_matic_pt = mkt_value_matic_pt[['日期', '资产账户', '账户名称', '证券市值']]
    mkt_value_matic_pt['日期'] = mkt_value_matic_pt['日期'].apply(
        lambda x: datetime.strptime(str(x), '%Y%m%d').strftime('%Y-%m-%d'))
    mkt_value_matic_pt.columns = ['成交日期', '资金账号', '账号名称', '总市值']

    # matic 信用
    try:
        path_matic_xy = glob.glob(r'Z:\tca\data\{}\matic_output\信用交易_资产报表*.csv'.format(date))[0]
        mkt_value_matic_xy = pd.read_csv(path_matic_xy, encoding='gbk')
        mkt_value_matic_xy = mkt_value_matic_xy[['资产账号', '账户名称', '证券市值']]
        mkt_value_matic_xy.insert(loc=0, column='日期', value=date)
        mkt_value_matic_xy['日期'] = mkt_value_matic_xy['日期'].apply(
            lambda x: datetime.strptime(str(x), '%Y%m%d').strftime('%Y-%m-%d'))
        mkt_value_matic_xy.columns = ['成交日期', '资金账号', '账号名称', '总市值']
    except:
        mkt_value_matic_xy = pd.DataFrame(columns=['成交日期', '资金账号', '账号名称', '总市值'])

    # 合并
    mkt_value = pd.concat([mkt_value_stock, mkt_value_credit, mkt_value_matic_pt, mkt_value_matic_xy])
    mkt_value.reset_index(drop=True, inplace=True)
    mkt_value = mkt_value.drop(index=mkt_value.loc[mkt_value['总市值'] == 0].index)
    mkt_value.reset_index(drop=True, inplace=True)

    return mkt_value


def calculate_vwap_slippage(deal, date):
    # calculate self deal VWAP
    df_buy = deal.loc[deal['操作'] == '买入']
    df_sell = deal.loc[deal['操作'] == '卖出']
    df_buy = pd.DataFrame(df_buy.groupby(['成交日期', '资金账号', '证券代码'])['成交金额', '成交数量'].sum())
    df_sell = pd.DataFrame(df_sell.groupby(['成交日期', '资金账号', '证券代码'])['成交金额', '成交数量'].sum())
    df_buy['VWAP_deal'] = df_buy['成交金额'] / df_buy['成交数量']
    df_sell['VWAP_deal'] = df_sell['成交金额'] / df_sell['成交数量']

    # calculate market VWAP
    VWAP_mkt = get_mkt_vwap(deal)
    df_buy = df_buy.join(VWAP_mkt, on=['成交日期', '证券代码'])
    df_sell = df_sell.join(VWAP_mkt, on=['成交日期', '证券代码'])
    df_buy['amount_deal'] = df_buy['成交数量'] * df_buy['VWAP_deal']
    df_sell['amount_deal'] = df_sell['成交数量'] * df_sell['VWAP_deal']
    df_buy['amount_mkt'] = df_buy['成交数量'] * df_buy['VWAP_mkt']
    df_sell['amount_mkt'] = df_sell['成交数量'] * df_sell['VWAP_mkt']

    # calculate difference and percentage diff
    df_buy['diff'] = df_buy['amount_mkt'] - df_buy['amount_deal']
    df_sell['diff'] = df_sell['amount_deal'] - df_sell['amount_mkt']
    excess_buy = df_buy.reset_index().groupby(by=['成交日期', '资金账号'])['diff'].sum()
    excess_sell = df_sell.reset_index().groupby(by=['成交日期', '资金账号'])['diff'].sum()
    vwap_base_buy = df_buy.reset_index().groupby(by=['成交日期', '资金账号'])['amount_deal'].sum()
    vwap_base_sell = df_sell.reset_index().groupby(by=['成交日期', '资金账号'])['amount_deal'].sum()
    mkt_value = get_mkt_value(date)
    mv_base = mkt_value.groupby(by=['成交日期', '资金账号'])['总市值'].sum()

    excess_total = pd.concat([excess_buy, excess_sell, vwap_base_buy, vwap_base_sell], axis=1)
    excess_total.reset_index(inplace=True)
    excess_total = pd.merge(excess_total, mv_base, on=['成交日期', '资金账号'], how='left')
    excess_total.columns = ['成交日期', '资金账号', 'excess_buy', 'excess_sell', 'base_buy', 'base_sell', 'mkt_value']
    excess_total[['excess_buy', 'excess_sell', 'base_buy', 'base_sell']] = excess_total[
        ['excess_buy', 'excess_sell', 'base_buy', 'base_sell']].fillna(0)

    final = calculate_ratio(excess_total)

    return final


def calculate_twap_slippage(deal, date):
    # merge TWAP data
    TWAP_mkt = pd.read_csv(r'Z:\tca\TWAP_data\TWAP_%s.csv' % date)
    TWAP_mkt['证券代码'] = TWAP_mkt['证券代码'].apply(lambda x: str(x).zfill(6))
    df = pd.merge(deal, TWAP_mkt, on=['成交日期', '证券代码'], how='left')

    # calculate difference and percentage diff
    df['amount_deal'] = df['成交价格'] * df['成交数量']
    df['amount_mkt'] = df['TWAP_mkt'] * df['成交数量']
    df_buy = df.loc[df['操作'] == '买入']
    df_sell = df.loc[df['操作'] == '卖出']
    df_buy['diff'] = df_buy['amount_mkt'] - df_buy['amount_deal']
    df_sell['diff'] = df_sell['amount_deal'] - df_sell['amount_mkt']
    excess_buy = df_buy.reset_index().groupby(by=['成交日期', '资金账号'])['diff'].sum()
    excess_sell = df_sell.reset_index().groupby(by=['成交日期', '资金账号'])['diff'].sum()
    twap_base_buy = df_buy.reset_index().groupby(by=['成交日期', '资金账号'])['amount_deal'].sum()
    twap_base_sell = df_sell.reset_index().groupby(by=['成交日期', '资金账号'])['amount_deal'].sum()
    mkt_value = get_mkt_value(date)
    mv_base = mkt_value.groupby(by=['成交日期', '资金账号'])['总市值'].sum()

    excess_total = pd.concat([excess_buy, excess_sell, twap_base_buy, twap_base_sell], axis=1)
    excess_total.reset_index(inplace=True)
    excess_total = pd.merge(excess_total, mv_base, on=['成交日期', '资金账号'], how='left')
    excess_total.columns = ['成交日期', '资金账号', 'excess_buy', 'excess_sell', 'base_buy', 'base_sell', 'mkt_value']
    excess_total[['excess_buy', 'excess_sell', 'base_buy', 'base_sell']] = excess_total[
        ['excess_buy', 'excess_sell', 'base_buy', 'base_sell']].fillna(0)

    final = calculate_ratio(excess_total)

    return final

def calculate_ratio(excess_total):
    # buy & sell side
    excess_total['pct_buy(deal base)'] = excess_total['excess_buy'] / excess_total['base_buy']
    excess_total['pct_sell(deal base)'] = excess_total['excess_sell'] / excess_total['base_sell']
    excess_total['pct_buy(mv base)'] = excess_total['excess_buy'] / excess_total['mkt_value']
    excess_total['pct_sell(mv base)'] = excess_total['excess_sell'] / excess_total['mkt_value']
    # total
    excess_total['excess_total'] = excess_total['excess_buy'] + excess_total['excess_sell']
    excess_total['base_total'] = excess_total['base_buy'] + excess_total['base_sell']
    excess_total['pct_total(deal base)'] = excess_total['excess_total'] / excess_total['base_total']
    excess_total['pct_total(mv base)'] = excess_total['excess_total'] / excess_total['mkt_value']

    # add account info
    account = pd.read_excel(r'Z:\tca\交易账户表.xlsx')
    final = pd.merge(account, excess_total, on='资金账号', how='right')
    final = final.drop(index=final[final['账号名称'].isna()].index)
    final.reset_index(drop=True, inplace=True)
    final = final.groupby(['成交日期', '资金账号', '账号名称'])[
        'pct_buy(deal base)', 'pct_sell(deal base)', 'pct_total(deal base)', 'pct_buy(mv base)', 'pct_sell(mv base)', 'pct_total(mv base)'].mean().reset_index()

    return final

def update_hist_deal():
    # set current date
    lag_n = 0  # today = 0
    curr_date = (date.today() - timedelta(lag_n)).strftime('%Y%m%d')
    select_date = datetime.strptime(curr_date,'%Y%m%d').strftime('%Y-%m-%d')

    hist_deal = pd.read_excel(r'Z:\tca\历史VWAP成交差异.xlsx')
    hist_deal = hist_deal.drop(hist_deal.loc[hist_deal['成交日期'] == select_date].index)
    hist_deal2 = pd.read_excel(r'Z:\tca\历史TWAP成交差异.xlsx')
    hist_deal2 = hist_deal2.drop(hist_deal2.loc[hist_deal2['成交日期'] == select_date].index)

    deal = get_deal(curr_date)
    # update historical VWAP difference
    deal_add = calculate_vwap_slippage(deal, curr_date)
    hist_deal = pd.concat([hist_deal, deal_add])
    hist_deal.reset_index(drop=True, inplace=True)
    hist_deal.to_excel(r'Z:\tca\历史VWAP成交差异.xlsx', index=False)
    # update historical TWAP difference
    deal_add2 = calculate_twap_slippage(deal, curr_date)
    hist_deal2 = pd.concat([hist_deal2, deal_add2])
    hist_deal2.reset_index(drop=True, inplace=True)
    hist_deal2.to_excel(r'Z:\tca\历史TWAP成交差异.xlsx', index=False)


# excute
update_hist_deal()
