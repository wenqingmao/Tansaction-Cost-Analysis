import pandas as pd
import numpy as np
from datetime import date
from datetime import timedelta
from datetime import datetime
import re
import glob
import rqdatac as rq

# rqdatac API
rq.init(xxx)

neutral = ['伏波1号','伏波2号','伏波3号','伏波5号','扶摇2号','之恒1000对冲1号','春晓对冲1号']

################ set number of lag date ################
lag_n = 0
curr_date = (date.today() - timedelta(lag_n)).strftime('%Y%m%d')

def get_marketvalue(date):  # date格式：yyyy-mm-dd

    typelist = ['Credit', 'Stock', 'matic_output']
    position = pd.DataFrame(columns=['产品名称', '资金账号', '证券代码', '当前拥股'])
    for name in typelist:
        ########################  ########################
        if name != 'matic_output':
            try:
                position_add = pd.read_csv(r'Z:\tca\data\{0}\{1}\PositionDetail.csv'.format(date, name), encoding='gbk',
                                           dtype={'证券代码': object, '资金账号': object})
                position_add = position_add[['产品名称', '资金账号', '证券代码', '当前拥股']]
                for i in range(len(position_add['产品名称'])):
                    try:
                        if re.findall('罗维盈安(.*)', position_add.loc[i, '产品名称'])[0]:
                            position_add.loc[i, '产品名称'] = \
                            re.findall('罗维盈安(.*)', position_add.loc[i, '产品名称'])[0]
                    except:
                        pass
                position_add = position_add.drop(position_add.loc[position_add['资金账号'] == '666810004062'].index)
                position = pd.concat([position, position_add])
            except:
                print(r'No such a file Z:\tca\data\{0}\{1}\PositionDetail.csv'.format(date, name))
                pass

                ######################## Matic平台 ########################
        if name == 'matic_output':
            try:
                path1 = glob.glob(r'Z:\tca\data\{0}\{1}\普通交易_持仓报表_*.csv'.format(date, name))[0]
                position_add = pd.read_csv(path1, encoding='gbk', dtype={'证券代码': object, '资产账户': object})
                position_add = position_add[['产品名称', '资产账户', '证券代码', '当前持仓']]
                position_add.columns = ['产品名称', '资金账号', '证券代码', '当前拥股']
                position_add['产品名称'] = position_add['产品名称'].apply(
                    lambda x: re.findall('罗维盈安(.*?)私募证券投资基金', x)[0])
                position = pd.concat([position, position_add])
            except:
                print(r'No such a file Z:\tca\data\{0}\{1}\普通交易_持仓报表_*.csv'.format(date, name))
                pass

            try:
                path2 = glob.glob(r'Z:\tca\data\{0}\{1}\信用交易_持仓报表_*.csv'.format(date, name))[0]
                position_add = pd.read_csv(path2, encoding='gbk', dtype={'证券代码': object, '资产账户': object})
                position_add = position_add[['产品名称', '资产账户', '证券代码', '当前持仓']]
                position_add.columns = ['产品名称', '资金账号', '证券代码', '当前拥股']
                position_add['产品名称'].replace('罗维量化金1选号私募证券投资基金',
                                                 '罗维盈安量化金1选号私募证券投资基金', inplace=True)
                position_add['产品名称'] = position_add['产品名称'].apply(
                    lambda x: re.findall('罗维盈安(.*?)私募证券投资基金', x)[0])
                position = pd.concat([position, position_add])
            except:
                print(r'No such a file Z:\tca\data\{0}\{1}\信用交易_持仓报表_*.csv'.format(date, name))
                pass
    position.reset_index(drop=True, inplace=True)

    date1 = datetime.strptime(date, '%Y%m%d')
    date1 = date1.strftime('%Y-%m-%d')
    close = get_close(date1)
    position = pd.merge(position, close, on='证券代码', how='left')  # merge close price
    position['持仓市值'] = position['当前拥股'] * position['收盘价']
    market_value = pd.DataFrame(position.groupby('产品名称')['持仓市值'].sum())
    market_value.reset_index(inplace=True)

    return market_value


def get_close(date):  # apidate format：yyyy-mm-dd

    # 沪深股票 收盘价
    stocks_id = rq.all_instruments('CS')['order_book_id'].tolist()
    data = rq.get_price(stocks_id, start_date=date, end_date=date, fields='close')
    close = data.droplevel(1)
    close = close.reset_index()
    close.columns = ['证券代码', '收盘价']
    close['证券代码'] = close['证券代码'].apply(lambda x: x.split('.')[0])

    return close


def notional_future(date):
    volume = pd.read_excel(r'Z:\tca\data\{0}\futures_{1}.xlsx'.format(date, date), sheet_name='volume', index_col=0)
    volume = volume.iloc[:, :-1]
    info = pd.read_excel(r'Z:\tca\data\{0}\futures_{1}.xlsx'.format(date, date), sheet_name='info', index_col=0)
    info.columns = [re.findall('(.*?).CFE', n)[0] for n in info.columns]
    notional = np.sum(info.loc['结算价'] * info.loc['合约乘数'] * volume, axis=1).reset_index()
    notional.columns = ['产品名称', '期货名义市值']

    return notional


def securities_loan(date):
    loan = pd.DataFrame(columns=['资金账号', '账号名称', '融券负债'])
    # Credit
    loan_add = pd.read_csv(r'Z:\tca\data\{}\Credit\ComprehInfo.csv'.format(date), encoding='gbk', dtype={'资金账号': str})
    loan_add = loan_add[['资金账号', '账号名称', '融券负债']]
    # loan_add.columns = ['资金账号', '账号名称', '融券负债']
    loan = pd.concat([loan, loan_add])
    # matic
    try:
        path2 = glob.glob(r'Z:\tca\data\{0}\matic_output\信用交易_资产报表*.csv'.format(date))[0]
        loan_add = pd.read_csv(path2, encoding='gbk', dtype={'资产账号': str})
        loan_add = loan_add[['资产账号', '账户名称', '融券市值']]
        loan_add.columns = ['资金账号', '账号名称', '融券负债']
        for i in range(2):
            loan_add.loc[i, '账号名称'] = re.findall('(.*?)私募证券投资基金', loan_add.loc[i, '账号名称'])[0]
        loan = pd.concat([loan, loan_add])
    except:
        print(r'No such a file Z:\tca\data\{0}\matic_output\信用交易_资产报表*.csv'.format(date, name))
        pass

    prodacc = pd.read_excel(r'Z:\tca\产品账户表.xlsx', dtype={'资金账号': str})
    loan = pd.merge(loan, prodacc, on='资金账号', how='left')
    loan = loan.groupby('产品名称')['融券负债'].sum().reset_index()
    return loan

def add_date(data):
    d = (date.today() - timedelta(lag_n)).strftime('%Y-%m-%d')
    data.insert(loc=0,column='Date',value=d, allow_duplicates=False)


def get_exposure(date):
    # short
    future = notional_future(date)
    loan = securities_loan(date)
    loan = loan.loc[loan['产品名称'].isin(neutral)]
    short_position = pd.merge(future, loan, on='产品名称', how='left').fillna(0)
    short_position['空头市值'] = short_position['期货名义市值'] + short_position['融券负债']
    # long
    market_value = get_marketvalue(date)
    long_position = market_value.loc[market_value['产品名称'].isin(neutral)]
    long_position.reset_index(drop=True, inplace=True)

    exposure = pd.merge(short_position, long_position, on='产品名称', how='left')
    exposure['多空敞口'] = exposure['持仓市值'] - exposure['空头市值']
    exposure['空头/多头'] = exposure['空头市值'] / exposure['持仓市值']
    add_date(exposure)

    return exposure

def get_risk_level(date):
    volume = pd.read_excel(r'Z:\tca\data\{0}\futures_{1}.xlsx'.format(date, date), sheet_name='volume', index_col=0)
    dynamic = volume.iloc[:, -1].reset_index()
    dynamic.columns = ['产品名称', '动态权益']
    volume = volume.iloc[:, :-1]
    info = pd.read_excel(r'Z:\tca\data\{0}\futures_{1}.xlsx'.format(date, date), sheet_name='info', index_col=0)
    info.columns = [re.findall('(.*?).CFE', n)[0] for n in info.columns]
    margin = np.sum(volume * info.loc['结算价'] * info.loc['合约乘数'] * info.loc['保证金比例'] / 100,
                    axis=1).reset_index()
    margin.columns = ['产品名称', '保证金占用']
    risk_level = pd.merge(margin, dynamic, on='产品名称', how='left')
    risk_level['风险度'] = risk_level['保证金占用'] / risk_level['动态权益']
    add_date(risk_level)

    return risk_level

def add_historical(exposure,risk_level):
    hist_data = pd.read_excel(r'Z:\tca\中性产品多空比例.xlsx')
    hist_data2 = pd.read_excel(r'Z:\tca\期货风险度.xlsx')
    select_date = (date.today() - timedelta(lag_n)).strftime('%Y-%m-%d')
    hist_data = hist_data.drop(hist_data.loc[hist_data['Date'] == select_date].index)
    hist_data2 = hist_data2.drop(hist_data2.loc[hist_data2['Date'] == select_date].index)
    hist_data = pd.concat([hist_data, exposure])
    hist_data2 = pd.concat([hist_data2, risk_level])
    hist_data.to_excel(r'Z:\tca\中性产品多空比例.xlsx', index=False)
    hist_data2.to_excel(r'Z:\tca\期货风险度.xlsx', index=False)

    return hist_data

#############################  计算多空比  #############################
exposure = get_exposure(curr_date)
risk_level = get_risk_level(curr_date)
add_historical(exposure,risk_level)