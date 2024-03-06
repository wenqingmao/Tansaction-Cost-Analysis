import pandas as pd
import numpy as np
from datetime import date
from datetime import timedelta
from datetime import datetime
import re
import glob
import rqdatac as rq

# rqdatac API
rq.init('license', 'MAFPix3ir-DdV81aNkxATTdYOhryZfY0-ExgfKQ3WGLysJT1-yAVAJcTzeoqv_ciOpj8ikC4rALuQ_AIp2Ny_D4VtjjEFwTLu9Rb8sY1fn3eya9iUghS15S8itTdsYefHHGFwhFadAO-VgcfnUnq_B-QgZNT3U4AnK0eon93_2Y=ZsvQzvVU-5XMDSZUDtIb__NfgxXL3Rq1MKolsrUS7aTZKN-g4Ngb8OdFBRwT4NqciBY5Jha5RvJyFYBTFB8Qgs6AthmhVX3vATneRTp-dqChgcAkD_n8eU-xdAzszgzhL3uQs8LJZFhHn4zRsTqvHhpRf70mbq0DqOt3y-pJWaw=', ("rqdatad-pro.ricequant.com", 16011))

# strategy baskets
csi500 = ['扶摇1号','牧起3号','凌云增强2号','牧起1号','牧起2号','罗维量化金选1号']
csi1000 = ['之升1000增强1号','安之升100增强2号']
neutral = ['伏波1号','伏波2号','伏波3号','伏波5号','扶摇2号','之恒1000对冲1号','春晓对冲1号']

################ set number of lag date ################
lag_n = 0    # today = 0

# def get_asset(date):  # date format: yyymmdd
#
#     typelist = ['Credit', 'Stock', 'matic_output']
#     account = pd.DataFrame(columns=['资金账号', '账号名称', '可用金额'])
#     for name in typelist:
#         ################### 普通账户 ###################
#         if name == 'Stock':
#             try:
#                 account_add = pd.read_csv(r'Z:\tca\data\{0}\{1}\Account.csv'.format(date, name), encoding='gbk',
#                                           dtype={'资金账号': str})
#                 account_add = account_add[['资金账号', '账号名称', '可用金额', '总市值']]
#                 account_add['总负债'] = 0
#                 # 删除与matic平台重复的账户
#                 account_add = account_add.drop(account_add.loc[account_add['资金账号'] == '666810004062'].index)
#                 account = pd.concat([account, account_add])
#             except:
#                 print(r'No such a file Z:\tca\data\{0}\{1}\Account.csv'.format(date, name))
#                 pass
#
#         ################### 信用账户 ###################
#         if name == 'Credit':
#             try:
#                 account_add = pd.read_csv(r'Z:\tca\data\{0}\{1}\Account.csv'.format(date, name), encoding='gbk',
#                                           dtype={'资金账号': str})
#                 account_add = account_add[['资金账号', '账号名称', '可用金额', '总市值', '总负债']]
#                 account = pd.concat([account, account_add])
#             except:
#                 print(r'No such a file Z:\tca\data\{0}\{1}\Account.csv'.format(date, name))
#                 pass
#
#         ################### matic平台 ###################
#         if name == 'matic_output':
#             try:
#                 path1 = glob.glob(r'Z:\tca\data\{0}\{1}\普通交易_资产报表*.csv'.format(date, name))[0]
#                 account_add = pd.read_csv(path1, encoding='gbk', dtype={'资产账户': str})
#                 account_add = account_add[['资产账户', '账户名称', '可用资金', '证券市值']]
#                 account_add.columns = ['资金账号', '账号名称', '可用金额', '总市值']
#                 account_add['总负债'] = 0
#                 account_add['账号名称'] = account_add['账号名称'].apply(
#                     lambda x: re.findall('(.*?)私募证券投资基金', x)[0])
#                 account = pd.concat([account, account_add])
#             except:
#                 print(r'No such a file Z:\tca\data\{0}\{1}\普通交易_资产报表*.csv'.format(date, name))
#                 pass
#             try:
#                 path2 = glob.glob(r'Z:\tca\data\{0}\{1}\信用交易_资产报表*.csv'.format(date, name))[0]
#                 account_add = pd.read_csv(path2, encoding='gbk', dtype={'资产账号': str})
#                 account_add = account_add[['资产账号', '账户名称', '现金资产', '证券市值', '合约总负债']]
#                 account_add.columns = ['资金账号', '账号名称', '可用金额', '总市值', '总负债']
#                 account_add['账号名称'] = account_add['账号名称'].apply(
#                     lambda x: re.findall('(.*?)私募证券投资基金', x)[0])
#                 account = pd.concat([account, account_add])
#             except:
#                 print(r'No such a file Z:\tca\data\{0}\{1}\信用交易_资产报表*.csv'.format(date, name))
#                 pass
#
#     account.reset_index(drop=True, inplace=True)
#     prodacc = pd.read_excel(r'Z:\tca\产品账户表.xlsx', dtype={'资金账号': str})
#     account = pd.merge(account, prodacc, on='资金账号', how='left')
#     asset = pd.DataFrame(account.groupby('产品名称')[['总市值', '可用金额', '总负债']].sum())
#     asset.reset_index(inplace=True)
#
#     return asset

# 更换成使用净资产计算
def get_asset(date):  # date format: yyymmdd

    typelist = ['Credit', 'Stock', 'matic_output']
    account = pd.DataFrame(columns=['资金账号', '账号名称', '可用金额', '总市值', '总资产', '总负债', '净资产'])
    for name in typelist:
        ################### 普通账户 ###################
        if name == 'Stock':
            try:
                account_add = pd.read_csv(r'Z:\tca\data\{0}\{1}\Account.csv'.format(date, name), encoding='gbk',
                                          dtype={'资金账号': str})
                account_add = account_add[['资金账号', '账号名称', '可用金额', '总市值', '总资产']]
                account_add['总负债'] = 0
                account_add['净资产'] = account_add['总市值'] + account_add['可用金额'] - account_add['总负债']
                # 删除与matic平台重复的账户
                account_add = account_add.drop(account_add.loc[account_add['资金账号'] == '666810004062'].index)
                account = pd.concat([account, account_add])
            except:
                print(r'No such a file Z:\tca\data\{0}\{1}\Account.csv'.format(date, name))
                pass

        ################### 信用账户 ###################
        if name == 'Credit':
            try:
                account_add = pd.read_csv(r'Z:\tca\data\{0}\{1}\Account.csv'.format(date, name), encoding='gbk',
                                          dtype={'资金账号': str})
                account_add = account_add[['资金账号', '账号名称', '可用金额', '总市值', '总资产', '总负债']]
                account_add['净资产'] = account_add['总资产'] - account_add['总负债']
                account = pd.concat([account, account_add])
            except:
                print(r'No such a file Z:\tca\data\{0}\{1}\Account.csv'.format(date, name))
                pass

        ################### matic平台 ###################
        if name == 'matic_output':
            try:
                path1 = glob.glob(r'Z:\tca\data\{0}\{1}\普通交易_资产报表*.csv'.format(date, name))[0]
                account_add = pd.read_csv(path1, encoding='gbk', dtype={'资产账户': str})
                account_add = account_add[['资产账户', '账户名称', '可用资金', '证券市值', '总资产']]
                account_add.columns = ['资金账号', '账号名称', '可用金额', '总市值', '总资产']
                account_add['总负债'] = 0
                account_add['净资产'] = account_add['总资产'] - account_add['总负债']
                for i in range(3):
                    account_add.loc[i,'账号名称'] = re.findall('(.*?)私募证券投资基金',account_add.loc[i,'账号名称'])
                account = pd.concat([account, account_add])
            except:
                print(r'No such a file Z:\tca\data\{0}\{1}\普通交易_资产报表*.csv'.format(date, name))
                pass
            try:
                path2 = glob.glob(r'Z:\tca\data\{0}\{1}\信用交易_资产报表*.csv'.format(date, name))[0]
                account_add = pd.read_csv(path2, encoding='gbk', dtype={'资产账号': str})
                account_add = account_add[['资产账号', '账户名称', '现金资产', '证券市值', '合约总负债', '净资产']]
                account_add.columns = ['资金账号', '账号名称', '可用金额', '总市值', '总负债', '净资产']
                for i in range(2):
                    account_add.loc[i,'账号名称'] = re.findall('(.*?)私募证券投资基金',account_add.loc[i,'账号名称'])
                account = pd.concat([account, account_add])
            except:
                print(r'No such a file Z:\tca\data\{0}\{1}\信用交易_资产报表*.csv'.format(date, name))
                pass

    account.reset_index(drop=True, inplace=True)
    prodacc = pd.read_excel(r'Z:\tca\产品账户表.xlsx', dtype={'资金账号': str})
    account = pd.merge(account, prodacc, on='资金账号', how='left')
    asset = pd.DataFrame(account.groupby('产品名称')[['总市值', '可用金额', '总负债', '净资产']].sum())
    asset.reset_index(inplace=True)

    return asset

# def totalequity(date, basket):  # date格式：yyyy-mm-dd
#
#     #     cash = get_cash(date)
#     #     marketvalue = get_marketvalue(date)
#     #     total_equity = pd.merge(marketvalue,cash,on='产品名称',how='left')
#     total_equity = get_asset(date)
#     total_equity['股票总权益'] = total_equity['总市值'] + total_equity['可用金额'] - total_equity['总负债']
#     total_equity = total_equity.loc[total_equity['产品名称'].isin(eval(basket))]
#     total_equity = total_equity.sort_values(by='产品名称')
#     total_equity.reset_index(drop=True, inplace=True)
#
#     return total_equity

def totalequity(date, basket):  # date格式：yyyy-mm-dd

    #     cash = get_cash(date)
    #     marketvalue = get_marketvalue(date)
    #     total_equity = pd.merge(marketvalue,cash,on='产品名称',how='left')
    total_equity = get_asset(date)
    total_equity['股票总权益'] = total_equity['净资产']
    #     total_equity['股票总权益'] = total_equity['总市值'] + total_equity['可用金额'] - total_equity['总负债']
    total_equity = total_equity.loc[total_equity['产品名称'].isin(eval(basket))]
    total_equity = total_equity.sort_values(by='产品名称')
    total_equity.reset_index(drop=True, inplace=True)

    return total_equity


def get_benchmark(date_t,date_t_1,basket):
    if basket == 'csi500':
        code = '000905.XSHG'
    if basket == 'csi1000':
        code = '000852.XSHG'
    benchmark_t_1 = rq.get_price(code, start_date=date_t_1, end_date=date_t_1,fields='close').droplevel(1).values[0][0]
    benchmark_t = rq.get_price(code, start_date=date_t, end_date=date_t,fields='close').droplevel(1).values[0][0]
    benchmark_return = benchmark_t / benchmark_t_1 - 1

    return benchmark_return


def cash_inout(date, basket):
    change = pd.read_excel(r'Z:\tca\data\{0}\每日出入金_{1}.xlsx'.format(date, date))
    change = change.groupby('产品名称')[['当日入金', '当日出金']].sum().reset_index()
    change = change.loc[change['产品名称'].isin(eval(basket))]
    change = change.sort_values(by='产品名称')
    change.reset_index(drop=True, inplace=True)

    return change


def enhanced_table(date1, date2, basket):
    equity_t = totalequity(date1, basket)
    equity_t_1 = totalequity(date2, basket)
    table = pd.DataFrame(equity_t['产品名称'])
    # adjust cash in & out
    try:
        change = cash_inout(date1, basket)
        change = pd.merge(table, change, on='产品名称', how='left').fillna(0)
        table['PnL'] = equity_t['股票总权益']/(equity_t_1['股票总权益']+change['当日入金']-change['当日出金']) - 1
    except:
        print('No cash in or out at %s' % date1)
        table['PnL'] = equity_t['股票总权益'] / equity_t_1['股票总权益'] - 1
        pass

    table['Benchmark'] = get_benchmark(*get_date('api'), basket)
    table['Excess_return'] = table['PnL'] - table['Benchmark']
    table = table.rename(columns={'产品名称': 'Product'})

    return table


def totalfuture(date):
    volume = pd.read_excel(r'Z:\tca\data\{0}\futures_{1}.xlsx'.format(date, date), sheet_name='volume', index_col=0)
    dynamic= volume.loc[:,'动态权益'].reset_index()
    volume = volume.iloc[:,:volume.columns.get_loc('动态权益')]
    info = pd.read_excel(r'Z:\tca\data\{0}\futures_{1}.xlsx'.format(date, date), sheet_name='info', index_col=0)
    info.columns = [re.findall('(.*?).CFE', n)[0] for n in info.columns]
    total_future = (info.loc['结算价'] - info.loc['收盘价']) * info.loc['合约乘数'] * volume
    total_future['差额'] = np.sum(total_future, axis=1)
    total_future = pd.merge(total_future, dynamic, on='期货', how='outer')
    total_future['期货权益'] = total_future['动态权益'] - total_future['差额']

    return total_future


def neutral_table(date1, date2, basket):
    equity_t_1 = totalequity(date2, basket).sort_values('产品名称').reset_index(drop=True)
    equity_t = totalequity(date1, basket).sort_values('产品名称').reset_index(drop=True)
    future_t_1 = totalfuture(date2).sort_values('期货').reset_index(drop=True)
    future_t = totalfuture(date1).sort_values('期货').reset_index(drop=True)
    table = pd.DataFrame(equity_t['产品名称'])
    # adjust cash in & out
    try:
        change = cash_inout(date1, basket)
        change = pd.merge(table, change, on='产品名称', how='left').fillna(0)
        table['Stock PnL'] = (equity_t['股票总权益'] - change['当日入金'] + change['当日出金']) / equity_t_1[
            '股票总权益'] - 1
        table['Future PnL'] = future_t['期货权益'] / future_t_1['期货权益'] - 1
        table['PnL'] = (equity_t['股票总权益'] + future_t['期货权益']) / (
                    equity_t_1['股票总权益'] + future_t_1['期货权益'] + change['当日入金'] - change['当日出金']) - 1

    except:
        print('No cash in or out at %s' % date1)
        table['Stock PnL'] = equity_t['股票总权益'] / equity_t_1['股票总权益'] - 1
        table['Future PnL'] = future_t['期货权益'] / future_t_1['期货权益'] - 1
        table['PnL'] = (equity_t['股票总权益'] + future_t['期货权益']) / (
                    equity_t_1['股票总权益'] + future_t_1['期货权益']) - 1

    table = table.rename(columns={'产品名称': 'Product'})

    return table


def get_date(source):
    if source == 'api':       # get data from Choice API
        date_t = date.today() - timedelta(lag_n)
        date_t = date_t.strftime('%Y-%m-%d')
        date_t_1 = rq.get_previous_trading_date(date_t,n=1).strftime('%Y-%m-%d')
    if source == 'file':
        date_t = date.today() - timedelta(lag_n)
        date_t = date_t.strftime('%Y-%m-%d')
        date_t_1 = rq.get_previous_trading_date(date_t,n=1).strftime('%Y%m%d')
        date_t = date_t.replace('-','')
    return date_t,date_t_1


def add_date(data):
    d = (date.today() - timedelta(lag_n)).strftime('%Y-%m-%d')
    data.insert(loc=0,column='Date',value=d, allow_duplicates=False)


def add_historical(data, basket):
    hist_data = pd.read_csv(r'Z:\tca\Historical_PnL_%s.csv' % basket, encoding='gbk')
    select_date = (date.today() - timedelta(lag_n)).strftime('%Y-%m-%d')
    hist_data = hist_data.drop(hist_data.loc[hist_data['Date'] == select_date].index)
    hist_data = pd.concat([hist_data, data])
    hist_data.to_csv(r'Z:\tca\Historical_PnL_%s.csv' % basket, index=False, encoding='gbk')

    return hist_data

#############################  500指增产品 #############################
print('当日500指增产品PnL')
data1 = enhanced_table(*get_date('file'),'csi500')
add_date(data1)
hist_data1 = add_historical(data1,'csi500')

#############################  1000指增产品 #############################
print('当日1000指增产品PnL')
data2 = enhanced_table(*get_date('file'),'csi1000')
add_date(data2)
hist_data2 = add_historical(data2,'csi1000')

############################# 中性产品 #############################
print('当日中性产品PnL')
data3= neutral_table(*get_date('file'),'neutral')
add_date(data3)
hist_data3 = add_historical(data3,'neutral')