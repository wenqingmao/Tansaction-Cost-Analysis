import pandas as pd
import numpy as np
import glob
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')


# dicts and lists
map_dict = {'SLW302':'fy1_nav.csv','SSH115':'mq3_nav.csv','SVY182':'fb1_nav.csv','SZA132':'zs2_nav.csv','SZH610':'cx1_nav.csv', \
           'SJF580':'dclzx1_nav.csv','SJT050':'fund_nav.csv','SLB914':'fund_nav_2.csv','SSH095':'zqjx2_nav.csv','SVK933':'fy2_nav.csv', \
           'SVM121':'fyjx2_nav.csv','SVP343':'zq2zx2_nav.csv','SSH118':'mq1_nav.csv','STY144':'fb2_nav.csv','STY145':'fb3_nav.csv', \
           'SXZ666':'zs1_nav.csv','SXZ663':'zh1000_nav.csv','SQS811':'mq2_nav.csv','SXV369':'fb5_nav.csv','SZN584':'lhjx_nav.csv'}
code_csi500 = ['SLW302','SSH115','SLB914','SSH118','SQS811','SZN584']
code_csi1000 = ['SXZ666','SZA132']
code_neutral = ['SVY182','STY144','STY145','SXV369','SVK933','SXZ663']
tg_list = ['国君','国信','华泰','长城','招商','中金']

# Process historical net values every day

# 处理国君历史净值数据
filename = os.listdir(r'Z:\tca\nav_data\国君')[0]
nav_gj = pd.read_excel(r'Z:\tca\nav_data\国君\%s'%filename,skiprows=1)
nav_gj = nav_gj[['产品代码','产品名称','估值日期','单位净值']]
nav_gj.columns = ['产品代码','产品名称','净值日期','单位净值']
nav_gj['净值日期'] = pd.to_datetime(nav_gj['净值日期'])

# 处理国信历史净值数据
filename = os.listdir(r'Z:\tca\nav_data\国信')[0]
nav_gx = pd.read_excel(r'Z:\tca\nav_data\国信\%s'%filename,skiprows=1)
nav_gx = nav_gx[['产品代码','产品名称','净值日期','单位净值']]
nav_gx['净值日期'] = pd.to_datetime(nav_gx['净值日期'].astype('str'))

# 处理华泰历史净值数据
filename = os.listdir(r'Z:\tca\nav_data\华泰')[0]
nav_ht = pd.read_excel(r'Z:\tca\nav_data\华泰\%s'%filename)
for i in range(len(nav_ht)):
    nav_ht.loc[i,'last_letter'] = nav_ht.loc[i,'产品名称'][-1]
nav_ht = nav_ht.drop(index=nav_ht.loc[nav_ht['last_letter']=='类'].index)
nav_ht.reset_index(drop=True,inplace=True)
nav_ht = nav_ht[['产品编码','产品名称','净值日期','单位净值']]
nav_ht.columns = ['产品代码','产品名称','净值日期','单位净值']
nav_ht['净值日期'] = pd.to_datetime(nav_ht['净值日期'])

# 处理长城历史净值数据
filename = os.listdir(r'Z:\tca\nav_data\长城')[0]
nav_cc = pd.read_excel(r'Z:\tca\nav_data\长城\%s'%filename,skiprows=3)
nav_cc = nav_cc[['产品代码','产品名称','净值日期','单位净值']]
nav_cc['净值日期'] = pd.to_datetime(nav_cc['净值日期'])

# 处理招商历史净值数据
filename = os.listdir(r'Z:\tca\nav_data\招商')[0]
nav_zs = pd.read_excel(r'Z:\tca\nav_data\招商\%s'%filename)
nav_zs = nav_zs[['产品代码','产品名称','净值日期','单位净值']]
nav_zs['净值日期'] = pd.to_datetime(nav_zs['净值日期'])

# 处理中金历史净值数据
filename = os.listdir(r'Z:\tca\nav_data\中金')[0]
nav_zj = pd.read_excel(r'Z:\tca\nav_data\中金\%s'%filename)
nav_zj = nav_zj[[ '  产品代码  ','  产品名称  ','  净值日期  ','  单位净值  ']]
nav_zj.columns = ['产品代码','产品名称','净值日期','单位净值']
nav_zj['净值日期'] = pd.to_datetime(nav_zj['净值日期'])

nav = pd.concat([nav_gj,nav_gx,nav_ht,nav_cc,nav_zs,nav_zj])
nav = nav.sort_values(['产品代码','净值日期'])
nav.reset_index(drop=True,inplace=True)

code_list = [*set(nav['产品代码'].tolist())]
code_list.remove('SVL945')

# save nav of each product as csv
for i in range(len(map_dict)):
    temp = nav.loc[nav['产品代码']==code_list[i]].sort_values('净值日期')
    temp = temp[['净值日期','单位净值']]
    temp.columns = ['date','nav']
    temp.to_csv(r'Z:\tca\historical_nav\%s'%map_dict[code_list[i]],index=False)

nav.to_excel(r'Z:\tca\nav_data\historical_nav.xlsx',index=False)