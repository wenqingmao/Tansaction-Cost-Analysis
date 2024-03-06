import pandas as pd
from datetime import date
from datetime import datetime
from datetime import timedelta
import os
import re
import rqdatac as rq
from bokeh.io import curdoc,show
from bokeh.models import ColumnDataSource,DataTable,TableColumn,CDSView,Div,NumberFormatter,DateRangeSlider, \
    DatePicker,CustomJS,CustomJSFilter,HoverTool,Legend, Tabs, Panel
from bokeh.layouts import column, row
from bokeh.plotting import figure, curdoc
from bokeh.palettes import Spectral6
import warnings
warnings.filterwarnings('ignore')

# rqdatac API
rq.init('license', 'MAFPix3ir-DdV81aNkxATTdYOhryZfY0-ExgfKQ3WGLysJT1-yAVAJcTzeoqv_ciOpj8ikC4rALuQ_AIp2Ny_D4VtjjEFwTLu9Rb8sY1fn3eya9iUghS15S8itTdsYefHHGFwhFadAO-VgcfnUnq_B-QgZNT3U4AnK0eon93_2Y=ZsvQzvVU-5XMDSZUDtIb__NfgxXL3Rq1MKolsrUS7aTZKN-g4Ngb8OdFBRwT4NqciBY5Jha5RvJyFYBTFB8Qgs6AthmhVX3vATneRTp-dqChgcAkD_n8eU-xdAzszgzhL3uQs8LJZFhHn4zRsTqvHhpRf70mbq0DqOt3y-pJWaw=', ("rqdatad-pro.ricequant.com", 16011))

map_dict = {'SLW302':'fy1_nav.csv','SSH115':'mq3_nav.csv','SVY182':'fb1_nav.csv','SZA132':'zs2_nav.csv','SZH610':'cx1_nav.csv', \
           'SJF580':'dclzx1_nav.csv','SJT050':'fund_nav.csv','SLB914':'fund_nav_2.csv','SSH095':'zqjx2_nav.csv','SVK933':'fy2_nav.csv', \
           'SVM121':'fyjx2_nav.csv','SVP343':'zq2zx2_nav.csv','SSH118':'mq1_nav.csv','STY144':'fb2_nav.csv','STY145':'fb3_nav.csv', \
           'SXZ666':'zs1_nav.csv','SXZ663':'zh1000_nav.csv','SQS811':'mq2_nav.csv','SXV369':'fb5_nav.csv','SZN584':'lhjx_nav.csv'}

start_date = {"SLW302":"2021-01-21",
            "SQS811":"2021-08-27",
            "SSH118":"2022-01-24",
            "SSH115":"2021-10-01",
            "SLB914":"2020-07-09",
            "SXZ666":"2023-02-28",
            "SZA132":"2023-02-28",
            "SZN584":"2023-04-10"}

code_csi500 = ['SLW302','SSH115','SLB914','SSH118','SQS811','SZN584']
code_csi1000 = ['SXZ666','SZA132']
product_csi500 = ['扶摇1号','牧起3号','凌云增强2号','牧起1号','牧起2号','罗维量化金选1号']
product_csi1000 = ['之升1000增强1号','之升1000增强2号']

def get_historical_nav(basket):
    nav = pd.read_excel(r'Z:\tca\nav_data\historical_nav.xlsx')

    if basket == 'csi500':
        code_list = code_csi500
    if basket == 'csi1000':
        code_list = code_csi1000
    nav_type = nav.loc[nav['产品代码'].isin(code_list)]
    nav_type.reset_index(drop=True, inplace=True)

    index = 0
    for c in code_list:
        index_add = nav_type.loc[(nav_type['产品代码'] == c) & (nav_type['净值日期'] >= start_date[c])].index
        try:
            index = index.append(index_add)
        except:
            index = index_add
    nav_type = nav_type.loc[index].sort_values(['产品代码', '净值日期'])
    nav_type = nav_type.dropna()
    nav_type.reset_index(drop=True, inplace=True)

    nav_type['净值涨跌幅'] = 0
    for i in range(len(code_list)):
        nav_type.loc[nav_type['产品代码'] == code_list[i], '净值涨跌幅'] = \
        nav_type.loc[nav_type['产品代码'] == code_list[i]]['单位净值'].pct_change()

    nav_type.columns = ['code', 'product', 'date', 'nav', 'prod_ret']
    nav_type['product'].replace('罗维量化金选1号私募证券投资基金', '罗维盈安量化金选1号私募证券投资基金', inplace=True)
    nav_type['product'] = nav_type['product'].apply(lambda x: re.findall('罗维盈安(.*?)私募证券投资基金', x)[0])
    nav_type.fillna(value=0, inplace=True)
    return nav_type

def get_historical_benckmark(df, basket):
    if basket == 'csi500':
        code = '000905.XSHG'
    if basket == 'csi1000':
        code = '000852.XSHG'
    start_date = df['date'].min()
    end_date = df['date'].max()

    benchmark = rq.get_price(code, start_date=start_date, end_date=end_date, fields='close').droplevel(0)
    benchmark.reset_index(inplace=True)
    benchmark['mkt_ret'] = benchmark['close'].pct_change()
    benchmark.fillna(value=0, inplace=True)
    return benchmark

def excess_return(nav_type,benchmark_type):
    excess_ret = pd.merge(nav_type,benchmark_type,on='date',how='left')
    excess_ret['excess_ret'] = excess_ret['prod_ret'] - excess_ret['mkt_ret']
    excess_ret = excess_ret.dropna()
    return excess_ret

# test
def cum_excess_return(final_table):
    cum_excess_ret = final_table.copy()
    product_list = [*set(cum_excess_ret['Product'].tolist())]
    for i in range(len(product_list)):
        cum_excess_ret.loc[cum_excess_ret['Product']==product_list[i],'1+pret'] = cum_excess_ret.loc[cum_excess_ret['Product']==product_list[i]]['PnL']+1
        cum_excess_ret.loc[cum_excess_ret['Product']==product_list[i],'1+mret'] = cum_excess_ret.loc[cum_excess_ret['Product']==product_list[i]]['Benchmark']+1
        cum_excess_ret.loc[cum_excess_ret['Product']==product_list[i],'cum_pret'] = cum_excess_ret.loc[cum_excess_ret['Product']==product_list[i]]['1+pret'].cumprod()-1
        cum_excess_ret.loc[cum_excess_ret['Product']==product_list[i],'cum_mret'] = cum_excess_ret.loc[cum_excess_ret['Product']==product_list[i]]['1+mret'].cumprod()-1
    cum_excess_ret['cum_excess_ret'] = cum_excess_ret['cum_pret'] - cum_excess_ret['cum_mret']
#     excess_ret = excess_ret.dropna()
    return cum_excess_ret

def get_date_range(date_range_slider):
    # start = pd.to_datetime(date_range_slider.value[0])
    # end = pd.to_datetime(date_range_slider.value[1])
    # start = date_range_slider.value[0]
    # end = date_range_slider.value[1]
    start = datetime.fromtimestamp(int(str(date_range_slider.value[0])[:10])).strftime('%Y-%m-%d')
    end = datetime.fromtimestamp(int(str(date_range_slider.value[1])[:10])).strftime('%Y-%m-%d')
    return start, end

# original data scource
nav_csi500 = get_historical_nav('csi500')
nav_csi1000 = get_historical_nav('csi1000')
benchmark_csi500 = get_historical_benckmark(nav_csi500, 'csi500')
benchmark_csi1000 = get_historical_benckmark(nav_csi1000, 'csi1000')
excess_ret_csi500 = excess_return(nav_csi500,benchmark_csi500)
excess_ret_csi1000 = excess_return(nav_csi1000,benchmark_csi1000)

# set date to show
lag_n = 0  # today = 0
curr_date = date.today() - timedelta(lag_n)
last_friday = (curr_date - timedelta(7-4+curr_date.weekday())).strftime('%Y-%m-%d')

curr_date = curr_date.strftime('%Y-%m-%d')
pre_date = rq.get_previous_trading_date(curr_date,n=1).strftime('%Y-%m-%d')

#############################  500指增产品 #############################
print('500指增产品')
# prepare final table1
concat_data1 = excess_ret_csi500[['date','product','prod_ret','mkt_ret','excess_ret']]
concat_data1.columns = ['Date','Product','PnL','Benchmark','Excess_return']
hist_data1 = pd.read_csv(r'Z:\tca\Historical_PnL_csi500.csv',encoding='gbk',parse_dates=['Date'])
concat_data2 = hist_data1.loc[hist_data1['Date']==curr_date]
concat_data2['Product'].replace('扶摇1号','凌云扶摇1号',inplace=True)
concat_data2['Product'].replace('罗维量化金选1号', '量化金选1号', inplace=True)
final_table1 = pd.concat([concat_data1,concat_data2]).sort_values(by=['Product','Date'])
final_table1.reset_index(drop=True,inplace=True)
final_table1['Date'] = final_table1['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))     # table show in str

# table
source1 = ColumnDataSource(data=final_table1)
table_columns1 = [TableColumn(field='Date', title='Date'), \
                  TableColumn(field='Product', title='Product'), \
                  TableColumn(field='PnL', title='PnL',formatter=NumberFormatter(format="0.0000%")), \
                  TableColumn(field='Benchmark', title='Benchmark',formatter=NumberFormatter(format="0.0000%")), \
                  TableColumn(field='Excess_return', title='Excess Return',formatter=NumberFormatter(format="0.0000%"))]


cum_excess_ret_csi500 = cum_excess_return(final_table1)
df_csi500 = cum_excess_ret_csi500.groupby(['Date','Product'])['cum_excess_ret'].mean().unstack().reset_index()
df_csi500['date_to_display'] = df_csi500['Date']
df_csi500['Date'] = df_csi500['Date'].apply(lambda x:datetime.strptime(x,'%Y-%m-%d'))

numlines=len(df_csi500.columns[1:-1])
col = list(df_csi500.columns[1:-1])
ind = list(df_csi500.index)
hist_source1 = ColumnDataSource(df_csi500)
colors = Spectral6[:numlines]

legend_it = []
p1 = figure(width=1200, height=500, x_axis_type="datetime", title='500指增产品', title_location='above')
p1.xaxis.axis_label = 'Date Range'
p1.yaxis.axis_label = 'Cumulative Excess Return'
for (cl, color) in zip(col, colors):
    c = p1.line(x='Date', y=cl, line_width=2, source=hist_source1, color=color )
    legend_it.append((cl, [c]))
    p1.add_tools(HoverTool(
            renderers=[c],
            tooltips=[('Date','@date_to_display'),('Product', f'{cl}'),('Value','@{%s}'%cl+'{0.00%}')],
            formatters={'Date': 'datetime'}))

# print(source.data.keys())
legend = Legend(items=legend_it, location=(300, 0), spacing = 5)
p1.add_layout(legend, 'right')
p1.legend.label_text_font_size = '15px'
p1.legend.location='center'
p1.legend.click_policy="hide"

#############################  1000指增产品 #############################
print('1000指增产品')
# prepare final table2
concat_data1 = excess_ret_csi1000[['date','product','prod_ret','mkt_ret','excess_ret']]
concat_data1.columns = ['Date','Product','PnL','Benchmark','Excess_return']
hist_data2 = pd.read_csv(r'Z:\tca\Historical_PnL_csi1000.csv',encoding='gbk',parse_dates=['Date'])
concat_data2 = hist_data2.loc[hist_data2['Date']==curr_date]
concat_data2['Product'].replace('安之升100增强2号','之升1000增强2号',inplace=True)
final_table2 = pd.concat([concat_data1,concat_data2]).sort_values(by=['Product','Date'])
final_table2.reset_index(drop=True,inplace=True)
final_table2['Date'] = final_table2['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))

# table
source2 = ColumnDataSource(data=final_table2)
table_columns2 = [TableColumn(field='Date', title='Date'),\
                  TableColumn(field='Product', title='Product'),\
                  TableColumn(field='PnL', title='PnL',formatter=NumberFormatter(format="0.0000%")),\
                  TableColumn(field='Benchmark', title='Benchmark',formatter=NumberFormatter(format="0.0000%")),\
                  TableColumn(field='Excess_return', title='Excess Return',formatter=NumberFormatter(format="0.0000%"))]


cum_excess_ret_csi1000 = cum_excess_return(final_table2)
df_csi1000 = cum_excess_ret_csi1000.groupby(['Date','Product'])['cum_excess_ret'].mean().unstack().reset_index()
df_csi1000['date_to_display'] = df_csi1000['Date']
df_csi1000['Date'] = df_csi1000['Date'].apply(lambda x:datetime.strptime(x,'%Y-%m-%d'))

numlines=len(df_csi1000.columns[1:-1])
col = list(df_csi1000.columns[1:-1])
ind = list(df_csi1000.index)
hist_source2 = ColumnDataSource(df_csi1000)
colors = Spectral6[:numlines]

# print(source.data.keys())
legend_it2 = []
p2 = figure(width=1200, height=500, x_axis_type="datetime", title='1000指增产品', title_location='above')
p2.xaxis.axis_label = 'Date Range'
p2.yaxis.axis_label = 'Cumulative Excess Return'
for (cl, color) in zip(col, colors):
    c = p2.line(x='Date', y=cl, line_width=2, source=hist_source2, color=color )
    legend_it2.append((cl, [c]))
    p2.add_tools(HoverTool(
            renderers=[c],
            tooltips=[('Date','@date_to_display'),('Product', f'{cl}'),('Value','@{%s}'%cl+'{0.00%}')]))

# print(source.data.keys())
legend = Legend(items=legend_it2, location=(300, 0), spacing = 5)
p2.add_layout(legend, 'right')
p2.legend.label_text_font_size = '15px'
p2.legend.location='center'
p2.legend.click_policy="hide"

############################# Compare with previous prediction #############################
compare1 = hist_data1.loc[hist_data1['Date']==pre_date]
compare1['Date'] = compare1['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
compare_source1 = ColumnDataSource(data=compare1)
compare_table_columns1 = [TableColumn(field='Date', title='Date'), \
                  TableColumn(field='Product', title='Product'), \
                  TableColumn(field='PnL', title='PnL',formatter=NumberFormatter(format="0.0000%")), \
                  TableColumn(field='Benchmark', title='Benchmark',formatter=NumberFormatter(format="0.0000%")), \
                  TableColumn(field='Excess_return', title='Excess Return',formatter=NumberFormatter(format="0.0000%"))]
compare_tab1 = DataTable(source=compare_source1,columns=compare_table_columns1, width=700, height=200)

compare2 = hist_data2.loc[hist_data2['Date']==pre_date]
compare2['Date'] = compare2['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
compare_source2 = ColumnDataSource(data=compare2)
compare_table_columns2 = [TableColumn(field='Date', title='Date'), \
                  TableColumn(field='Product', title='Product'), \
                  TableColumn(field='PnL', title='PnL',formatter=NumberFormatter(format="0.0000%")), \
                  TableColumn(field='Benchmark', title='Benchmark',formatter=NumberFormatter(format="0.0000%")), \
                  TableColumn(field='Excess_return', title='Excess Return',formatter=NumberFormatter(format="0.0000%"))]
compare_tab2 = DataTable(source=compare_source2,columns=compare_table_columns2, width=700, height=200)

############################# week cummulative excess return #############################
week_excess_return1 = final_table1.loc[(final_table1['Date']>last_friday)&(final_table1['Date']<=curr_date)]
week_excess_return1.reset_index(drop=True, inplace=True)
cum_excess_return1 = cum_excess_return(week_excess_return1)
week_table1 = cum_excess_return1.loc[cum_excess_return1['Date']==curr_date][['Date','Product','cum_excess_ret']]
week_source1 = ColumnDataSource(data=week_table1)
week_table_columns1 = [TableColumn(field='Date', title='Date'), \
                  TableColumn(field='Product', title='Product'), \
                  TableColumn(field='cum_excess_ret', title='Cum Excess Return',formatter=NumberFormatter(format="0.0000%"))]
week_tab1 = DataTable(source=week_source1,columns=week_table_columns1, width=500, height=200)

week_excess_return2 = final_table2.loc[(final_table2['Date']>last_friday)&(final_table1['Date']<=curr_date)]
week_excess_return2.reset_index(drop=True, inplace=True)
cum_excess_return2 = cum_excess_return(week_excess_return2)
week_table2 = cum_excess_return2.loc[cum_excess_return2['Date']==curr_date][['Date','Product','cum_excess_ret']]
week_source2 = ColumnDataSource(data=week_table2)
week_table_columns2 = [TableColumn(field='Date', title='Date'), \
                  TableColumn(field='Product', title='Product'), \
                  TableColumn(field='cum_excess_ret', title='Cum Excess Return',formatter=NumberFormatter(format="0.0000%"))]
week_tab2 = DataTable(source=week_source2,columns=week_table_columns2, width=500, height=200)

# Widgets
date_range_slider1 = DateRangeSlider(title="Date Range ", start=excess_ret_csi500['date'].min(), end=excess_ret_csi500['date'].max(), \
                                                         value=(excess_ret_csi500['date'].min(),excess_ret_csi500['date'].max()), step=1)
date_range_slider2 = DateRangeSlider(title="Date Range ", start=excess_ret_csi1000['date'].min(), end=excess_ret_csi1000['date'].max(), \
                                                         value=(excess_ret_csi1000['date'].min(),excess_ret_csi1000['date'].max()), step=1)
date_picker = DatePicker(title='Select date', value=date.today(), min_date='2023-01-01', max_date=date.today())
date_filter = CustomJSFilter(args=dict(date_picker=date_picker), code='''
const indices = [];
// iterate through rows of data source and see if each satisfies some constraint
for (let i = 0; i < source.get_length(); i++){
    if (source.data['Date'][i] == date_picker.value){
        indices.push(true);
    } else {
        indices.push(false);
    }
}
return indices;
''')

# JS Callbacks
callback1 = CustomJS(args=dict(date_picker=date_picker,source=source1), code="""
    console.log('date_picker: value=' + this.value, this.toString())
    source.change.emit();
""")
callback2 = CustomJS(args=dict(date_picker=date_picker,source=source2), code="""
    console.log('date_picker: value=' + this.value, this.toString())
    source.change.emit();
""")

# Python Callbacks
def range_change1(attrname, old, new):
    date_range_slider1.value = new
    update1()

def update1():
    rng = get_date_range(date_range_slider1)
    new_df = final_table1.loc[(final_table1['Date']>=rng[0])&(final_table1['Date']<=rng[1])]
    new_df_csi500 = cum_excess_return(new_df)
    new_df_csi500 = new_df_csi500.groupby(['Date', 'Product'])['cum_excess_ret'].mean().unstack().reset_index()
    new_df_csi500['date_to_display'] = new_df_csi500['Date']
    new_df_csi500['Date'] = new_df_csi500['Date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    hist_source1.data = new_df_csi500

def range_change2(attrname, old, new):
    date_range_slider2.value = new
    update2()

def update2():
    rng = get_date_range(date_range_slider2)
    new_df = final_table2.loc[(final_table2['Date']>=rng[0])&(final_table2['Date']<=rng[1])]
    new_df_csi1000 = cum_excess_return(new_df)
    new_df_csi1000 = new_df_csi1000.groupby(['Date', 'Product'])['cum_excess_ret'].mean().unstack().reset_index()
    new_df_csi1000['date_to_display'] = new_df_csi1000['Date']
    new_df_csi1000['Date'] = new_df_csi1000['Date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    hist_source2.data = new_df_csi1000

# Events
date_picker.js_on_change("value",callback1)
date_picker.js_on_change("value",callback2)

# date_range_slider1.js_link('value', p1.x_range, 'start', attr_selector=0)
# date_range_slider1.js_link('value', p1.x_range, 'end', attr_selector=1)
# date_range_slider2.js_link('value', p2.x_range, 'start', attr_selector=0)
# date_range_slider2.js_link('value', p2.x_range, 'end', attr_selector=1)

date_range_slider1.on_change('value',range_change1)
date_range_slider2.on_change('value',range_change2)

# Datatable
view1 = CDSView(source=source1,filters=[date_filter])
table1 = DataTable(source=source1, view=view1,columns=table_columns1, width=700, height=200)
view2 = CDSView(source=source2,filters=[date_filter])
table2 = DataTable(source=source2, view=view2, columns=table_columns2, width=700, height=100)

# HTML
div_title = Div(text="""
<h1>指增产品Dashboard</h1>
""")
div_csi500 = Div(text="""
<p>"500指增产品每日PnL"
</p>
""")
div_csi1000 = Div(text="""
<p>"1000指增产品每日PnL"
</p>
""")
compare_div_csi500 = Div(text="""
<p>"昨日对比"
</p>
""")
compare_div_csi1000 = Div(text="""
<p>"昨日对比"
</p>
""")
week_div = Div(text="""
<p>"本周累计超额收益"
</p>
""")
div_csi500_hist = Div(text="""
<p>"500指增产品历史累计超额收益"
</p>
""")
div_csi1000_hist = Div(text="""
<p>"1000指增产品历史累计超额收益"
</p>
""")

# layout
# layout = column(div_title, date_picker, week_div,row(week_tab1, week_tab2), row(column(div_csi500,tab1), column(compare_div_csi500,compare_tab1)), \
#                 row(column(div_csi1000,tab2), column(compare_div_csi1000,compare_tab2)),column(div_csi500_hist,date_range_slider1,p1),column(div_csi1000_hist,date_range_slider2,p2))
tab1_layout = column(div_title, date_picker, row(column(div_csi500,table1), column(compare_div_csi500,compare_tab1)), \
                row(column(div_csi1000,table2), column(compare_div_csi1000,compare_tab2)))
tab2_layout = column(div_title, week_div,row(week_tab1, week_tab2))

tab1 = Panel(child=tab1_layout, title='PnL')
tab2 = Panel(child=tab2_layout, title='cum PnL')

tabs = Tabs(tabs=[tab1, tab2], height=800)

# show(layout)
curdoc().add_root(tabs)
