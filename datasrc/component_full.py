import tushare as ts
import pandas as pd
import datetime

token = input("请输入token：")

ts.set_token(token)
pro = ts.pro_api(token)


'''
该接口每次最多返回6000条数据，故先生成每月终末日，再循环获取
'''

periodList = []

for month in range(8,12):
    period = []
    period.append(datetime.date(2015, month, 1).strftime('%Y%m%d'))
    period.append((datetime.date(2015, month+1, 1)-datetime.timedelta(days=1)).strftime('%Y%m%d'))
    periodList.append(period)

period = []
period.append(datetime.date(2015, 12, 1).strftime('%Y%m%d'))
period.append((datetime.date(2015, 12, 31)).strftime('%Y%m%d'))
periodList.append(period)

for year in range(2016,2025):
    for month in range(1,12):
        period = []
        period.append(datetime.date(year, month, 1).strftime('%Y%m%d'))
        period.append((datetime.date(year, month+1, 1)-datetime.timedelta(days=1)).strftime('%Y%m%d'))
        periodList.append(period)
    period = []
    period.append(datetime.date(year, 12, 1).strftime('%Y%m%d'))
    period.append(datetime.date(year, 12, 31).strftime('%Y%m%d'))
    periodList.append(period)

for month in range(1,8):
    period = []
    period.append(datetime.date(2025, month, 1).strftime('%Y%m%d'))
    period.append((datetime.date(2025, month+1, 1)-datetime.timedelta(days=1)).strftime('%Y%m%d'))
    periodList.append(period)

period = []
period.append(datetime.date(2025, 12, 1).strftime('%Y%m%d'))
period.append(datetime.date(2025, 12, 31).strftime('%Y%m%d'))
periodList.append(period)

componentDfList = []
for period in periodList:
    print(period)
    df = pro.index_weight(**{
        "index_code": "399300.SZ",
        "start_date": period[0],
        "end_date": period[1],
    })
    componentDfList.append(df)

component_df = pd.concat(componentDfList)


'''
下载包含权重的成分数据
'''

component_df.rename(columns={'con_code':'share_code'},inplace=True)
component_df["trade_date"] = pd.to_datetime(component_df["trade_date"])
#del component_df['weight']
del component_df['index_code']

component_df.to_parquet('../data/component_full.parquet', engine='pyarrow')