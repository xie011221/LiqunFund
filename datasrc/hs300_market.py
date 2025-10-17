import tushare as ts
import pandas as pd

token = input("请输入token：")

ts.set_token(token)
pro = ts.pro_api(token)

# 拉取数据

df_list = []

df = pro.index_daily(**{
    "ts_code": "399300.SZ",
    "trade_date": "",
    "start_date": '20150901',
    "end_date": '20250831',
    "limit": "",
    "offset": ""
}, fields=[
    "ts_code",
    "trade_date",
    "close",
    "open",
    "high",
    "low",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount"
])

df.rename(columns={'ts_code': 'share_code'},inplace=True)
df['trade_date'] = pd.to_datetime(df['trade_date'])

df.to_parquet('../data/hs300_market.parquet',engine='pyarrow')