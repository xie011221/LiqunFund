import time
import tushare as ts
import pandas as pd

token = input("请输入token：")

ts.set_token(token)
pro = ts.pro_api(token)

df = pro.stock_basic(**{
    "ts_code": "",
    "name": "",
    "exchange": "",
    "market": "",
    "is_hs": "",
    "list_status": "",
    "limit": "",
    "offset": ""
}, fields=[
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "cnspell",
    "market",
    "list_date",
    "act_name",
    "act_ent_type",
    "fullname"
])

#df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
df.rename(columns={'ts_code': 'share_code'}, inplace=True)

df.to_parquet('../data/stock_list.parquet',engine='pyarrow')