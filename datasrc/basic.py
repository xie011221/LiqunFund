import time
import tushare as ts
import pandas as pd

token = input("请输入token：")

ts.set_token(token)
pro = ts.pro_api(token)

component = pd.read_parquet('../data/component.parquet')
share_list = component['share_code'].tolist()

basic_df_list = []

for share in share_list:
    print(share)
    time.sleep(0.5)
    df = pro.daily_basic(**{
        "ts_code": share,
        "trade_date": "",
        "start_date": '20150901',
        "end_date": '20200831',
        "limit": "",
        "offset": ""
    },fields=[
        "ts_code",
        "trade_date",
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
        "limit_status"
    ])
    basic_df_list.append(df)
    time.sleep(0.5)
    df = pro.daily_basic(**{
        "ts_code": share,
        "trade_date": "",
        "start_date": '20200901',
        "end_date": '20250831',
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "trade_date",
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
        "limit_status"
    ])
    basic_df_list.append(df)

basic_df = pd.concat(basic_df_list)

basic_df.rename(columns={'ts_code': 'share_code'}, inplace=True)
basic_df['trade_date'] = pd.to_datetime(basic_df['trade_date'])

basic_df.to_parquet('../data/basic.parquet')