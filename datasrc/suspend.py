import tushare as ts
import pandas as pd

token = input("请输入token：")

ts.set_token(token)
pro = ts.pro_api(token)

component = pd.read_parquet('../data/component.parquet')

share_list = component['share_code'].tolist()

suspend_df_list = []

for share in share_list:
    print(share)
    df = pro.suspend_d(**{
        "ts_code": share,
        "suspend_type": "S",
        "trade_date": "",
        "start_date": "20150901",
        "end_date": "20200831",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "trade_date"
    ])
    suspend_df_list.append(df)

    df = pro.suspend_d(**{
        "ts_code": share,
        "suspend_type": "S",
        "trade_date": "",
        "start_date": "20200901",
        "end_date": "20250831",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "trade_date"
    ])
    suspend_df_list.append(df)

suspend_df = pd.concat(suspend_df_list)

suspend_df.rename(columns={'ts_code': 'share_code'}, inplace=True)
suspend_df['trade_date'] = pd.to_datetime(suspend_df['trade_date'])
suspend_df = suspend_df[['share_code', 'trade_date']]

suspend_df.to_parquet('../data/suspend.parquet',engine='pyarrow')