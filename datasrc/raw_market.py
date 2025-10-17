import tushare as ts
import pandas as pd

token = input("请输入token：")

ts.set_token(token)
pro = ts.pro_api(token)

component = pd.read_parquet('../data/component.parquet')

share_list = component['share_code'].tolist()

market_df_list = []

for share in share_list:
    print(share)
    df = pro.daily(**{
        "ts_code": share,
        "start_date": '20150901',
        "end_date": '20200831'
    },
   fields=[
       "ts_code",
       "trade_date",
       "open",
       "high",
       "low",
       "close",
       "pre_close",
       "change",
       "pct_chg",
       "vol",
       "amount"
   ])
    market_df_list.append(df)

    df = pro.daily(**{
    "ts_code": share,
    "start_date": '20200901',
    "end_date": '20250831'
    },
    fields = [
        "ts_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount"
    ])
    market_df_list.append(df)

market_df = pd.concat(market_df_list)

market_df.rename(columns={'ts_code': 'share_code'}, inplace=True)
market_df['trade_date'] = pd.to_datetime(market_df['trade_date'])

market_df.to_parquet('../data/raw_market.parquet',engine='pyarrow')