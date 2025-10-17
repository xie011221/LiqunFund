import tushare as ts
import pandas as pd

# 设置pandas不要出现 FutureWarning: Down casting
pd.set_option('future.no_silent_downcasting', True)

token = input("请输入token：")

ts.set_token(token)
pro = ts.pro_api(token)

component = pd.read_parquet('../data/component.parquet')

share_list = component['share_code'].tolist()

hfq_market_df_list = []

for share in share_list:
    print(share)
    df = ts.pro_bar(ts_code=share, adj='hfq', start_date='20150901', end_date='20200831')
    hfq_market_df_list.append(df)
    df = ts.pro_bar(ts_code=share, adj='hfq', start_date='20200901', end_date='20250831')
    hfq_market_df_list.append(df)

hfq_market_df = pd.concat(hfq_market_df_list)

hfq_market_df.rename(columns={'ts_code': 'share_code'}, inplace=True)
hfq_market_df['trade_date'] = pd.to_datetime(hfq_market_df['trade_date'])

hfq_market_df.to_parquet('../data/hfq_market.parquet',engine='pyarrow')