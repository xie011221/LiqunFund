import pandas as pd
import tushare as ts
import time

token = input("请输入token：")

ts.set_token(token)
pro = ts.pro_api(token)

dividend_df_list = []
component = pd.read_parquet('../data/component.parquet')
share_list = component['share_code'].tolist()

for share in share_list:
    time.sleep(0.5)
    print(share)
    df = pro.dividend(**{
        "ts_code": share,
        "ann_date": "",
        "end_date": "",
        "record_date": "",
        "ex_date": "",
        "imp_ann_date": "",
        "limit": "",
        "offset": ""
    }, fields=[
        "ts_code",
        "end_date",
        "ann_date",
        "div_proc",
        "stk_div",
        "stk_bo_rate",
        "stk_co_rate",
        "cash_div",
        "cash_div_tax",
        "record_date",
        "ex_date",
        "pay_date",
        "div_listdate",
        "imp_ann_date"
    ])
    dividend_df_list.append(df)

dividend_df = pd.concat(dividend_df_list)


all_dividend = dividend_df

all_dividend.rename(columns={'ts_code':'share_code'}, inplace=True)

full_dividend = all_dividend.query("div_proc == '实施'")[['share_code','stk_div','cash_div','ex_date']]

full_dividend['ex_date'] = pd.to_datetime(full_dividend['ex_date'])

full_dividend.to_parquet('../data/dividend.parquet',engine='pyarrow')