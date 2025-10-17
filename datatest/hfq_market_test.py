import pandas as pd
import numpy as np

hfq_market = pd.read_parquet('../data/hfq_market.parquet')

hfq_market = hfq_market.sort_values(['share_code', 'trade_date']).reset_index(drop=True)
hfq_market['log_return'] = hfq_market.groupby('share_code')['open'].apply(
            lambda x: np.log(x / x.shift(1))).reset_index(drop=True)

component = pd.read_parquet('../data/component.parquet')


component_list = component['share_code'].tolist()

cnt = 0
num_of_shares = len(component_list)

for share in component_list:
    share_df = hfq_market[hfq_market['share_code'] == share]
    n = share_df.shape[0]
    if n < 2200:
        cnt += 1
        print(f'{cnt}/{num_of_shares}. {share} : {n}')




