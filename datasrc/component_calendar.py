import pandas as pd

component = pd.read_parquet('../data/component_full.parquet')
market = pd.read_parquet('../data/market.parquet')

market_calendar = market['trade_date'].drop_duplicates()
del market

component_calendar = component['trade_date'].drop_duplicates().sort_values().reset_index(drop=True)

market_calendar = market_calendar.iloc[::-1].reset_index(drop = True)

last_date = component.iloc[0]['trade_date']

i = 1

current_component_df_list = []

for trade_date in market_calendar:
    last_calendar = component_calendar[component_calendar <= trade_date]
    last_date = last_calendar.iloc[-1]
    current_component_df = component[component['trade_date'] == last_date].copy(deep=True)
    current_component_df['trade_date'] = trade_date
    current_component_df_list.append(current_component_df)

component_df = pd.concat(current_component_df_list)
component_df = component_df.rename(columns={'weight': 'raw_weight'})
#component_df = component_df[['trade_date','share_code']]
#component_df.rename(columns={'con_code':'share_code'}, inplace=True)

component_df.to_parquet('../data/component_calendar.parquet',engine='pyarrow')