import pandas as pd

raw_market = pd.read_parquet('../data/raw_market.parquet')

trade_date = raw_market['trade_date'].copy(deep=True)
trade_date.drop_duplicates(inplace=True)

share_code = raw_market['share_code'].copy(deep=True)
share_code.drop_duplicates(inplace=True)

all_combinations = pd.DataFrame(
    [(share, date) for share in share_code for date in trade_date],
    columns=["share_code", "trade_date"]
)

full_prices = all_combinations.merge(
    raw_market,
    on=["share_code", "trade_date"],
    how="left"
)

full_prices = full_prices.sort_values(["share_code", "trade_date"])
filled_prices = full_prices.groupby("share_code", group_keys=False).apply(
    lambda group: group.ffill() # 对每组进行前向填充
)

filled_prices = filled_prices[['share_code', 'trade_date', 'open']]
filled_prices.rename(columns={'open':'price'}, inplace=True)

filled_prices.to_parquet('../data/market.parquet')