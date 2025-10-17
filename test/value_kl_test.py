import pandas as pd

from src.basicstrategy import BasicStrategy
from src.klselection import KLSelection
from src.valueportfolio import ValuePortfolio


market = pd.read_parquet('../data/market.parquet')
hfq_market = pd.read_parquet('../data/hfq_market.parquet')
component_calendar = pd.read_parquet('../data/component_calendar.parquet')
basic = pd.read_parquet('../data/basic.parquet')


full_equal_strategy = BasicStrategy(ValuePortfolio(),KLSelection())
full_equal_strategy.read_market(market)
full_equal_strategy.read_hfq_market(hfq_market)
full_equal_strategy.read_component_calendar(component_calendar)
full_equal_strategy.read_basic(basic)


value = 300000000
date = pd.to_datetime('20230901')

portfolio = full_equal_strategy.portfolio.get_portfolio(date,value)
print(portfolio)


price = full_equal_strategy.get_current_price(date)
portfolio = pd.merge(portfolio, price, how='left', on='share_code')
portfolio['value'] = portfolio['quantity'] * portfolio['price']
print(portfolio['value'].sum())
print(portfolio['value'].sum()*1.00021)