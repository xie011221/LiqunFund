import pandas as pd
import numpy as np
from src.portfolio import Portfolio


class ReturnPortfolio(Portfolio):

    def get_portfolio(self, date: pd.Timestamp, value: float) -> pd.DataFrame:
        shares = self.strategy.selection.get_selection(date)
        year_hfq_market = self.strategy.get_year_hfq_market(date)
        current_price = self.strategy.get_current_price(date)
        average_log_returns = year_hfq_market.groupby('share_code')['log_return'].mean().reset_index()
        shares = pd.merge(shares, average_log_returns, how='left', on='share_code')
        shares.loc[shares['log_return'] < 0, 'log_return'] = 0
        shares['weight'] = shares['log_return'] / shares['log_return'].sum()
        shares = pd.merge(shares, current_price, how='left', on='share_code')
        shares['value'] = shares['weight'] * value
        shares['quantity'] = shares['value'] / shares['price']
        shares['quantity'] = np.floor_divide(shares['quantity'], 100) * 100
        portfolio = shares[['share_code','quantity']].copy(deep=True)
        portfolio = portfolio[portfolio['quantity'] != 0]
        return portfolio

    def get_portfolio_name(self) -> str:
        return 'Return'
