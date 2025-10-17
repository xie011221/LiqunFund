import pandas as pd
import numpy as np
from src.portfolio import Portfolio


class ValuePortfolio(Portfolio):

    def get_portfolio(self,date: pd.Timestamp,value: float) -> pd.DataFrame:
        shares = self.strategy.selection.get_selection(date)
        current_basic  = self.strategy.get_current_basic(date)
        current_price = self.strategy.get_current_price(date)
        shares = pd.merge(shares, current_basic, how='left', on='share_code')
        shares['weight'] = shares['circ_mv'] / shares['circ_mv'].sum()
        shares['weight'] = shares['weight'] / shares['weight'].sum()
        shares = pd.merge(shares, current_price, how='left', on='share_code')
        shares['share_value'] = shares['weight'] * value
        shares['quantity'] = shares['share_value'] / shares['price']
        shares['quantity'] = np.floor_divide(shares['quantity'], 100) * 100
        portfolio = shares[['share_code','quantity']].copy(deep=True)
        return portfolio

    def get_portfolio_name(self) -> str:
        return 'Value'