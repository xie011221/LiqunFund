import pandas as pd
import numpy as np
from src.portfolio import Portfolio


class EqualPortfolio(Portfolio):

    def get_portfolio(self,date: pd.Timestamp,value: float) -> pd.DataFrame:
        shares = self.strategy.selection.get_selection(date)
        shares['weight'] = 1
        shares['weight'] = shares['weight'] / shares['weight'].sum()
        current_price = self.strategy.get_current_price(date)
        shares = pd.merge(shares, current_price, how='left', on='share_code')
        shares['share_value'] = shares['weight'] * value
        shares['quantity'] = shares['share_value'] / shares['price']
        shares['quantity'] = np.floor_divide(shares['quantity'], 100) * 100
        portfolio = shares[['share_code','quantity']].copy(deep=True)
        return portfolio

    def get_portfolio_name(self) -> str:
        return 'Equal'