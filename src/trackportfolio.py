import pandas as pd
import numpy as np
from src.portfolio import Portfolio


class TrackPortfolio(Portfolio):

    def get_portfolio(self,date: pd.Timestamp,value: float) -> pd.DataFrame:
        shares = self.strategy.component_calendar[self.strategy.component_calendar['trade_date'] == date].copy(deep=True)
        current_price = self.strategy.get_current_price(date)
        shares['weight'] = shares['raw_weight'] / shares['raw_weight'].sum()
        shares = pd.merge(shares, current_price, how='left', on='share_code')
        shares['share_value'] = shares['weight'] * value
        shares['quantity'] = shares['share_value'] / shares['price']
        shares['quantity'] = np.floor_divide(shares['quantity'], 100) * 100
        portfolio = shares[['share_code','quantity']].copy(deep=True)
        return portfolio