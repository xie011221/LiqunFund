import pandas as pd
import numpy as np
from src.portfolio import Portfolio


class TestPortfolio(Portfolio):

    def get_portfolio(self,date: pd.Timestamp,value: float) -> pd.DataFrame:
        share_list = ['600515.SH']
        quantity = [10000]
        portfolio = pd.DataFrame({'share_code':share_list,'quantity':quantity})
        return portfolio