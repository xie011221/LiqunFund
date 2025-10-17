from abc import ABC, abstractmethod

import pandas as pd

class Portfolio(ABC):

    def __init__(self) -> None:
        self.strategy = None

    def set_strategy(self,strategy) -> None:
        self.strategy = strategy

    @abstractmethod
    def get_portfolio(self, date: pd.Timestamp, value: float) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_portfolio_name(self) -> str:
        pass