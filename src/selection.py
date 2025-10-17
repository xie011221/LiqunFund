from abc import ABC, abstractmethod

import pandas as pd


class Selection(ABC):

    def __init__(self) -> None:
        self.strategy = None

    def set_strategy(self,strategy):
        self.strategy = strategy

    @abstractmethod
    def get_selection(self,date: pd.Timestamp) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_selection_name(self) -> str:
        pass

