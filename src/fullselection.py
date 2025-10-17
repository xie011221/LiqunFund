import pandas as pd
from src.selection import Selection

class FullSelection(Selection):

    def get_selection(self,date: pd.Timestamp) -> pd.DataFrame:
        shares = self.strategy.get_current_component(date)
        return shares

    def get_selection_name(self) -> str:
        return 'Full'