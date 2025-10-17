import pandas as pd
import numpy as np

class BasicStrategy(object):

    def __init__(self,portfolio,selection):
        # 设置组合优化和选股模型
        self.portfolio = portfolio
        self.selection = selection

        # 设置模型引用
        self.portfolio.set_strategy(self)
        self.selection.set_strategy(self)

        # 设置数据
        self.market = pd.DataFrame()
        self.hfq_market = pd.DataFrame()
        self.basic = pd.DataFrame()
        self.component_calendar = pd.DataFrame()
        self.suspend = pd.DataFrame()

    def read_market(self,market) -> None:
        self.market = market.copy(deep=True)

    def read_hfq_market(self,hfq_market) -> None:
        '''
        读入之后立即计算对数收益率，减少函数调用开销
        '''
        self.hfq_market = hfq_market.copy(deep=True)
        self.hfq_market = self.hfq_market.sort_values(['share_code', 'trade_date']).reset_index(drop=True)
        self.hfq_market['log_return'] = self.hfq_market.groupby('share_code')['open'].apply(
            lambda x: np.log(x / x.shift(1))).reset_index(drop=True)


    def read_basic(self,basic) -> None:
        self.basic = basic.copy(deep=True)

    def read_component_calendar(self,component_calendar) -> None:
        self.component_calendar = component_calendar.copy(deep=True)

    def read_suspend(self,suspend) -> None:
        self.suspend = suspend.copy(deep=True)

    def get_year_market(self,end_date: pd.Timestamp) -> pd.DataFrame:
        start_date = end_date - pd.DateOffset(years=1)
        year_market = self.market[[self.market['trade_date'].between(start_date,end_date)]]
        return year_market.copy(deep=True)

    def get_year_hfq_market(self,end_date: pd.Timestamp) -> pd.DataFrame:
        start_date = end_date - pd.DateOffset(years=1)
        year_hfq_market = self.hfq_market[(self.hfq_market['trade_date'] >= start_date)
                                          & (self.hfq_market['trade_date'] < end_date)]
        return year_hfq_market.copy(deep=True)

    def get_current_component(self,trade_date: pd.Timestamp) -> pd.DataFrame:
        current_component = self.component_calendar[self.component_calendar['trade_date'] < trade_date]
        last_trade_date = current_component['trade_date'].max()
        current_component = current_component[current_component['trade_date'] == last_trade_date]
        current_suspend = self.get_current_suspend(trade_date)
        current_component  = current_component[~current_component['share_code'].isin(current_suspend['share_code'])]
        return current_component['share_code'].to_frame('share_code')

    def get_current_price(self,trade_date: pd.Timestamp) -> pd.DataFrame:
        current_price = self.market[self.market['trade_date'] == trade_date]
        return current_price.copy(deep=True)

    def get_current_basic(self,trade_date: pd.Timestamp) -> pd.DataFrame:
        current_basic = self.basic[self.basic['trade_date'] < trade_date]
        last_trade_date = current_basic['trade_date'].max()
        current_basic = current_basic[current_basic['trade_date'] == last_trade_date]
        return current_basic.copy(deep=True)

    def get_current_suspend(self,trade_date: pd.Timestamp) -> pd.DataFrame:
        current_suspend = self.suspend[self.suspend['trade_date'] == trade_date]
        current_suspend = current_suspend['share_code'].to_frame('share_code')
        return current_suspend

    def get_strategy_name(self) -> str:
        portfolio_name = self.portfolio.get_portfolio_name()
        selection_name = self.selection.get_selection_name()
        return f"{portfolio_name} {selection_name}"


