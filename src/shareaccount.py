import numpy as np
import pandas as pd

class ShareAccount():

    def __init__(self):
        self.cash_balance = 0
        self.equity_holding = pd.DataFrame({'share_code':[],'holding_quantity':[]})
        self.current_date = pd.to_datetime('2025-09-01')
        self.fake_date = 0
        self.market = pd.DataFrame()
        self.market_calendar = pd.DataFrame()
        self.dividend_calendar = pd.DataFrame()
        self.commission_rate = 0.0002
        self.tax_rate = 0.0005
        self.transfer_fee = 0.00001
        self.cumulative_cost = 0
        self.div = 0

    def read_market(self, market):
        self.market = market.copy(deep=True)
        self.market_calendar = self.market['trade_date'].copy(deep=True)
        self.market_calendar = self.market_calendar.drop_duplicates().sort_values().reset_index(drop=True)
        self.current_date = self.market_calendar[self.fake_date]

    def read_dividend(self, dividend):
        self.dividend_calendar = dividend.copy(deep=True)

    def update_holding(self, shares):
        new_holding = pd.merge(self.equity_holding, shares, how = 'outer', on = 'share_code').fillna(0)
        new_holding['delta'] = new_holding['holding_quantity'] - new_holding['quantity']
        current_price = self.market.query("trade_date == @self.current_date")
        new_holding = pd.merge(new_holding, current_price, how = 'left', on = 'share_code')
        new_holding['delta_price'] = new_holding['delta'] * new_holding['price']
        buy_fee_rate = self.transfer_fee + self.commission_rate
        sell_fee_rate = self.transfer_fee + self.commission_rate + self.tax_rate
        #负数是买入，正数是卖出
        new_holding['fee'] = np.where(new_holding['delta_price'] < 0,
                                      buy_fee_rate * new_holding['delta_price'],
                                      -sell_fee_rate * new_holding['delta_price'])
        delta_cash = new_holding['delta_price'].sum()
        total_fee = new_holding['fee'].sum() #此处算出的费用必为负数，以负数表示扣费
        self.cash_balance += delta_cash
        self.cash_balance += total_fee
        self.cumulative_cost -= total_fee
        self.equity_holding = shares.copy(deep = True)
        self.equity_holding.rename(columns = {'quantity':'holding_quantity'}, inplace = True)

    def update_dividend(self):
        current_dividend = self.dividend_calendar.query("ex_date == @self.current_date")
        if current_dividend.empty:
            pass
        else:
            self.equity_holding = pd.merge(self.equity_holding, current_dividend, how = 'left', on = 'share_code').fillna(0)
            self.equity_holding['profit'] = self.equity_holding['cash_div'] * self.equity_holding['holding_quantity']
            self.equity_holding['holding_quantity'] = self.equity_holding['holding_quantity'] * (self.equity_holding['stk_div'] + 1)
            self.cash_balance += self.equity_holding['profit'].sum()
            self.div += self.equity_holding['profit'].sum()
            self.equity_holding = self.equity_holding[['share_code','holding_quantity']].copy()

    def next_day(self):
        self.fake_date += 1
        self.current_date = self.market_calendar[self.fake_date]

    def get_net_value(self) -> float:
        current_price = self.market.query("trade_date == @self.current_date")
        current_holding = pd.merge(self.equity_holding,current_price, how = 'left', on = 'share_code')
        current_holding['value'] = current_holding['holding_quantity'] * current_holding['price']
        return current_holding['value'].sum() + self.cash_balance

    def set_date(self, date):
        target_date = pd.to_datetime(date)
        row_indices = self.market_calendar[self.market_calendar == target_date].index
        self.fake_date = row_indices[0]
        self.current_date = target_date

    def get_date(self):
        return self.current_date

    def set_cash_balance(self, cash):
        self.cash_balance = cash

    def get_cash_balance(self):
        return self.cash_balance

    def get_remain_time(self):
        return len(self.market_calendar) - self.fake_date

    def get_cumulative_cost(self) -> float:
        return self.cumulative_cost

