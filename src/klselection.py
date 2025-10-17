import pandas as pd
import numpy as np
from src.selection import Selection
from scipy.stats import entropy,gaussian_kde

class KLSelection(Selection):

    def get_selection(self,date: pd.Timestamp) -> pd.DataFrame:
        component = self.strategy.get_current_component(date)['share_code'].to_list()
        year_hfq_market = self.strategy.get_year_hfq_market(date)
        share_kl = []
        for share in component:
            share_year_hfq_market = year_hfq_market[year_hfq_market['share_code'] == share].copy(deep=True)
            kl = self.calculate_kl_divergence(share_year_hfq_market)
            share_kl.append(kl)
        share_kl_df = pd.DataFrame({'share_code': component, 'share_kl': share_kl})
        share_kl_df = share_kl_df.sort_values('share_kl', ascending=True).reset_index(drop=True)
        share_50 = share_kl_df.iloc[self.first:self.last]['share_code'].to_frame()
        return share_50
        return shares

    def calculate_kl_divergence(self,share_year_hfq_market,bandwidth=0.00001):
        share_year_hfq_market = share_year_hfq_market.sort_values(['share_code', 'trade_date'])
        share_year_hfq_market.reset_index(drop=True, inplace=True)
        share_year_hfq_market.dropna(inplace=True)
        midpoint = share_year_hfq_market['trade_date'].median()
        first_half = share_year_hfq_market[share_year_hfq_market['trade_date'] <= midpoint]
        second_half = share_year_hfq_market[share_year_hfq_market['trade_date'] > midpoint]
        p_data = first_half['log_return'].values
        q_data = second_half['log_return'].values
        p_kde = gaussian_kde(p_data, bw_method=bandwidth)
        q_kde = gaussian_kde(q_data, bw_method=bandwidth)
        x_min = min(p_data.min(), q_data.min()) - 1
        x_max = max(p_data.max(), q_data.max()) + 1
        x = np.linspace(x_min, x_max, 1000)
        p_pdf = p_kde(x)
        q_pdf = q_kde(x)
        mask = (p_pdf > 1e-10) & (q_pdf > 1e-10)
        kl_div = entropy(p_pdf[mask], q_pdf[mask])
        return kl_div

    def set_grouped(self,first: int,last: int) -> None:
        self.first = first
        self.last = last


    def get_selection_name(self) -> str:
        return 'KL'