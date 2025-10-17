import pandas as pd
import numpy as np
from src.portfolio import Portfolio
from scipy.optimize import minimize


class MarkowitzPortfolio(Portfolio):

    def get_portfolio(self, date: pd.Timestamp, value: float) -> pd.DataFrame:
        shares_list = self.strategy.selection.get_selection(date)['share_code'].to_list()
        year_hfq_market = self.strategy.get_year_hfq_market(date)
        year_hfq_market = year_hfq_market.query("share_code in @shares_list").copy(deep=True)
        current_price = self.strategy.get_current_price(date)

        year_returns_pivot = year_hfq_market.pivot(index='trade_date', columns='share_code', values='log_return')
        year_returns_mean = year_returns_pivot.mean()
        year_returns_cov = year_returns_pivot.cov()

        n_assets = year_returns_cov.shape[0]
        init_guess = np.repeat(1 / n_assets, n_assets)
        constraints = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
        bounds = tuple((0, 0.1) for asset in range(n_assets))
        np.random.seed(0)
        result = minimize(
            fun=self.get_portfolio_var,
            # x0=init_guess,
            x0=np.random.dirichlet(np.ones(n_assets)),
            args=(year_returns_cov,),
            method='SLSQP',
            bounds=bounds,
            constraints=constraints,
            # options={'xtol': 1e-4, 'gtol': 1e-4, 'maxiter': 1000}
        )
        weights = pd.Series(
            data=result.x,
            index=year_returns_mean.index,
            name='optimized_weights',
            dtype=float
        )

        shares = pd.DataFrame(weights)
        shares = pd.merge(shares, current_price, how='left', on='share_code')
        shares.loc[shares['optimized_weights'] < 0, 'optimized_weights'] = 0
        shares['optimized_weights'] = shares['optimized_weights'] / shares['optimized_weights'].sum() # 确保约束等于1
        shares['quantity'] = shares['optimized_weights'] * value / shares['price']
        shares['quantity'] = np.floor_divide(shares['quantity'], 100) * 100
        portfolio = shares[['share_code', 'quantity']].copy(deep=True)
        portfolio = portfolio[portfolio['quantity'] != 0]
        return portfolio

    def get_portfolio_var(self,weights,returns_cov):
        return weights @ returns_cov @ weights

    def get_portfolio_name(self) -> str:
        return 'MV'