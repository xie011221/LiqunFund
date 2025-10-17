import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from src.shareaccount import ShareAccount
from src.basicstrategy import BasicStrategy
from src.fullselection import FullSelection
from src.testportfolio import TestPortfolio
from src.indicator import indicator

from datetime import datetime

# 设置pandas不要出现 FutureWarning: Down casting
pd.set_option('future.no_silent_downcasting', True)

# 记录回测时间 模型名字
rebalance_freq_month = 100000
now = datetime.now().strftime('%Y-%m-%d %H%M%S')
name = f'Dividend Test Rebalance={rebalance_freq_month}M'
print(name)

# 读取数据
market = pd.read_parquet('../data/market.parquet')
hfq_market = pd.read_parquet('../data/hfq_market.parquet')
component_calendar = pd.read_parquet('../data/component_calendar.parquet')
basic = pd.read_parquet('../data/basic.parquet')
dividend = pd.read_parquet('../data/dividend.parquet')
hs300 = pd.read_parquet('../data/hs300_market.parquet')

# 配置策略：等权重全样本
equal_full_strategy = BasicStrategy(TestPortfolio(),FullSelection())
equal_full_strategy.read_market(market)
equal_full_strategy.read_hfq_market(hfq_market)
equal_full_strategy.read_component_calendar(component_calendar)
equal_full_strategy.read_basic(basic)

# 设置主账户：起始日2022年9月1日，起始资金300,000,000
main_account = ShareAccount()
main_account.read_market(market)
main_account.read_dividend(dividend)
main_account.set_date('20170901')
main_account.set_cash_balance(300000000)

#
day_list = []
net_value_list = []
swap_date_list = []
portfolio_df_list = []

rebalance_after = rebalance_freq_month
last_month = main_account.get_date().month
current_month = main_account.get_date().month

for day in range(main_account.get_remain_time()-1):
    current_month = main_account.get_date().month
    if current_month != last_month:
        last_month = current_month
        rebalance_after = rebalance_after + 1
    if rebalance_after == rebalance_freq_month:
        td = main_account.get_date()
        print(f'调仓日: {td}')
        portfolio = equal_full_strategy.portfolio.get_portfolio(main_account.get_date(),main_account.get_net_value())
        main_account.update_holding(portfolio)
        swap_date_list.append(main_account.get_date())
        portfolio['trade_date'] = main_account.get_date()
        portfolio_df_list.append(portfolio)
        rebalance_after = 0
    main_account.update_dividend()
    day_list.append(main_account.get_date())
    net_value_list.append(main_account.get_net_value())
    main_account.next_day()

#print(swap_date_list)

# 保存持仓数据
portfolio_df = pd.concat(portfolio_df_list)
portfolio_df.to_parquet(f'../plotdata/Calendar {name} {now}.parquet')
portfolio_df.to_parquet(f'../plotdata/Calendar {name}.parquet')

# 处理净值数据
dn_pd = pd.DataFrame({'trade_date':day_list,'net_value':net_value_list})
dn_pd = pd.merge(dn_pd,hs300,how='left',on='trade_date')
#dn_pd['market_portfolio'] = dn_pd['open'] / dn_pd['net_value'].iloc[0] * 300000000

# 只保留三列，并且保存为parquet
#dn_pd = dn_pd[['trade_date','net_value','market_portfolio']]
dn_pd.to_parquet(f'../plotdata/{name} {now}.parquet')
dn_pd.to_parquet(f'../plotdata/{name}.parquet')

# 根据dn_pd计算各项指标，该函数自带输出，有副作用，故传递拷贝
strategy_indicator = indicator(dn_pd.copy(deep=True),now,name)
# 保存指标方便查阅
experiment_dataset = pd.read_parquet('../plotdata/experiment_dataset.parquet')
experiment_dataset = pd.concat([experiment_dataset,strategy_indicator])
experiment_dataset.to_parquet('../plotdata/experiment_dataset.parquet',engine='pyarrow')

# 基金净值
plt.figure(1,figsize = (10,4))
plt.plot(dn_pd['trade_date'],dn_pd['net_value'],label=name)
#plt.plot(dn_pd['trade_date'],dn_pd['market_portfolio'],label='Market')
plt.grid()
plt.xlabel('Date', fontsize=12)
plt.ylabel('Net Value (CNY)', fontsize=12)
plt.legend()
plt.show()