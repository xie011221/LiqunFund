import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from src.shareaccount import ShareAccount
from src.basicstrategy import BasicStrategy
from src.fullselection import FullSelection
from src.markowitz import MarkowitzPortfolio
from src.indicator import indicator

from datetime import datetime

# 设置pandas不要出现 FutureWarning: Down casting
pd.set_option('future.no_silent_downcasting', True)

# 配置策略
strategy = BasicStrategy(MarkowitzPortfolio(), FullSelection())

# 记录回测时间 模型名字
rebalance_freq_month = 2
now = datetime.now().strftime('%Y-%m-%d %H%M%S')
strategy_name = strategy.get_strategy_name()
name = f'{strategy_name} {rebalance_freq_month}M BT'
print(name)

# 设置回测起止时间
start_date = pd.to_datetime('2020-01-02')
end_date = pd.to_datetime('2023-01-01')

# 读取数据
market = pd.read_parquet('../data/market.parquet')
hfq_market = pd.read_parquet('../data/hfq_market.parquet')
component_calendar = pd.read_parquet('../data/component_calendar.parquet')
basic = pd.read_parquet('../data/basic.parquet')
dividend = pd.read_parquet('../data/dividend.parquet')
hs300 = pd.read_parquet('../data/hs300_market.parquet')
suspend = pd.read_parquet('../data/suspend.parquet')

# 配置策略：等权重全样本
strategy.read_market(market)
strategy.read_hfq_market(hfq_market)
strategy.read_component_calendar(component_calendar)
strategy.read_basic(basic)


# 设置主账户：起始日2022年9月1日，起始资金300,000,000
main_account = ShareAccount()
main_account.read_market(market)
main_account.read_dividend(dividend)
main_account.set_date(start_date)
main_account.set_cash_balance(300000000)
strategy.read_suspend(suspend)

#
day_list = []
net_value_list = []
swap_date_list = []
portfolio_df_list = []

rebalance_after = rebalance_freq_month
last_month = main_account.get_date().month
current_month = main_account.get_date().month

#for day in range(main_account.get_remain_time()-1):
while main_account.get_date() < end_date:
    current_month = main_account.get_date().month
    if current_month != last_month:
        last_month = current_month
        rebalance_after = rebalance_after + 1
    if rebalance_after == rebalance_freq_month:
        td = main_account.get_date()
        print(f'调仓日: {td}')
        portfolio = strategy.portfolio.get_portfolio(main_account.get_date(), main_account.get_net_value())
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
dn_pd['market_portfolio'] = dn_pd['open'] / dn_pd['open'].iloc[0] * 300000000

# 只保留三列，并且保存为parquet
dn_pd = dn_pd[['trade_date','net_value','market_portfolio']]
dn_pd.to_parquet(f'../plotdata/{name} {now}.parquet')
dn_pd.to_parquet(f'../plotdata/{name}.parquet')

# 根据dn_pd计算各项指标，该函数自带输出，有副作用，故传递拷贝
strategy_indicator = indicator(dn_pd.copy(deep=True),now,name)
# 保存指标方便查阅
experiment_dataset = pd.read_parquet('../plotdata/experiment_dataset.parquet')
experiment_dataset = pd.concat([experiment_dataset,strategy_indicator])
experiment_dataset.to_parquet('../plotdata/experiment_dataset.parquet',engine='pyarrow')

# 计算绘图需求
dn_pd['excess'] = dn_pd['net_value'] - dn_pd['market_portfolio']

dn_pd['unit_net_value'] = dn_pd['net_value'] / dn_pd['net_value'].iloc[0]
dn_pd['unit_market_portfolio'] = dn_pd['market_portfolio'] / dn_pd['market_portfolio'].iloc[0]
dn_pd['unit_excess'] = dn_pd['unit_net_value'] - dn_pd['unit_market_portfolio']

dn_pd['log_return'] = np.log(dn_pd['unit_net_value'])
dn_pd['log_market_return'] = np.log(dn_pd['unit_market_portfolio'])
dn_pd['log_return_excess'] = dn_pd['log_return'] - dn_pd['log_market_return']

# 基金净值
plt.figure(1,figsize = (10,4))
plt.plot(dn_pd['trade_date'],dn_pd['net_value'],label=name)
plt.plot(dn_pd['trade_date'],dn_pd['market_portfolio'],label='Market')
plt.grid()
plt.xlabel('Date', fontsize=12)
plt.ylabel('Net Value (CNY)', fontsize=12)
plt.legend()
plt.savefig(f'../figures/{name} {now}.eps',format='eps')
plt.savefig(f'../figures/{name}.eps',format='eps')


# 基金净值带超额
plt.figure(2,figsize = (10,4))
plt.plot(dn_pd['trade_date'],dn_pd['net_value'],label=name)
plt.plot(dn_pd['trade_date'],dn_pd['market_portfolio'],label='Market')
plt.plot(dn_pd['trade_date'],dn_pd['excess'],label='Excess')
plt.grid()
plt.xlabel('Date', fontsize=12)
plt.ylabel('Net Value (CNY)', fontsize=12)
plt.legend()
plt.savefig(f'../figures/{name} with Excess {now}.eps',format='eps')
plt.savefig(f'../figures/{name} with Excess.eps',format='eps')


# 单位净值
plt.figure(3,figsize = (10,4))
plt.plot(dn_pd['trade_date'],dn_pd['unit_net_value'],label=name)
plt.plot(dn_pd['trade_date'],dn_pd['unit_market_portfolio'],label='Market')
plt.grid()
plt.xlabel('Date', fontsize=12)
plt.ylabel('Unit Net Value (CNY)', fontsize=12)
plt.legend()
plt.savefig(f'../figures/{name} UNV {now}.eps',format='eps')
plt.savefig(f'../figures/{name} UNV.eps',format='eps')


# 单位净值带超额
plt.figure(4,figsize = (10,4))
plt.plot(dn_pd['trade_date'],dn_pd['unit_net_value'],label=name)
plt.plot(dn_pd['trade_date'],dn_pd['unit_market_portfolio'],label='Market')
plt.plot(dn_pd['trade_date'],dn_pd['unit_excess'],label='Excess')
plt.grid()
plt.xlabel('Date', fontsize=12)
plt.ylabel('Unit Net Value (CNY)', fontsize=12)
plt.legend()
plt.savefig(f'../figures/{name} UNV with Excess {now}.eps',format='eps')
plt.savefig(f'../figures/{name} UNV with Excess.eps',format='eps')


# 对数收益率
plt.figure(5,figsize = (10,4))
plt.plot(dn_pd['trade_date'],dn_pd['log_return'],label=name)
plt.plot(dn_pd['trade_date'],dn_pd['log_market_return'],label='Market')
plt.grid()
plt.xlabel('Date', fontsize=12)
plt.ylabel('Log Return', fontsize=12)
plt.legend()
plt.savefig(f'../figures/{name} Log Return {now}.eps',format='eps')
plt.savefig(f'../figures/{name} Log Return.eps',format='eps')


# 对数收益率带超额
plt.figure(6,figsize = (10,4))
plt.plot(dn_pd['trade_date'],dn_pd['log_return'],label=name)
plt.plot(dn_pd['trade_date'],dn_pd['log_market_return'],label='Market')
plt.plot(dn_pd['trade_date'],dn_pd['log_return_excess'],label='Excess')
plt.grid()
plt.xlabel('Date', fontsize=12)
plt.ylabel('Log Return', fontsize=12)
plt.legend()
plt.savefig(f'../figures/{name} Log Return with Excess {now}.eps',format='eps')
plt.savefig(f'../figures/{name} Log Return with Excess.eps',format='eps')

plt.show()