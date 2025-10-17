import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from src.shareaccount import ShareAccount
from src.basicstrategy import BasicStrategy
from src.klselection import KLSelection
from src.equalportfolio import EqualPortfolio
from src.markowitz import MarkowitzPortfolio
from src.indicator import indicator

from datetime import datetime

# 设置pandas不要出现 FutureWarning: Down casting
pd.set_option('future.no_silent_downcasting', True)

# 配置策略
'''
strategy = BasicStrategy(EqualPortfolio(), KLSelection())
strategy2 = BasicStrategy(EqualPortfolio(), KLSelection())
strategy3 = BasicStrategy(EqualPortfolio(), KLSelection())
strategy4 = BasicStrategy(EqualPortfolio(), KLSelection())
strategy5 = BasicStrategy(EqualPortfolio(), KLSelection())
strategy6 = BasicStrategy(EqualPortfolio(), KLSelection())
'''

strategy = BasicStrategy(MarkowitzPortfolio(), KLSelection())
strategy2 = BasicStrategy(MarkowitzPortfolio(), KLSelection())
strategy3 = BasicStrategy(MarkowitzPortfolio(), KLSelection())
strategy4 = BasicStrategy(MarkowitzPortfolio(), KLSelection())
strategy5 = BasicStrategy(MarkowitzPortfolio(), KLSelection())
strategy6 = BasicStrategy(MarkowitzPortfolio(), KLSelection())

# 记录回测时间 模型名字
rebalance_freq_month = 2
now = datetime.now().strftime('%Y-%m-%d %H%M%S')
strategy_name = strategy.get_strategy_name()
name = f'{strategy_name} Grouped {rebalance_freq_month}M R'
print(name)

# 设置回测起止时间
start_date = pd.to_datetime('2017-01-04')
end_date = pd.to_datetime('2025-08-28')

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
strategy.selection.set_grouped(0,50)

strategy2.read_market(market)
strategy2.read_hfq_market(hfq_market)
strategy2.read_component_calendar(component_calendar)
strategy2.read_basic(basic)
strategy2.selection.set_grouped(50,100)

strategy3.read_market(market)
strategy3.read_hfq_market(hfq_market)
strategy3.read_component_calendar(component_calendar)
strategy3.read_basic(basic)
strategy3.selection.set_grouped(100,150)

strategy4.read_market(market)
strategy4.read_hfq_market(hfq_market)
strategy4.read_component_calendar(component_calendar)
strategy4.read_basic(basic)
strategy4.selection.set_grouped(150,200)

strategy5.read_market(market)
strategy5.read_hfq_market(hfq_market)
strategy5.read_component_calendar(component_calendar)
strategy5.read_basic(basic)
strategy5.selection.set_grouped(200,250)

strategy6.read_market(market)
strategy6.read_hfq_market(hfq_market)
strategy6.read_component_calendar(component_calendar)
strategy6.read_basic(basic)
strategy6.selection.set_grouped(250,300)

# 设置主账户：起始日2022年9月1日，起始资金300,000,000
main_account = ShareAccount()
main_account.read_market(market)
main_account.read_dividend(dividend)
main_account.set_date(start_date)
main_account.set_cash_balance(300000000)
strategy.read_suspend(suspend)

main_account2 = ShareAccount()
main_account2.read_market(market)
main_account2.read_dividend(dividend)
main_account2.set_date(start_date)
main_account2.set_cash_balance(300000000)
strategy2.read_suspend(suspend)

main_account3 = ShareAccount()
main_account3.read_market(market)
main_account3.read_dividend(dividend)
main_account3.set_date(start_date)
main_account3.set_cash_balance(300000000)
strategy3.read_suspend(suspend)

main_account4 = ShareAccount()
main_account4.read_market(market)
main_account4.read_dividend(dividend)
main_account4.set_date(start_date)
main_account4.set_cash_balance(300000000)
strategy4.read_suspend(suspend)

main_account5 = ShareAccount()
main_account5.read_market(market)
main_account5.read_dividend(dividend)
main_account5.set_date(start_date)
main_account5.set_cash_balance(300000000)
strategy5.read_suspend(suspend)

main_account6 = ShareAccount()
main_account6.read_market(market)
main_account6.read_dividend(dividend)
main_account6.set_date(start_date)
main_account6.set_cash_balance(300000000)
strategy6.read_suspend(suspend)

#
day_list = []
net_value_list = []
swap_date_list = []
portfolio_df_list = []

net_value_list2 = []
net_value_list3 = []
net_value_list4 = []
net_value_list5 = []
net_value_list6 = []

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

        portfolio  = strategy.portfolio.get_portfolio( main_account.get_date(),  main_account.get_net_value())
        portfolio2 = strategy2.portfolio.get_portfolio(main_account2.get_date(), main_account2.get_net_value())
        portfolio3 = strategy3.portfolio.get_portfolio(main_account3.get_date(), main_account3.get_net_value())
        portfolio4 = strategy4.portfolio.get_portfolio(main_account4.get_date(), main_account4.get_net_value())
        portfolio5 = strategy5.portfolio.get_portfolio(main_account5.get_date(), main_account5.get_net_value())
        portfolio6 = strategy6.portfolio.get_portfolio(main_account6.get_date(), main_account6.get_net_value())

        main_account.update_holding(portfolio)
        main_account2.update_holding(portfolio2)
        main_account3.update_holding(portfolio3)
        main_account4.update_holding(portfolio4)
        main_account5.update_holding(portfolio5)
        main_account6.update_holding(portfolio6)

        swap_date_list.append(main_account.get_date())
        portfolio['trade_date'] = main_account.get_date()
        portfolio_df_list.append(portfolio)
        rebalance_after = 0

    main_account.update_dividend()
    main_account2.update_dividend()
    main_account3.update_dividend()
    main_account4.update_dividend()
    main_account5.update_dividend()
    main_account6.update_dividend()

    day_list.append(main_account.get_date())

    net_value_list.append(main_account.get_net_value())
    net_value_list2.append(main_account2.get_net_value())
    net_value_list3.append(main_account3.get_net_value())
    net_value_list4.append(main_account4.get_net_value())
    net_value_list5.append(main_account5.get_net_value())
    net_value_list6.append(main_account6.get_net_value())

    main_account.next_day()
    main_account2.next_day()
    main_account3.next_day()
    main_account4.next_day()
    main_account5.next_day()
    main_account6.next_day()

#print(swap_date_list)

dn_pd = pd.DataFrame({'trade_date':day_list,
                      'net_value':net_value_list,
                      'net_value2':net_value_list2,
                      'net_value3':net_value_list3,
                      'net_value4':net_value_list4,
                      'net_value5':net_value_list5,
                      'net_value6':net_value_list6
                      })

dn_pd.to_parquet(f'../plotdata/{name}')

plt.figure(figsize=(20,8))
plt.plot(dn_pd['trade_date'],dn_pd['net_value'],label=name)
plt.plot(dn_pd['trade_date'],dn_pd['net_value2'],label=f'{name} 2')
plt.plot(dn_pd['trade_date'],dn_pd['net_value3'],label=f'{name} 3')
plt.plot(dn_pd['trade_date'],dn_pd['net_value4'],label=f'{name} 4')
plt.plot(dn_pd['trade_date'],dn_pd['net_value5'],label=f'{name} 5')
plt.plot(dn_pd['trade_date'],dn_pd['net_value6'],label=f'{name} 6')
plt.legend()
plt.grid()

plt.savefig(f'../figures/{name} time {now}.png')
plt.savefig(f'../figures/{name} time {now}.eps',format='eps')

plt.show()