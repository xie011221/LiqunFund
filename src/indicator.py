import pandas as pd
import numpy as np

def indicator(dn_pd: pd.DataFrame,test_time: str,model_name: str) -> pd.DataFrame:

    corr = dn_pd[['net_value','market_portfolio']].corr()
    print(corr)

    start_date = pd.to_datetime(dn_pd['trade_date'].iloc[0])
    end_date = pd.to_datetime(dn_pd['trade_date'].iloc[-1])
    print(f'从: {start_date} 到: {end_date}')

    total_days = (end_date - start_date).days
    total_years = total_days / 365
    print(f'投资时长: {total_days}天, 或 {total_years:.2f}年')
    start_net_value = dn_pd['net_value'].iloc[0]
    end_net_value = dn_pd['net_value'].iloc[-1]
    delta_net_value = end_net_value - start_net_value
    print(f'期初净值: {start_net_value:.2f}元')
    print(f'期末净值: {end_net_value:.2f}元')
    print(f'净增长：{delta_net_value:.2f}元')
    total_log_return = np.log(dn_pd['net_value'].iloc[-1]/dn_pd['net_value'].iloc[0])
    annualized_log_return = total_log_return / total_years
    print(f'年化对数收益率: {annualized_log_return:.3%}')
    print(f'总对数收益率: {total_log_return:.3%}')

    risk_free_rate = 0.017
    #daily_risk_free = (1 + risk_free_rate) ** (1 / 252) - 1
    #daily_risk_free = np.log()
    daily_returns = dn_pd['net_value'].pct_change().dropna()
    dn_pd['log_return'] = np.log(dn_pd['net_value']).diff()
    #excess_returns = daily_returns - daily_risk_free
    #annualized_excess_return = dn_pd['log_return'].mean() * 252 - risk_free_rate
    annualized_excess_return = annualized_log_return - risk_free_rate
    annualized_volatility = dn_pd['log_return'].std() * np.sqrt(252)
    print(f'年化超额收益率: {annualized_excess_return:.3%}')
    print(f'年化波动率: {annualized_volatility:.3%}')
    # 夏普比率
    sharpe_ratio = annualized_excess_return / annualized_volatility
    print(f"夏普比率: {sharpe_ratio:.4f}")

    dn_pd['pct_value'] = dn_pd['net_value'] / dn_pd['net_value'].iloc[0]
    pre_max_pct_value = dn_pd['pct_value'].iloc[0]
    max_drawdown = 0
    for pct_value in dn_pd['pct_value'].values:
        if pct_value > pre_max_pct_value:
            pre_max_pct_value = pct_value
        if pre_max_pct_value - pct_value > max_drawdown:
            max_drawdown = pre_max_pct_value - pct_value
    print(f'最大回撤: {max_drawdown:.3%}')

    indicator_df = pd.DataFrame({
        'test_time': [pd.to_datetime(test_time)],
        'model_name': [model_name],
        'delta_time':[total_days],
        'start_net_value':[start_net_value],
        'end_net_value':[end_net_value],
        'delta_net_value':[delta_net_value],
        'annualized_log_return':[annualized_log_return],
        'total_log_return':[total_log_return],
        'annualized_excess_return':[annualized_excess_return],
        'annualized_volatility':[annualized_volatility],
        'sharpe_ratio':[sharpe_ratio],
        'max_drawdown':[max_drawdown]
    })
    return indicator_df

# 单元测试
if __name__ == '__main__':
    dn_pd = pd.read_parquet('../plotdata/Equal Full Rebalance=50.parquet')
    test_time = '2025-10-08 160651'
    model_name = 'Equal Full Rebalance=50'
    test_indicator = indicator(dn_pd,test_time,model_name)
    print(test_indicator)
    experiment_dataset = pd.read_parquet('../plotdata/experiment_dataset.parquet')
    experiment_dataset = pd.concat([experiment_dataset,test_indicator])
    experiment_dataset.to_parquet('../plotdata/experiment_dataset.parquet',engine='pyarrow')