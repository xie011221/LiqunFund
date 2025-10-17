import pandas as pd

component_df = pd.read_parquet('../data/component_full.parquet')

component_full = component_df.drop_duplicates(subset=['share_code'])

component_full.to_parquet('../data/component.parquet',engine='pyarrow')