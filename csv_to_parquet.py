import pandas as pd
df = pd.read_csv('cdc_measurement_data_0001.csv')
df.to_parquet('cdc_measurement_data_0001.parquet')