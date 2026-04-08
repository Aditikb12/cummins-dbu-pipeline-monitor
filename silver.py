import duckdb
import pandas as pd
import numpy as np

con = duckdb.connect("cummins_pipeline.db")
df = con.execute("SELECT * FROM bronze_engine").df()

# Drop low variance sensors (settings that dont change much)
low_variance_cols = ['setting_3', 'sensor_1', 'sensor_5', 'sensor_10', 'sensor_16', 'sensor_18', 'sensor_19']
df = df.drop(columns=[c for c in low_variance_cols if c in df.columns])

# Add rolling average for key sensors per engine (smoothing noise)
df = df.sort_values(['engine_id', 'cycle'])
for sensor in ['sensor_2', 'sensor_3', 'sensor_4', 'sensor_7', 'sensor_11', 'sensor_12']:
    if sensor in df.columns:
        df[f'{sensor}_rolling_avg'] = df.groupby('engine_id')[sensor].transform(
            lambda x: x.rolling(window=5, min_periods=1).mean()
        ).round(4)

# Data quality flag
def quality_check(row):
    if row['RUL'] < 0:
        return 'Fail'
    if pd.isnull(row['sensor_2']) or pd.isnull(row['sensor_3']):
        return 'Fail'
    return 'Pass'

df['data_quality_flag'] = df.apply(quality_check, axis=1)
df['data_layer'] = 'Silver'

# Engine health flag
df['engine_health'] = df['RUL'].apply(
    lambda x: 'Critical' if x <= 30 else ('Warning' if x <= 80 else 'Healthy')
)

con.execute("DROP TABLE IF EXISTS silver_engine")
con.execute("CREATE TABLE silver_engine AS SELECT * FROM df")

pass_count = len(df[df['data_quality_flag'] == 'Pass'])
fail_count = len(df[df['data_quality_flag'] == 'Fail'])
print(f"Silver loaded: {len(df)} records")
print(f"Pass: {pass_count} | Fail: {fail_count}")
print(f"Engine health breakdown:\n{df.groupby('engine_health')['engine_id'].nunique()}")
con.close()