import pandas as pd
import numpy as np

col_names = ['engine_id', 'cycle'] + \
            [f'setting_{i}' for i in range(1, 4)] + \
            [f'sensor_{i}' for i in range(1, 22)]

# Fix parsing — use whitespace separator and skip bad columns
df = pd.read_csv(
    r'C:/Users/aditi/Documents/python/train_FD001.txt',
    sep=r'\s+',
    header=None,
    names=col_names + ['extra1', 'extra2'],
    engine='python'
)

# Drop extra empty columns at the end
df = df.drop(columns=['extra1', 'extra2'], errors='ignore')

# Force correct data types
df['engine_id'] = df['engine_id'].astype(int)
df['cycle'] = df['cycle'].astype(int)

# Add metadata
df['ingestion_timestamp'] = pd.Timestamp.now().isoformat()
df['source_system'] = 'NASA_Turbofan_Sensor'

# Calculate RUL per engine
max_cycles = df.groupby('engine_id')['cycle'].max().reset_index()
max_cycles.columns = ['engine_id', 'max_cycle']
df = df.merge(max_cycles, on='engine_id')
df['RUL'] = df['max_cycle'] - df['cycle']
df.drop(columns=['max_cycle'], inplace=True)

df.to_csv('engine_data.csv', index=False)
print(f"Data generated: {len(df)} rows, {df['engine_id'].nunique()} engines")
print(df[['engine_id', 'cycle', 'RUL']].head(10))