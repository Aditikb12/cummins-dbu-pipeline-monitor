import duckdb
import pandas as pd

con = duckdb.connect("cummins_pipeline.db")

df = pd.read_csv('engine_data.csv')

# Bronze — raw ingestion, add governance tags
df['sensitivity_label'] = 'Internal'
df['retention_category'] = 'Operational_1yr'
df['data_layer'] = 'Bronze'

con.execute("DROP TABLE IF EXISTS bronze_engine")
con.execute("CREATE TABLE bronze_engine AS SELECT * FROM df")

print(f"Bronze loaded: {len(df)} records")
print(f"Engines: {df['engine_id'].nunique()}")
print(f"Columns: {list(df.columns)}")
con.close()