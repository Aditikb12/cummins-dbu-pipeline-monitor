import duckdb
import pandas as pd

con = duckdb.connect("cummins_pipeline.db")
df = con.execute("SELECT * FROM silver_engine").df()

# Gold summary per engine
gold_engine = df.groupby('engine_id').agg(
    total_cycles=('cycle', 'max'),
    min_RUL=('RUL', 'min'),
    max_RUL=('RUL', 'max'),
    current_RUL=('RUL', 'first'),
    avg_sensor_2=('sensor_2', 'mean'),
    avg_sensor_3=('sensor_3', 'mean'),
    avg_sensor_4=('sensor_4', 'mean'),
    avg_sensor_7=('sensor_7', 'mean'),
    quality_pass_rate=('data_quality_flag', lambda x: round((x == 'Pass').sum() / len(x) * 100, 1))
).reset_index()

gold_engine['engine_health'] = gold_engine['max_RUL'].apply(
    lambda x: 'Critical' if x <= 100 else ('Warning' if x <= 200 else 'Healthy')
)

# Overall pipeline quality summary
gold_quality = pd.DataFrame([{
    'source': 'NASA_Turbofan',
    'total_records': len(df),
    'pass_count': len(df[df['data_quality_flag'] == 'Pass']),
    'fail_count': len(df[df['data_quality_flag'] == 'Fail']),
    'pass_rate': round(len(df[df['data_quality_flag'] == 'Pass']) / len(df) * 100, 1),
    'critical_engines': len(gold_engine[gold_engine['engine_health'] == 'Critical']),
    'warning_engines': len(gold_engine[gold_engine['engine_health'] == 'Warning']),
    'healthy_engines': len(gold_engine[gold_engine['engine_health'] == 'Healthy']),
}])

con.execute("DROP TABLE IF EXISTS gold_engine")
con.execute("CREATE TABLE gold_engine AS SELECT * FROM gold_engine")
con.execute("DROP TABLE IF EXISTS gold_quality")
con.execute("CREATE TABLE gold_quality AS SELECT * FROM gold_quality")

print("Gold loaded")
print(gold_quality.T)
con.close()