import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os

st.set_page_config(page_title="Cummins DBU Pipeline Monitor", layout="wide")

def build_pipeline(con):
    # Generate data
    col_names = ['engine_id', 'cycle'] + \
                [f'setting_{i}' for i in range(1, 4)] + \
                [f'sensor_{i}' for i in range(1, 22)]
    
    df = pd.read_csv('train_FD001.txt', sep=r'\s+', header=None,
                     names=col_names + ['extra1', 'extra2'], engine='python')
    df = df.drop(columns=['extra1', 'extra2'], errors='ignore')
    df['engine_id'] = df['engine_id'].astype(int)
    df['cycle'] = df['cycle'].astype(int)
    df['ingestion_timestamp'] = pd.Timestamp.now().isoformat()
    df['source_system'] = 'NASA_Turbofan_Sensor'
    max_cycles = df.groupby('engine_id')['cycle'].max().reset_index()
    max_cycles.columns = ['engine_id', 'max_cycle']
    df = df.merge(max_cycles, on='engine_id')
    df['RUL'] = df['max_cycle'] - df['cycle']
    df.drop(columns=['max_cycle'], inplace=True)

    # Bronze
    df['sensitivity_label'] = 'Internal'
    df['retention_category'] = 'Operational_1yr'
    df['data_layer'] = 'Bronze'
    con.execute("DROP TABLE IF EXISTS bronze_engine")
    con.execute("CREATE TABLE bronze_engine AS SELECT * FROM df")

    # Silver
    low_variance_cols = ['setting_3', 'sensor_1', 'sensor_5', 'sensor_10', 'sensor_16', 'sensor_18', 'sensor_19']
    df = df.drop(columns=[c for c in low_variance_cols if c in df.columns])
    df = df.sort_values(['engine_id', 'cycle'])
    for sensor in ['sensor_2', 'sensor_3', 'sensor_4', 'sensor_7', 'sensor_11', 'sensor_12']:
        if sensor in df.columns:
            df[f'{sensor}_rolling_avg'] = df.groupby('engine_id')[sensor].transform(
                lambda x: x.rolling(window=5, min_periods=1).mean()).round(4)
    df['data_quality_flag'] = df.apply(
        lambda r: 'Fail' if r['RUL'] < 0 or pd.isnull(r.get('sensor_2')) else 'Pass', axis=1)
    df['data_layer'] = 'Silver'
    df['engine_health'] = df['RUL'].apply(
        lambda x: 'Critical' if x <= 30 else ('Warning' if x <= 80 else 'Healthy'))
    con.execute("DROP TABLE IF EXISTS silver_engine")
    con.execute("CREATE TABLE silver_engine AS SELECT * FROM df")

    # Gold
    gold_engine = df.groupby('engine_id').agg(
        total_cycles=('cycle', 'max'),
        min_RUL=('RUL', 'min'),
        max_RUL=('RUL', 'max'),
        avg_sensor_2=('sensor_2', 'mean'),
        avg_sensor_3=('sensor_3', 'mean'),
        quality_pass_rate=('data_quality_flag', lambda x: round((x == 'Pass').sum() / len(x) * 100, 1))
    ).reset_index()
    gold_engine['engine_health'] = gold_engine['max_RUL'].apply(
        lambda x: 'Critical' if x <= 100 else ('Warning' if x <= 200 else 'Healthy'))
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

# Build pipeline
con = duckdb.connect("cummins_pipeline.db")
try:
    con.execute("SELECT 1 FROM gold_quality LIMIT 1")
except:
    with st.spinner("Building pipeline: this takes about 30 seconds..."):
        build_pipeline(con)

st.title("Cummins DBU: Engine Sensor Data Pipeline Monitor")
st.caption("Simulating Azure Data Lake + Microsoft Purview governance | NASA Turbofan Engine Dataset")

tab1, tab2, tab3, tab4 = st.tabs(["Pipeline Health", "Engine Health", "Sensor Trends", "Governance"])

with tab1:
    st.subheader("Medallion Architecture: Layer Status")
    gold_q = con.execute("SELECT * FROM gold_quality").df()
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("### Bronze Layer")
        bronze_count = con.execute("SELECT COUNT(*) as cnt FROM bronze_engine").df()['cnt'][0]
        st.metric("Total Records Ingested", f"{bronze_count:,}")
        st.metric("Source", "NASA Turbofan Sensors")
        st.metric("Sensitivity", "Internal")
    with col2:
        st.markdown("### Silver Layer")
        silver_count = con.execute("SELECT COUNT(*) as cnt FROM silver_engine").df()['cnt'][0]
        st.metric("Records After Cleaning", f"{silver_count:,}")
        st.metric("Rolling Avg Applied", "6 sensors")
        st.metric("Low Variance Dropped", "7 columns")
    with col3:
        st.markdown("### Gold Layer")
        st.metric("Quality Pass Rate", f"{gold_q['pass_rate'][0]}%")
        st.metric("Critical Engines", f"{int(gold_q['critical_engines'][0])}", delta="Needs attention", delta_color="inverse")
        st.metric("Healthy Engines", f"{int(gold_q['healthy_engines'][0])}", delta="On track")

with tab2:
    st.subheader("Engine Health: Remaining Useful Life (RUL)")
    gold_e = con.execute("SELECT * FROM gold_engine ORDER BY min_RUL ASC").df()
    col1, col2, col3 = st.columns(3)
    with col1:
        critical = gold_e[gold_e['engine_health'] == 'Critical']
        st.error(f"CRITICAL — {len(critical)} engines (RUL ≤ 30 cycles)")
        st.dataframe(critical[['engine_id', 'min_RUL', 'total_cycles']].head(10))
    with col2:
        warning = gold_e[gold_e['engine_health'] == 'Warning']
        st.warning(f"WARNING — {len(warning)} engines (RUL ≤ 80 cycles)")
        st.dataframe(warning[['engine_id', 'min_RUL', 'total_cycles']].head(10))
    with col3:
        healthy = gold_e[gold_e['engine_health'] == 'Healthy']
        st.success(f"HEALTHY — {len(healthy)} engines (RUL > 80 cycles)")
        st.dataframe(healthy[['engine_id', 'min_RUL', 'total_cycles']].head(10))
    fig = px.bar(gold_e, x='engine_id', y='max_RUL',
                 color='engine_health',
                 color_discrete_map={'Critical': 'red', 'Warning': 'orange', 'Healthy': 'green'},
                 title='Remaining Useful Life per Engine')
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.subheader("Sensor Degradation Over Time")
    gold_e = con.execute("SELECT * FROM gold_engine").df()
    engine_ids = sorted(gold_e['engine_id'].tolist())
    selected_engine = st.selectbox("Select Engine to Inspect", engine_ids)
    silver_df = con.execute(f"SELECT * FROM silver_engine WHERE engine_id = {selected_engine} ORDER BY cycle").df()
    sensor_options = [s for s in ['sensor_2', 'sensor_3', 'sensor_4', 'sensor_7', 'sensor_11', 'sensor_12'] if s in silver_df.columns]
    selected_sensor = st.selectbox("Select Sensor", sensor_options)
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=silver_df['cycle'], y=silver_df[selected_sensor],
                             mode='lines', name='Raw Reading', line=dict(color='lightblue', width=1)))
    rolling_col = f"{selected_sensor}_rolling_avg"
    if rolling_col in silver_df.columns:
        fig.add_trace(go.Scatter(x=silver_df['cycle'], y=silver_df[rolling_col],
                                 mode='lines', name='Rolling Avg (Silver)', line=dict(color='orange', width=2)))
    fig.update_layout(title=f'Engine {selected_engine} — {selected_sensor} over cycles',
                      xaxis_title='Cycle', yaxis_title='Sensor Value')
    st.plotly_chart(fig, use_container_width=True)
    health = gold_e[gold_e['engine_id'] == selected_engine]['engine_health'].values[0]
    rul = gold_e[gold_e['engine_id'] == selected_engine]['min_RUL'].values[0]
    if health == 'Critical':
        st.error(f"Engine {selected_engine} — {health} | RUL: {rul} cycles remaining")
    elif health == 'Warning':
        st.warning(f"Engine {selected_engine} — {health} | RUL: {rul} cycles remaining")
    else:
        st.success(f"Engine {selected_engine} — {health} | RUL: {rul} cycles remaining")

with tab4:
    st.subheader("Data Governance — Purview-style Classification")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Sensitivity Labels")
        st.write("🟢 **NASA Turbofan Sensor Data** — Internal")
        st.write("🔴 **Engine ID Mappings** — Confidential")
        st.write("🔴 **Maintenance Records** — Confidential")
    with col2:
        st.markdown("#### Retention Policies")
        st.write(" **Sensor Readings** — Operational_1yr")
        st.write(" **Engine Health Reports** — Compliance_7yr")
        st.write(" **Pipeline Audit Logs** — Archive_Permanent")
    st.divider()
    gold_q = con.execute("SELECT * FROM gold_quality").df()
    st.subheader("Pipeline Quality Summary")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Records", f"{int(gold_q['total_records'][0]):,}")
    col2.metric("Pass Rate", f"{gold_q['pass_rate'][0]}%")
    col3.metric("Failed Records", f"{int(gold_q['fail_count'][0]):,}")

con.close()