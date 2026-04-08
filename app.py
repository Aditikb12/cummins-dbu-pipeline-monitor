import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import subprocess
import os

st.set_page_config(page_title="Cummins DBU Pipeline Monitor", layout="wide")

# Auto-build database if it doesn't exist
if not os.path.exists("cummins_pipeline.db"):
    with st.spinner("Building pipeline database..."):
        subprocess.run(["python", "generate_data.py"])
        subprocess.run(["python", "bronze.py"])
        subprocess.run(["python", "silver.py"])
        subprocess.run(["python", "gold.py"])

con = duckdb.connect("cummins_pipeline.db")

st.title("Cummins DBU: Engine Sensor Data Pipeline Monitor")
st.caption("Simulating Azure Data Lake + Microsoft Purview governance | NASA Turbofan Engine Dataset")

tab1, tab2, tab3, tab4 = st.tabs(["Pipeline Health", "Engine Health", "Sensor Trends", "Governance"])

# ── TAB 1: Pipeline Health ──
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

# ── TAB 2: Engine Health ──
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

# ── TAB 3: Sensor Trends ──
with tab3:
    st.subheader("Sensor Degradation Over Time")
    gold_e = con.execute("SELECT * FROM gold_engine").df()
    engine_ids = gold_e['engine_id'].tolist()
    selected_engine = st.selectbox("Select Engine to Inspect", engine_ids)

    silver_df = con.execute(f"SELECT * FROM silver_engine WHERE engine_id = {selected_engine} ORDER BY cycle").df()

    sensor_options = ['sensor_2', 'sensor_3', 'sensor_4', 'sensor_7', 'sensor_11', 'sensor_12']
    sensor_options = [s for s in sensor_options if s in silver_df.columns]
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

# ── TAB 4: Governance ──
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