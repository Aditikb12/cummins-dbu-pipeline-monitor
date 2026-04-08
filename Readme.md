# Cummins DBU Engine Sensor Data Pipeline Monitor

A data engineering project simulating Cummins' DBU Azure Data Lake architecture with Medallion layers, Purview-style governance, and engine sensor quality monitoring.

## Live Demo
👉 [View Live Dashboard](https://dbu-engine-pipeline-monitor.streamlit.app)

## What it does
- Ingests NASA turbofan engine sensor data (20,631 records across 100 engines)
- Structures data into Bronze → Silver → Gold Medallion layers
- Applies rolling averages at Silver layer to smooth noisy sensor readings
- Classifies engine health based on Remaining Useful Life (RUL)
- Implements Purview-style sensitivity labels and retention policies
- Monitors data quality pass/fail rates across the pipeline

## Tech Stack
- Python, Pandas, DuckDB
- Streamlit (dashboard)
- Plotly (visualizations)
- Medallion Architecture (Bronze/Silver/Gold)
- Microsoft Purview governance simulation

## Architecture
Raw Sensor Data → Bronze (raw ingestion) → Silver (cleaned + rolling avg) → Gold (aggregated health metrics) → Dashboard