import streamlit as st
import pandas as pd
import psycopg2
import time
import plotly.express as px

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Global Server Monitor",
    page_icon="🦑",
    layout="wide",
)

# --- DATABASE CONNECTION ---
def get_data():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="bigdata_db",
            user="postgres",
            password="password"
        )
        # Grab the most recent 2000 rows to see trends better
        query = """
        SELECT * FROM server_metrics 
        ORDER BY timestamp DESC 
        LIMIT 2000
        """
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

# --- HELPER: CLEAN UP REGIONS ---
def get_region(host_id):
    # Note: We removed "LIVE" because now the stream mimics these 3 real places!
    if "IND" in host_id: return "🇮🇳 India Lab"
    if "us" in host_id: return "🇺🇸 US Cloud"
    if "DE" in host_id: return "🇩🇪 Germany HPC"
    return "Other"

# --- MAIN DASHBOARD ---
st.title("🚀 Real-Time Big Data Pipeline Monitor")

# SIDEBAR CONTROLS
st.sidebar.header("Controls")
live_mode = st.sidebar.checkbox("Enable Real-Time Updates", value=True)
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", 0.5, 100.0, 1.0)

# 1. Get Data
df = get_data()

if df.empty:
    st.warning("⏳ Waiting for data... Ensure pipelines are running.")
    if live_mode:
        time.sleep(2)
        st.rerun()
else:
    # Apply Region Grouping
    df['Region'] = df['host_id'].apply(get_region)
    # Filter for high CPU (>90%) or High Temp (>90C)
    anomalies = df[ (df['cpu_load'] > 90) | (df['temp_c'] > 90) ]
    
    if not anomalies.empty:
        # Show a big red warning box
        st.error(f"🚨 CRITICAL ALERT: {len(anomalies)} Servers are Overheating (>90°C) or Overloaded!")
        
        # Optional: Show the specific bad servers in a dropout menu
        with st.expander("⚠️ View Problematic Servers Details"):
            st.dataframe(anomalies[['timestamp', 'host_id', 'Region', 'temp_c', 'cpu_load']])
    else:
        st.success("✅ System Status: Normal Operations")

    # 2. KPI Metrics 
    kpi1, kpi2, kpi3 = st.columns(3)

    avg_temp = df['temp_c'].mean()
    avg_cpu = df['cpu_load'].mean()
    total_power = df['power_watts'].sum()
    
    curr_temp = df['temp_c'].iloc[0]
    curr_cpu = df['cpu_load'].iloc[0]

    kpi1.metric("Avg Global Temp", f"{avg_temp:.1f} °C", f"{curr_temp - avg_temp:.1f}")
    kpi2.metric("Avg CPU Load", f"{avg_cpu:.1f}%", f"{curr_cpu - avg_cpu:.1f}")
    kpi3.metric("Est. Power Usage", f"{total_power/1000:.1f} kW")

    # 3. Top Charts (Line & Histogram)
    fig_col1, fig_col2 = st.columns(2)

    with fig_col1:
        st.markdown("### 📡 Live CPU Load (By Region)")
        chart_df = df.groupby(['timestamp', 'Region'])['cpu_load'].mean().reset_index()
        fig = px.line(chart_df, x="timestamp", y="cpu_load", color="Region", title="CPU Load Trends")
        st.plotly_chart(fig, use_container_width=True)

    with fig_col2:
        st.markdown("### 📊 Data Volume by Region")
        # Counts how many rows came from each region
        count_df = df['Region'].value_counts().reset_index()
        count_df.columns = ['Region', 'Record Count']
        
        fig3 = px.bar(
            count_df, 
            x="Region", 
            y="Record Count", 
            color="Region", 
            title="Incoming Data Volume (Last 2000 Records)"
        )
        st.plotly_chart(fig3, use_container_width=True)

    # 4. Bottom Chart (Temperature)
    st.markdown("### 🌡️ Temperature Distribution")
    fig2 = px.histogram(df, x="temp_c", color="Region", nbins=20, title="Temp Distribution")
    st.plotly_chart(fig2, use_container_width=True)

    # 5. Data Table
    st.markdown("### 📝 Live Data Feed")
    st.dataframe(df.head(15), use_container_width=True)

    # --- autorefresh logic ---
    if live_mode:
        time.sleep(refresh_rate)
        st.rerun()