#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEXC Scalper V5.2 - Modern Dashboard
Menggunakan Streamlit + Plotly untuk visualisasi profesional
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import requests
import json
import time

# Konfigurasi Halaman
st.set_page_config(
    page_title="MEXC Scalper V5.2 Dashboard",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS untuk tampilan lebih modern
st.markdown("""
<style>
    .metric-card {
        background-color: #1e1e1e;
        border-radius: 10px;
        padding: 20px;
        margin: 10px 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar - Kontrol Utama
st.sidebar.title("⚙️ Kontrol Bot")
st.sidebar.markdown("---")

# Status Koneksi
BOT_URL = "http://localhost:8080"  # Ganti dengan URL bot Anda
try:
    response = requests.get(f"{BOT_URL}/api/status", timeout=2)
    bot_status = response.json()
    is_online = True
except:
    is_online = False
    bot_status = {
        "mode": "UNKNOWN",
        "positions_count": 0,
        "daily_pnl": 0,
        "uptime": 0
    }

if is_online:
    st.sidebar.success("✅ Bot Online")
else:
    st.sidebar.error("❌ Bot Offline")

st.sidebar.markdown("---")

# Trading Mode Selector
st.sidebar.subheader("🎯 Trading Mode")
mode_options = ["AUTO", "SNIPER", "ACTIVE", "AGGRESSIVE", "DEFENSIVE"]
current_mode = st.sidebar.selectbox(
    "Pilih Mode:",
    mode_options,
    index=mode_options.index(bot_status.get("mode", "AUTO")) if bot_status.get("mode") in mode_options else 0
)

if st.sidebar.button("🔄 Update Mode"):
    try:
        requests.post(f"{BOT_URL}/api/set_mode", json={"mode": current_mode}, timeout=5)
        st.sidebar.success(f"Mode diubah ke {current_mode}")
        time.sleep(1)
        st.rerun()
    except Exception as e:
        st.sidebar.error(f"Gagal update mode: {e}")

# Sensitivity Sliders
st.sidebar.subheader("🎚️ Sensitivitas")
entry_sensitivity = st.sidebar.slider("Entry Sensitivity", 0, 100, 50)
vol_sensitivity = st.sidebar.slider("Volatility Sensitivity", 0, 100, 45)
news_sensitivity = st.sidebar.slider("News Sensitivity", 0, 100, 80)

if st.sidebar.button("💾 Apply Settings"):
    try:
        settings = {
            "entry_sensitivity": entry_sensitivity,
            "vol_sensitivity": vol_sensitivity,
            "news_sensitivity": news_sensitivity
        }
        requests.post(f"{BOT_URL}/api/config", json=settings, timeout=5)
        st.sidebar.success("Settings updated!")
    except Exception as e:
        st.sidebar.error(f"Gagal update settings: {e}")

st.sidebar.markdown("---")

# Emergency Controls
st.sidebar.subheader("🚨 Emergency")
if st.sidebar.button("⛔ STOP ALL TRADING", type="primary"):
    try:
        requests.post(f"{BOT_URL}/api/emergency_stop", timeout=5)
        st.sidebar.warning("Emergency stop activated!")
    except Exception as e:
        st.sidebar.error(f"Error: {e}")

if st.sidebar.button("🔄 Restart Bot"):
    try:
        requests.post(f"{BOT_URL}/api/restart", timeout=5)
        st.sidebar.info("Bot restarting...")
    except Exception as e:
        st.sidebar.error(f"Error: {e}")

# Main Content
st.title("🚀 MEXC Scalper V5.2 Dashboard")
st.markdown("**Real-time Monitoring & Analytics**")

# Refresh button
if st.button("🔄 Refresh Data"):
    st.rerun()

# Top Metrics Row
col1, col2, col3, col4 = st.columns(4)

with col1:
    # Tampilkan Adaptive Mode jika tersedia
    adaptive_mode = bot_status.get("adaptive_mode", bot_status.get("mode", "N/A"))
    st.metric(
        label="Trading Mode",
        value=adaptive_mode,
        delta="Auto" if not bot_status.get("manual_override", True) else "Manual"
    )

with col2:
    st.metric(
        label="Open Positions",
        value=bot_status.get("positions_count", 0),
        delta=None
    )

with col3:
    pnl = bot_status.get("daily_pnl", 0)
    st.metric(
        label="Daily PnL",
        value=f"${pnl:.2f}",
        delta=f"{pnl:.2%}" if pnl != 0 else None,
        delta_color="normal" if pnl >= 0 else "inverse"
    )

with col4:
    uptime_sec = bot_status.get("uptime", 0)
    uptime_str = f"{int(uptime_sec // 3600)}h {int((uptime_sec % 3600) // 60)}m"
    st.metric(
        label="Uptime",
        value=uptime_str,
        delta=None
    )

st.markdown("---")

# Charts Row
chart_col1, chart_col2 = st.columns([2, 1])

with chart_col1:
    st.subheader("📈 Equity Curve")
    
    # Dummy data (ganti dengan data real dari bot)
    dates = pd.date_range(start=datetime.now() - pd.Timedelta(days=7), periods=100, freq='H')
    equity = 10000 + np.cumsum(np.random.randn(100) * 50)
    
    fig_equity = go.Figure()
    fig_equity.add_trace(go.Scatter(
        x=dates,
        y=equity,
        mode='lines',
        name='Equity',
        line=dict(color='#00CC96', width=2),
        fill='tozeroy'
    ))
    fig_equity.update_layout(
        height=400,
        xaxis_title="Time",
        yaxis_title="Equity ($)",
        template="plotly_dark",
        hovermode='x unified'
    )
    st.plotly_chart(fig_equity, use_container_width=True)

with chart_col2:
    st.subheader("🎭 Market Regime")
    
    # Gauge Chart untuk Market Regime berdasarkan Adaptive Mode
    regime_map = {
        "SNIPER": {"value": 80, "color": "#00CC96", "label": "High Vol"},
        "ACTIVE": {"value": 50, "color": "#636EFA", "label": "Normal"},
        "AGGRESSIVE": {"value": 20, "color": "#EF553B", "label": "Low Vol"},
        "DEFENSIVE": {"value": 10, "color": "#FFA15A", "label": "Risk Off"},
        "NEWS_BLACKOUT": {"value": 0, "color": "#FF0000", "label": "News Event"}
    }
    
    current_regime = bot_status.get("mode", "ACTIVE")
    regime_data = regime_map.get(current_regime, regime_map["ACTIVE"])
    
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=regime_data["value"],
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': f"<b>{regime_data['label']}</b>", 'font': {'size': 24}},
        gauge={
            'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "white"},
            'bar': {'color': regime_data["color"]},
            'bgcolor': "black",
            'borderwidth': 2,
            'bordercolor': "white",
            'steps': [
                {'range': [0, 20], 'color': 'rgba(255,0,0,0.3)'},
                {'range': [20, 50], 'color': 'rgba(255,165,0,0.3)'},
                {'range': [50, 80], 'color': 'rgba(0,255,0,0.3)'}
            ],
        }
    ))
    fig_gauge.update_layout(height=400, paper_bgcolor="black", font={'color': "white"})
    st.plotly_chart(fig_gauge, use_container_width=True)

# Open Positions Table
st.subheader("📊 Open Positions")

# Dummy positions data (ganti dengan data real)
positions_data = {
    "Symbol": ["BTCUSDT", "ETHUSDT"],
    "Side": ["LONG", "SHORT"],
    "Entry Price": [42500.0, 2280.0],
    "Current Price": [42650.0, 2275.0],
    "Size": [0.1, 1.5],
    "PnL ($)": [15.0, -7.5],
    "PnL (%)": [0.35, -0.33]
}

df_positions = pd.DataFrame(positions_data)

# Styling table
st.dataframe(
    df_positions.style.format({
        "Entry Price": "${:.2f}",
        "Current Price": "${:.2f}",
        "PnL ($)": "${:.2f}",
        "PnL (%)": "{:.2f}%"
    }).applymap(
        lambda x: 'color: #00CC96' if x > 0 else ('color: #EF553B' if x < 0 else ''),
        subset=["PnL ($)", "PnL (%)"]
    ),
    use_container_width=True,
    height=300
)

# Recent Trades Log
st.subheader("📜 Recent Activity")

# Dummy log data
log_entries = [
    {"time": datetime.now().strftime("%H:%M:%S"), "level": "INFO", "message": "Mode changed to SNIPER"},
    {"time": (datetime.now() - pd.Timedelta(minutes=5)).strftime("%H:%M:%S"), "level": "INFO", "message": "Opened LONG BTCUSDT @ 42500"},
    {"time": (datetime.now() - pd.Timedelta(minutes=15)).strftime("%H:%M:%S"), "level": "WARNING", "message": "High volatility detected"},
    {"time": (datetime.now() - pd.Timedelta(minutes=30)).strftime("%H:%M:%S"), "level": "INFO", "message": "Closed ETHUSDT position +$25.50"},
]

for log in log_entries:
    color = "#00CC96" if log["level"] == "INFO" else ("#FFA15A" if log["level"] == "WARNING" else "#EF553B")
    st.markdown(
        f"<div style='background-color: #1e1e1e; padding: 10px; margin: 5px 0; border-left: 4px solid {color}; border-radius: 5px;'>"
        f"<span style='color: gray; font-size: 12px;'>{log['time']}</span> "
        f"<span style='color: {color}; font-weight: bold;'>[{log['level']}]</span> "
        f"<span style='color: white;'>{log['message']}</span>"
        f"</div>",
        unsafe_allow_html=True
    )

# Footer
st.markdown("---")
st.caption("MEXC Scalper V5.2 | Dashboard running on Streamlit | Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

# Auto-refresh (optional)
# time.sleep(5)
# st.rerun()
