#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""MEXC Scalper V5.1 — Modern Streamlit Dashboard"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime
import requests
import time
import json

st.set_page_config(
    page_title="MEXC Scalper V5.1",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
/* ── Banners ── */
.circuit-banner {
    background: #5c0000; border: 2px solid #ff4444;
    border-radius: 8px; padding: 14px; margin: 8px 0;
}
.news-banner {
    background: #3a2a00; border: 2px solid #ffaa00;
    border-radius: 8px; padding: 10px; margin: 6px 0;
}

/* ── Mobile status bar (hidden on desktop) ── */
.status-bar {
    display: none;
    background: #1a1a2e;
    border: 1px solid #2d2d4e;
    border-radius: 10px;
    padding: 10px 14px;
    margin: 0 0 12px 0;
    gap: 8px;
    align-items: center;
    flex-wrap: wrap;
}
.sb-symbol { font-weight: 700; font-size: 1rem; color: #fff; }
.sb-price  { color: #aaa; font-size: 0.88rem; }
.sb-badge  { padding: 2px 8px; border-radius: 4px; font-size: 0.72rem; font-weight: 700; }
.sb-long   { background: rgba(0,204,150,.2);  color: #00CC96; }
.sb-short  { background: rgba(239,85,59,.2);  color: #EF553B; }
.sb-neut   { background: rgba(255,255,255,.08); color: #aaa; }
.sb-sniper     { background: rgba(0,204,150,.2);  color: #00CC96; }
.sb-active     { background: rgba(99,110,250,.2); color: #636EFA; }
.sb-aggressive { background: rgba(239,85,59,.2);  color: #EF553B; }
.sb-iter   { color: #666; font-size: 0.72rem; margin-left: auto; }

/* ── Metric cards grid ── */
.metrics-row {
    display: grid;
    grid-template-columns: repeat(6, 1fr);
    gap: 8px;
    margin: 4px 0 14px 0;
}
.m-card {
    background: #1a1a2e;
    border: 1px solid #2d2d4e;
    border-radius: 10px;
    padding: 14px 8px;
    text-align: center;
}
.m-label {
    font-size: 0.67rem;
    color: #888;
    text-transform: uppercase;
    letter-spacing: .05em;
    margin-bottom: 5px;
}
.m-value {
    font-size: 1.15rem;
    font-weight: 700;
    color: #fff;
    line-height: 1.2;
}
.m-sub {
    font-size: 0.67rem;
    color: #666;
    margin-top: 3px;
}

/* ── Mobile ── */
@media (max-width: 768px) {
    .status-bar { display: flex; }

    .block-container {
        padding-left: .75rem !important;
        padding-right: .75rem !important;
        padding-top: .5rem !important;
        max-width: 100% !important;
    }

    /* Stack all Streamlit columns */
    [data-testid="column"] {
        width: 100% !important;
        flex: 1 1 100% !important;
        min-width: 100% !important;
    }

    /* Metrics 2-per-row */
    .metrics-row { grid-template-columns: repeat(2, 1fr); gap: 6px; }
    .m-value { font-size: 1rem; }
    .m-card  { padding: 11px 8px; }

    h1 {
        font-size: 1.1rem !important;
        line-height: 1.3 !important;
        white-space: normal !important;
        word-break: break-word !important;
        overflow-wrap: break-word !important;
    }
    h2, h3 { font-size: 1rem !important; }

    /* Tabs scrollable */
    [data-testid="stTabs"] > div:first-child {
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch;
        scrollbar-width: none;
    }
    [data-testid="stTabs"] > div:first-child::-webkit-scrollbar { display: none; }
    button[data-baseweb="tab"] {
        font-size: 0.72rem !important;
        padding: 6px 8px !important;
        white-space: nowrap !important;
    }

    /* Touch-friendly controls */
    [data-testid="stButton"] > button { min-height: 44px !important; }
    [data-testid="stSlider"] { padding: 8px 0 !important; }

    /* Scrollable tables */
    [data-testid="stDataFrame"] { overflow-x: auto !important; }
    .stDataFrame > div { overflow-x: auto !important; -webkit-overflow-scrolling: touch; }

    /* Reduce divider spacing */
    hr { margin: .5rem 0 !important; }
}

@media (max-width: 400px) {
    .m-value { font-size: .88rem; }
    .metrics-row { gap: 4px; }
}
</style>
""", unsafe_allow_html=True)

BOT_URL = "http://localhost:5003"

def fetch(path: str, timeout: int = 3):
    try:
        r = requests.get(f"{BOT_URL}{path}", timeout=timeout)
        return r.json()
    except Exception:
        return None

def post(path: str, data: dict = {}):
    try:
        requests.post(f"{BOT_URL}{path}", json=data, timeout=5)
        return True
    except Exception:
        return False

# ── Fetch semua data ──
status = fetch("/api/status")
scanner_data = fetch("/api/scanner") or []
journal_data = fetch("/api/journal") or {"trades": []}
cfg_data = fetch("/api/config") or {}
trades = journal_data.get("trades", [])
is_online = status is not None

# ── Sidebar ──
st.sidebar.title("⚙️ Kontrol Bot V5.1")
st.sidebar.markdown("---")

if is_online:
    st.sidebar.success("✅ Bot Online")
    regime = status.get("adaptive_regime", "ACTIVE")
    adaptive_on = status.get("adaptive_mode", False)
    regime_colors = {"SNIPER": "🟢", "ACTIVE": "🔵", "AGGRESSIVE": "🔴"}
    st.sidebar.info(f"{regime_colors.get(regime,'⚪')} Regime: **{regime}**")
    st.sidebar.caption(f"Adaptive Mode: {'ON' if adaptive_on else 'OFF'}")
else:
    st.sidebar.error("❌ Bot Offline")

st.sidebar.markdown("---")
st.sidebar.subheader("🎛️ Config Live")

if is_online:
    # Sync session state dari API setiap kali page di-refresh (session baru)
    # atau saat bot restart (iteration rendah / berubah drastis)
    api_adaptive = status.get("adaptive_mode", False)
    bot_iter = status.get("iteration", 0)
    prev_iter = st.session_state.get("_last_bot_iter", 0)
    bot_restarted = bot_iter < prev_iter or (prev_iter == 0 and bot_iter <= 5)
    st.session_state["_last_bot_iter"] = bot_iter

    if "adaptive_synced" not in st.session_state or bot_restarted:
        st.session_state["adaptive_toggle_key"] = api_adaptive
        st.session_state["adaptive_synced"] = True

    def _on_adaptive_change():
        new_val = st.session_state["adaptive_toggle_key"]
        post("/api/config", {"ADAPTIVE_MODE": new_val})

    st.sidebar.toggle(
        "Adaptive Mode",
        key="adaptive_toggle_key",
        on_change=_on_adaptive_change
    )
    # Indikator sinkronisasi: tampilkan jika dashboard vs bot tidak sinkron
    dash_val = st.session_state.get("adaptive_toggle_key", False)
    if dash_val != api_adaptive:
        st.sidebar.caption("⏳ Menyimpan...")

st.sidebar.markdown("---")
st.sidebar.subheader("🚨 Emergency")
if st.sidebar.button("⛔ CLOSE ALL POSITIONS", type="primary"):
    post("/api/close_all")
    st.sidebar.warning("Semua posisi ditutup!")
    st.rerun()

if st.sidebar.button("🔄 Refresh Data"):
    st.rerun()

# ── Main Content ──
st.title("⚡ MEXC Scalper V5.1")

if not is_online:
    st.error("Bot tidak bisa dijangkau di `http://localhost:5003`. Jalankan: `python mexc_scalperV5.1.py --dashboard`")
    st.stop()

@st.fragment(run_every="15s")
def live_panel():
    # Fetch data fresh di dalam fragment — tidak ganggu halaman lain
    s      = fetch("/api/status") or {}
    sc     = fetch("/api/scanner") or []
    jdata  = fetch("/api/journal") or {"trades": []}
    trades = jdata.get("trades", [])

    # ── Banners ──
    if s.get("circuit_breaker"):
        st.markdown(
            f'<div class="circuit-banner">🚨 <b>CIRCUIT BREAKER AKTIF</b> — '
            f'{s.get("circuit_reason","?")} [{s.get("circuit_type","")}]</div>',
            unsafe_allow_html=True
        )
    if s.get("news_blackout"):
        st.markdown(
            '<div class="news-banner">📰 <b>NEWS BLACKOUT AKTIF</b> — Entry diblokir sementara</div>',
            unsafe_allow_html=True
        )

    # ── Mobile status bar ──
    sig     = s.get("last_signal", "NEUTRAL")
    regime  = s.get("adaptive_regime", "ACTIVE")
    sig_cls = "sb-long" if sig == "LONG" else "sb-short" if sig == "SHORT" else "sb-neut"
    reg_cls = f"sb-{regime.lower()}"
    ws_dot  = "🟢" if s.get("ws_alive") else "🔴"
    dry_badge = "🟡 DRY" if s.get("dry_run") else "🔴 LIVE"
    st.markdown(f"""
    <div class="status-bar">
        <span class="sb-symbol">{s.get('symbol','?')}</span>
        <span class="sb-price">${float(s.get('price',0)):.4f}</span>
        <span class="sb-badge {sig_cls}">{sig}</span>
        <span class="sb-badge {reg_cls}">{regime}</span>
        <span class="sb-iter">{ws_dot} #{s.get('iteration',0)} · {dry_badge}</span>
    </div>
    """, unsafe_allow_html=True)

    # ── Metrics ──
    balance      = float(s.get("balance", 0))
    peak         = float(s.get("peak_balance", 0))
    daily_pnl    = float(s.get("daily_pnl", 0))
    total_pnl    = float(s.get("total_pnl", 0))
    win_rate     = float(s.get("win_rate", 0))
    drawdown     = float(s.get("drawdown", 0))
    total_trades = int(s.get("total_trades", 0))

    pnl_d_col = "#00CC96" if daily_pnl >= 0 else "#EF553B"
    pnl_t_col = "#00CC96" if total_pnl >= 0 else "#EF553B"
    dd_col    = "#EF553B" if drawdown > 5 else "#FFA15A" if drawdown > 2 else "#aaa"
    mode_col  = "#FFA15A" if s.get("dry_run") else "#EF553B"
    mode_lbl  = "DRY RUN" if s.get("dry_run") else "LIVE"

    st.markdown(f"""
    <div class="metrics-row">
      <div class="m-card">
        <div class="m-label">Balance</div>
        <div class="m-value">${balance:.2f}</div>
        <div class="m-sub">peak ${peak:.2f}</div>
      </div>
      <div class="m-card">
        <div class="m-label">Daily PnL</div>
        <div class="m-value" style="color:{pnl_d_col}">${daily_pnl:+.2f}</div>
      </div>
      <div class="m-card">
        <div class="m-label">Total PnL</div>
        <div class="m-value" style="color:{pnl_t_col}">${total_pnl:+.2f}</div>
      </div>
      <div class="m-card">
        <div class="m-label">Win Rate</div>
        <div class="m-value">{win_rate:.1f}%</div>
        <div class="m-sub">{total_trades} trades</div>
      </div>
      <div class="m-card">
        <div class="m-label">Drawdown</div>
        <div class="m-value" style="color:{dd_col}">{drawdown:.1f}%</div>
      </div>
      <div class="m-card">
        <div class="m-label">Mode</div>
        <div class="m-value" style="color:{mode_col}; font-size:.88rem">{mode_lbl}</div>
        <div class="m-sub">iter #{s.get('iteration',0)}</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Charts ──
    ch1, ch2 = st.columns([2, 1])

    with ch1:
        st.subheader("📈 Equity Curve")
        if trades:
            df_t = pd.DataFrame(trades)
            pnl_col  = "pnl"       if "pnl"       in df_t.columns else None
            time_col = "closed_at" if "closed_at" in df_t.columns else None
            if pnl_col and time_col:
                df_t[pnl_col]  = pd.to_numeric(df_t[pnl_col], errors="coerce").fillna(0)
                df_t = df_t[df_t[pnl_col] != 0].copy()
                df_t[time_col] = pd.to_datetime(df_t[time_col], errors="coerce")
                df_t = df_t.sort_values(time_col)
                df_t["equity"] = (balance - df_t[pnl_col].sum()) + df_t[pnl_col].cumsum()
                fig_eq = go.Figure()
                fig_eq.add_trace(go.Scatter(
                    x=df_t[time_col], y=df_t["equity"],
                    mode="lines", name="Equity",
                    line=dict(color="#00CC96", width=2),
                    fill="tozeroy", fillcolor="rgba(0,204,150,0.1)"
                ))
                fig_eq.update_layout(height=300, template="plotly_dark",
                    xaxis_title="Waktu", yaxis_title="Equity ($)", hovermode="x unified",
                    margin=dict(l=10, r=10, t=10, b=40))
                st.plotly_chart(fig_eq, use_container_width=True)
            else:
                st.info("Data trade tidak lengkap untuk equity curve.")
        else:
            dates     = pd.date_range(end=datetime.now(), periods=50, freq="h")
            dummy_eq  = balance + np.cumsum(np.random.randn(50) * 0.5)
            fig_eq    = go.Figure()
            fig_eq.add_trace(go.Scatter(x=dates, y=dummy_eq, mode="lines",
                name="Equity (demo)", line=dict(color="#636EFA", width=2, dash="dash")))
            fig_eq.update_layout(height=300, template="plotly_dark",
                margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig_eq, use_container_width=True)
            st.caption("⚠️ Menampilkan data demo — belum ada trade.")

    with ch2:
        st.subheader("🎭 Market Regime")
        regime_map = {
            "SNIPER":     {"value": 85, "color": "#00CC96", "label": "SNIPER<br>High Volatility"},
            "ACTIVE":     {"value": 50, "color": "#636EFA", "label": "ACTIVE<br>Normal"},
            "AGGRESSIVE": {"value": 15, "color": "#EF553B", "label": "AGGRESSIVE<br>Low Volatility"},
        }
        rd = regime_map.get(s.get("adaptive_regime", "ACTIVE"), regime_map["ACTIVE"])
        fig_g = go.Figure(go.Indicator(
            mode="gauge+number", value=rd["value"],
            title={"text": f"<b>{rd['label']}</b>", "font": {"size": 16, "color": "white"}},
            gauge={
                "axis": {"range": [0, 100], "tickcolor": "white"},
                "bar": {"color": rd["color"]}, "bgcolor": "#111",
                "borderwidth": 2, "bordercolor": "#444",
                "steps": [
                    {"range": [0,  33], "color": "rgba(239,85,59,0.15)"},
                    {"range": [33, 66], "color": "rgba(99,110,250,0.15)"},
                    {"range": [66,100], "color": "rgba(0,204,150,0.15)"},
                ],
            }
        ))
        fig_g.update_layout(height=300, paper_bgcolor="#111", font={"color": "white"},
            margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig_g, use_container_width=True)
        st.caption("✅ Adaptive ON" if s.get("adaptive_mode") else "⚪ Adaptive OFF")

    # ── Open Positions ──
    st.subheader("📊 Posisi Terbuka")
    positions = s.get("positions", [])
    if positions:
        df_pos = pd.DataFrame(positions)
        dcols  = [c for c in ["symbol","side","entry_price","current_price",
                               "quantity","pnl_live","sl_price","tp1_price","opened_at"]
                  if c in df_pos.columns]
        fmt    = {c: "${:.4f}" for c in ["entry_price","current_price","sl_price","tp1_price","pnl_live"]
                  if c in dcols}
        styled = df_pos[dcols].style.format(fmt)
        if "pnl_live" in dcols:
            styled = styled.applymap(
                lambda x: "color:#00CC96" if isinstance(x,(int,float)) and x > 0
                          else ("color:#EF553B" if isinstance(x,(int,float)) and x < 0 else ""),
                subset=["pnl_live"]
            )
        st.dataframe(styled, use_container_width=True, height=200)
    else:
        st.info("Tidak ada posisi terbuka.")

    st.markdown("---")

    # ── Scanner + Journal ──
    sc1, sc2 = st.columns(2)

    with sc1:
        st.subheader("🔍 Scanner Top Coins")
        if sc:
            df_sc   = pd.DataFrame(sc)
            show_sc = [c for c in ["symbol","signal","composite","bull_score","bear_score",
                                    "atr_pct","adx","vol_ratio","price","momentum"]
                       if c in df_sc.columns]
            if show_sc:
                sort_by = "composite" if "composite" in show_sc else show_sc[2]
                df_sc_s = df_sc[show_sc].sort_values(sort_by, ascending=False)
                fmt_sc  = {c: f for c, f in [("composite","{:.1f}"),("atr_pct","{:.2f}%"),
                                               ("adx","{:.1f}"),("price","${:.4f}"),
                                               ("vol_ratio","{:.2f}x")]
                           if c in show_sc}
                st.dataframe(df_sc_s.head(15).style.format(fmt_sc),
                             use_container_width=True, height=350)
        else:
            st.info("Scanner belum ada data.")

    with sc2:
        st.subheader("📜 Journal — 20 Trade Terakhir")
        if trades:
            df_j   = pd.DataFrame(trades)
            show_j = [c for c in ["closed_at","symbol","side","entry_price",
                                   "close_price","pnl","close_reason"]
                      if c in df_j.columns]
            for nc in ["entry_price","close_price","pnl"]:
                if nc in df_j.columns:
                    df_j[nc] = pd.to_numeric(df_j[nc], errors="coerce")
            df_js   = df_j[show_j].tail(20).iloc[::-1]
            fmt_j   = {c: "${:.4f}" for c in ["entry_price","close_price","pnl"] if c in show_j}
            styled_j = df_js.style.format(fmt_j)
            if "pnl" in show_j:
                styled_j = styled_j.applymap(
                    lambda x: "color:#00CC96" if isinstance(x,(int,float)) and x > 0
                              else ("color:#EF553B" if isinstance(x,(int,float)) and x < 0 else ""),
                    subset=["pnl"]
                )
            st.dataframe(styled_j, use_container_width=True, height=350)
        else:
            st.info("Journal kosong — belum ada trade selesai.")

    # ── Footer live ──
    ws_icon    = "🟢" if s.get("ws_alive") else "🔴"
    mode_label = "🟡 DRY RUN" if s.get("dry_run") else "🔴 LIVE TRADING"
    st.caption(
        f"Symbol: `{s.get('symbol','?')}` | Iter #{s.get('iteration',0)} | "
        f"WS: {ws_icon} | {mode_label} | {datetime.now().strftime('%H:%M:%S')} "
        f"— live update setiap 15s"
    )

live_panel()

# ── Settings Panel ──
st.markdown("---")
with st.expander("⚙️ Pengaturan Konfigurasi Bot", expanded=False):
    if not cfg_data:
        st.warning("Tidak bisa mengambil config dari bot.")
    else:
        st.caption("Perubahan langsung aktif tanpa restart bot.")

        tab_risk, tab_entry, tab_exit, tab_adaptive, tab_news, tab_advanced = st.tabs([
            "💰 Risk", "🎯 Entry", "🚪 Exit", "🤖 Adaptive", "📰 News", "🔧 Lainnya"
        ])

        # ── Tab Risk ──
        with tab_risk:
            c1, c2, c3 = st.columns(3)
            with c1:
                dry_run = st.toggle("DRY RUN Mode", value=bool(cfg_data.get("DRY_RUN", True)))
            with c2:
                leverage = st.number_input("Leverage", min_value=1, max_value=100,
                    value=int(cfg_data.get("LEVERAGE", 10)))
            with c3:
                max_trades = st.number_input("Max Open Trades", min_value=1, max_value=10,
                    value=int(cfg_data.get("MAX_OPEN_TRADES", 1)))

            c4, c5, c6 = st.columns(3)
            with c4:
                risk_pct = st.slider("Risk Per Trade (%)", 1, 30,
                    value=int(round(float(cfg_data.get("RISK_PER_TRADE", 0.08)) * 100)),
                    help="% dari balance yang dirisiko per trade")
            with c5:
                max_daily_loss = st.slider("Max Daily Loss (%)", 1, 50,
                    value=int(round(float(cfg_data.get("MAX_DAILY_LOSS_PCT", 0.15)) * 100)),
                    help="Circuit breaker aktif jika daily loss melebihi ini")
            with c6:
                max_drawdown = st.slider("Max Drawdown (%)", 5, 60,
                    value=int(round(float(cfg_data.get("MAX_DRAWDOWN_PCT", 0.30)) * 100)))

            if st.button("💾 Simpan Risk Settings", key="save_risk"):
                payload = {
                    "DRY_RUN": dry_run,
                    "LEVERAGE": leverage,
                    "MAX_OPEN_TRADES": max_trades,
                    "RISK_PER_TRADE": risk_pct / 100,
                    "MAX_DAILY_LOSS_PCT": max_daily_loss / 100,
                    "MAX_DRAWDOWN_PCT": max_drawdown / 100,
                }
                if post("/api/config", payload):
                    st.success("✅ Risk settings disimpan!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("Gagal menyimpan — bot tidak merespons.")

        # ── Tab Entry ──
        with tab_entry:
            c1, c2 = st.columns(2)
            with c1:
                symbol = st.text_input("Symbol",
                    value=cfg_data.get("SYMBOL", "XAUT_USDT"),
                    help="Format: BTC_USDT, ETH_USDT, XAUT_USDT")
                min_bull = st.number_input("Min Bull Score", min_value=1, max_value=30,
                    value=int(cfg_data.get("MIN_BULL_SCORE", 15)))
                min_bear = st.number_input("Min Bear Score", min_value=1, max_value=30,
                    value=int(cfg_data.get("MIN_BEAR_SCORE", 15)))
            with c2:
                adx_min = st.slider("ADX Min Threshold", 10, 60,
                    value=int(cfg_data.get("ADX_MIN_THRESHOLD", 25)),
                    help="Entry hanya jika ADX di atas nilai ini")
                min_atr = st.slider("Min ATR% (filter flat)", 0.0, 5.0,
                    value=float(cfg_data.get("MIN_ATR_PCT", 0.3)),
                    step=0.1,
                    help="Skip entry jika instrumen terlalu flat")
                max_atr = st.slider("Max ATR% Entry", 1.0, 10.0,
                    value=float(cfg_data.get("MAX_ATR_PCT_ENTRY", 4.0)),
                    step=0.5,
                    help="Skip entry jika volatilitas terlalu ekstrem")

            entry_mode = st.selectbox("Entry Mode",
                options=["PULLBACK", "BREAKOUT", "IMMEDIATE"],
                index=["PULLBACK", "BREAKOUT", "IMMEDIATE"].index(
                    cfg_data.get("ENTRY_MODE", "PULLBACK")
                ) if cfg_data.get("ENTRY_MODE", "PULLBACK") in ["PULLBACK", "BREAKOUT", "IMMEDIATE"] else 0
            )

            if st.button("💾 Simpan Entry Settings", key="save_entry"):
                payload = {
                    "SYMBOL": symbol.strip().upper(),
                    "MIN_BULL_SCORE": min_bull,
                    "MIN_BEAR_SCORE": min_bear,
                    "ADX_MIN_THRESHOLD": adx_min,
                    "MIN_ATR_PCT": float(min_atr),
                    "MAX_ATR_PCT_ENTRY": float(max_atr),
                    "ENTRY_MODE": entry_mode,
                }
                if post("/api/config", payload):
                    st.success("✅ Entry settings disimpan!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("Gagal menyimpan.")

        # ── Tab Exit ──
        with tab_exit:
            c1, c2 = st.columns(2)
            with c1:
                atr_sl = st.slider("ATR SL Multiplier", 0.5, 3.0,
                    value=float(cfg_data.get("ATR_SL_MULT", 1.0)),
                    step=0.1, help="Stop loss = ATR × multiplier")
                atr_tp1 = st.slider("ATR TP1 Multiplier", 1.0, 8.0,
                    value=float(cfg_data.get("ATR_TP1_MULT", 2.5)), step=0.1)
                atr_tp2 = st.slider("ATR TP2 Multiplier", 2.0, 10.0,
                    value=float(cfg_data.get("ATR_TP2_MULT", 4.0)), step=0.1)
            with c2:
                use_trailing = st.toggle("Trailing Stop",
                    value=bool(cfg_data.get("USE_TRAILING_STOP", True)))
                trail_act = st.slider("Trail Activation (%)", 0.1, 5.0,
                    value=round(float(cfg_data.get("TRAIL_ACTIVATION_PCT", 0.015)) * 100, 2),
                    step=0.1, help="Aktifkan trailing setelah profit X%")
                trail_dist = st.slider("Trail Distance (%)", 0.1, 3.0,
                    value=round(float(cfg_data.get("TRAIL_DISTANCE_PCT", 0.010)) * 100, 2),
                    step=0.1)

            tp1_partial = st.toggle("Partial Close di TP1",
                value=bool(cfg_data.get("TP1_PARTIAL_CLOSE", True)))
            tp1_close_pct = st.slider("% Posisi Ditutup di TP1", 10, 100,
                value=int(cfg_data.get("TP1_CLOSE_PCT", 40)),
                disabled=not tp1_partial)

            if st.button("💾 Simpan Exit Settings", key="save_exit"):
                payload = {
                    "ATR_SL_MULT": atr_sl,
                    "ATR_TP1_MULT": atr_tp1,
                    "ATR_TP2_MULT": atr_tp2,
                    "USE_TRAILING_STOP": use_trailing,
                    "TRAIL_ACTIVATION_PCT": float(trail_act) / 100,
                    "TRAIL_DISTANCE_PCT": float(trail_dist) / 100,
                    "TP1_PARTIAL_CLOSE": tp1_partial,
                    "TP1_CLOSE_PCT": tp1_close_pct,
                }
                if post("/api/config", payload):
                    st.success("✅ Exit settings disimpan!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("Gagal menyimpan.")

        # ── Tab Adaptive ──
        with tab_adaptive:
            st.info("Adaptive mode otomatis sesuaikan ADX, SL, dan risk berdasarkan kondisi volatilitas pasar.")
            c1, c2, c3 = st.columns(3)
            with c1:
                adaptive_en = st.toggle("Aktifkan Adaptive Mode",
                    value=bool(cfg_data.get("ADAPTIVE_MODE", False)))
            with c2:
                high_vol_thr = st.slider("ATR% → SNIPER threshold", 0.5, 5.0,
                    value=float(cfg_data.get("HIGH_VOL_ATR_PCT", 1.5)), step=0.1)
            with c3:
                low_vol_thr = st.slider("ATR% → AGGRESSIVE threshold", 0.1, 2.0,
                    value=float(cfg_data.get("LOW_VOL_ATR_PCT", 0.5)), step=0.1)

            st.markdown("**Efek per Regime:**")
            st.markdown("""
| Regime | ATR% | Risk | ADX Min | SL Mult | Score |
|---|---|---|---|---|---|
| 🟢 SNIPER | ≥ threshold | ×0.5 | +5 | 1.5× | +3 |
| 🔵 ACTIVE | antara | ×1.0 | normal | 1.0× | normal |
| 🔴 AGGRESSIVE | ≤ threshold | ×1.3 | -5 | 0.8× | -2 |
""")
            if st.button("💾 Simpan Adaptive Settings", key="save_adaptive"):
                payload = {
                    "ADAPTIVE_MODE": adaptive_en,
                    "HIGH_VOL_ATR_PCT": high_vol_thr,
                    "LOW_VOL_ATR_PCT": low_vol_thr,
                }
                if post("/api/config", payload):
                    st.success("✅ Adaptive settings disimpan!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("Gagal menyimpan.")

        # ── Tab News ──
        with tab_news:
            st.info("Masukkan jadwal event berita besar. Bot tidak akan entry dalam window waktu tersebut.")
            current_news = cfg_data.get("NEWS_BLACKOUT", [])

            news_json = st.text_area(
                "NEWS_BLACKOUT (JSON array)",
                value=json.dumps(current_news, indent=2),
                height=200,
                help='Contoh: [{"time": "13:30", "event": "CPI", "margin_min": 60}]'
            )
            if st.button("💾 Simpan News Blackout", key="save_news"):
                try:
                    parsed = json.loads(news_json)
                    if not isinstance(parsed, list):
                        st.error("Harus berupa JSON array [ ... ]")
                    else:
                        if post("/api/config", {"NEWS_BLACKOUT": parsed}):
                            st.success(f"✅ {len(parsed)} event tersimpan!")
                            time.sleep(0.5)
                            st.rerun()
                        else:
                            st.error("Gagal menyimpan.")
                except json.JSONDecodeError as e:
                    st.error(f"JSON tidak valid: {e}")

            st.markdown("**Template event umum:**")
            st.code(json.dumps([
                {"time": "13:30", "event": "CPI/NFP (US)", "margin_min": 60},
                {"time": "19:00", "event": "FOMC (Rabu)", "margin_min": 120},
                {"time": "02:30", "event": "CPI Australia", "margin_min": 45},
            ], indent=2), language="json")

        # ── Tab Advanced ──
        with tab_advanced:
            c1, c2 = st.columns(2)
            with c1:
                loss_cooldown = st.number_input("Loss Cooldown (detik)",
                    min_value=0, max_value=3600,
                    value=int(cfg_data.get("LOSS_COOLDOWN_SEC", 300)),
                    help="Jeda setelah loss sebelum boleh entry lagi")
                poll_interval = st.number_input("Poll Interval (detik)",
                    min_value=5, max_value=300,
                    value=int(cfg_data.get("POLL_INTERVAL", 15)))
            with c2:
                scan_top_n = st.number_input("Scan Top N Coins",
                    min_value=5, max_value=200,
                    value=int(cfg_data.get("SCAN_TOP_N", 50)))
                scan_min_vol = st.number_input("Scan Min Volume (USDT)",
                    min_value=10000, max_value=10000000, step=50000,
                    value=int(cfg_data.get("SCAN_MIN_VOLUME", 300000)))

            st.markdown("**Config lengkap (read-only):**")
            safe_cfg = {k: v for k, v in cfg_data.items()
                        if k not in ("MEXC_API_KEY", "MEXC_API_SECRET")}
            st.json(safe_cfg, expanded=False)

            if st.button("💾 Simpan Advanced Settings", key="save_advanced"):
                payload = {
                    "LOSS_COOLDOWN_SEC": loss_cooldown,
                    "POLL_INTERVAL": poll_interval,
                    "SCAN_TOP_N": scan_top_n,
                    "SCAN_MIN_VOLUME": scan_min_vol,
                }
                if post("/api/config", payload):
                    st.success("✅ Advanced settings disimpan!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("Gagal menyimpan.")

st.markdown("---")
st.caption("MEXC Scalper V5.1 | Live panel auto-refreshes every 15s | Settings changes apply immediately")
