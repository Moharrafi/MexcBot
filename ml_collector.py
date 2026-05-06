"""
ML Training Data Collector v2 — MEXC Scalper V4
Improvements over v1:
  - Multi-timeframe features: 1H trend + 15m HTF bias
  - Candle pattern alignment (pinbar, engulfing, doji, liq sweep)
  - Market structure (HH/HL vs LH/LL)
  - Squeeze momentum direction alignment
  - Volume spike flag

Run: python ml_collector.py
Output: ml_training_data.csv
"""

import sys, os, time
import numpy as np
import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mexc_scalperV4 import ScalperTA, CONFIG

SYMBOLS = [
    "BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "BNB_USDT",
    "DOGE_USDT", "ADA_USDT", "AVAX_USDT", "SUI_USDT", "PEPE_USDT",
    "WIF_USDT", "HYPE_USDT", "OP_USDT", "ARB_USDT", "LTC_USDT",
    "LINK_USDT", "TON_USDT", "TRX_USDT", "DOT_USDT", "UNI_USDT",
    "XAUT_USDT", "NEAR_USDT", "INJ_USDT", "SEI_USDT", "JUP_USDT",
]

MEXC_BASE     = "https://contract.mexc.com"
LOOKBACK_DAYS = 180
REQUEST_DELAY = 0.25
BATCH         = 1500

TF_INFO = {
    "5m":  ("Min5",  300),
    "15m": ("Min15", 900),
    "1h":  ("Min60", 3600),
}

CFG = CONFIG.copy()
ta  = ScalperTA(CFG)

OUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ml_training_data.csv")


# ── Data fetching ─────────────────────────────────────────────────────────────

def _fetch_batch(symbol: str, interval: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    try:
        r = requests.get(
            f"{MEXC_BASE}/api/v1/contract/kline/{symbol}",
            params={"interval": interval, "start": start_ts, "end": end_ts},
            timeout=15,
        )
        d = r.json()
        if not d.get("success") or not d.get("data"):
            return pd.DataFrame()
        df = pd.DataFrame(d["data"], columns=["time","open","close","high","low","vol","amount"])
        df.rename(columns={"vol": "volume"}, inplace=True)
        df["open_time"] = pd.to_datetime(df["time"], unit="s")
        df.set_index("open_time", inplace=True)
        for col in ["open","close","high","low","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_index()
    except Exception as e:
        print(f"    fetch error: {e}")
        return pd.DataFrame()


def fetch_tf(symbol: str, tf: str, label: str = "") -> pd.DataFrame:
    interval, tf_sec = TF_INFO[tf]
    now          = int(time.time())
    start_global = now - LOOKBACK_DAYS * 86400
    end_ts       = now
    frames       = []

    tag = label or tf
    print(f"    {tag}...", end="", flush=True)
    while end_ts > start_global:
        start_ts = max(end_ts - BATCH * tf_sec, start_global)
        df = _fetch_batch(symbol, interval, start_ts, end_ts)
        if df.empty:
            break
        frames.append(df)
        end_ts = start_ts - tf_sec
        time.sleep(REQUEST_DELAY)

    if not frames:
        print(" FAILED")
        return pd.DataFrame()

    combined = pd.concat(frames).sort_index()
    combined = combined[~combined.index.duplicated(keep="first")]
    print(f" {len(combined):,} candles")
    return combined


# ── Multi-TF merge ────────────────────────────────────────────────────────────

def compute_and_merge(df_5m: pd.DataFrame, df_15m: pd.DataFrame, df_1h: pd.DataFrame) -> pd.DataFrame:
    """Compute indicators on all TFs and merge HTF cols into 5m DataFrame."""

    # Compute 5m
    df = ta.compute(df_5m.copy())
    df.dropna(inplace=True)

    # Compute 1H — keep only key columns for merge
    if not df_1h.empty and len(df_1h) >= 50:
        dh = ta.compute(df_1h.copy()).dropna()
        dh = dh[["st_dir", "adx", "roc", "dema_fast", "dema_slow"]].rename(columns={
            "st_dir":    "st_dir_1h",
            "adx":       "adx_1h",
            "roc":       "roc_1h",
            "dema_fast": "dema_fast_1h",
            "dema_slow": "dema_slow_1h",
        })
        df = pd.merge_asof(
            df.reset_index().sort_values("open_time"),
            dh.reset_index().sort_values("open_time"),
            on="open_time", direction="backward",
        ).set_index("open_time")
    else:
        for col in ["st_dir_1h", "adx_1h", "roc_1h"]:
            df[col] = 0.0

    # Compute 15m — keep key columns
    if not df_15m.empty and len(df_15m) >= 50:
        dm = ta.compute(df_15m.copy()).dropna()
        dm = dm[["st_dir", "adx"]].rename(columns={
            "st_dir": "st_dir_15m",
            "adx":    "adx_15m",
        })
        df = pd.merge_asof(
            df.reset_index().sort_values("open_time"),
            dm.reset_index().sort_values("open_time"),
            on="open_time", direction="backward",
        ).set_index("open_time")
    else:
        df["st_dir_15m"] = 0.0
        df["adx_15m"]    = 0.0

    df.dropna(subset=["st_dir_1h", "st_dir_15m"], inplace=True)
    return df


# ── Entry simulation ──────────────────────────────────────────────────────────

def simulate(df: pd.DataFrame, symbol: str) -> list:
    adx_min  = CFG.get("SCAN_MIN_ADX", 22)
    sl_mult  = CFG.get("ATR_SL_MULT",  1.0)
    tp1_mult = CFG.get("ATR_TP1_MULT", 2.0)
    max_hold = 50

    closes = df["close"].values
    highs  = df["high"].values
    lows   = df["low"].values
    n      = len(df)

    records = []
    i = 0
    while i < n - max_hold:
        row = df.iloc[i]

        st_dir    = float(row.get("st_dir",    0))
        adx       = float(row.get("adx",       0))
        atr       = float(row.get("atr",       0))
        roc       = float(row.get("roc",       0))
        cvd_trend = float(row.get("cvd_trend", 0))

        if st_dir == 0 or adx < adx_min or atr <= 0:
            i += 1; continue

        roc_ok = (roc > 0) if st_dir == 1 else (roc < 0)
        cvd_ok = (cvd_trend > 0) if st_dir == 1 else (cvd_trend < 0)
        if not (roc_ok or cvd_ok):
            i += 1; continue

        is_long = st_dir == 1
        entry   = closes[i]
        sl      = entry - atr * sl_mult  if is_long else entry + atr * sl_mult
        tp1     = entry + atr * tp1_mult if is_long else entry - atr * tp1_mult

        outcome      = None
        hold_candles = 0
        for j in range(i + 1, min(i + max_hold + 1, n)):
            h, l = highs[j], lows[j]
            if is_long:
                if l <= sl:  outcome = 0; break
                if h >= tp1: outcome = 1; break
            else:
                if h >= sl:  outcome = 0; break
                if l <= tp1: outcome = 1; break
            hold_candles += 1

        if outcome is None:
            i += 5; continue

        # ── Feature extraction ──
        side    = 1 if is_long else -1
        atr_pct = float(row.get("atr_pct",    0))
        willr   = float(row.get("willr",      -50))
        roc_acc = float(row.get("roc_accel",  0))
        sq_on   = int(bool(row.get("squeeze_on", False)))
        sq_mom  = float(row.get("squeeze_mom", 0))
        vol_r   = float(row.get("vol_ratio",  1))
        body_r  = float(row.get("body_ratio", 0))
        consec  = int(row.get("consec", 0))
        vwap    = float(row.get("vwap", entry))
        dema_f  = float(row.get("dema_fast", entry))
        dema_s  = float(row.get("dema_slow", entry))

        # MTF features
        st_1h   = float(row.get("st_dir_1h",  0))
        adx_1h  = float(row.get("adx_1h",     0))
        roc_1h  = float(row.get("roc_1h",     0))
        st_15m  = float(row.get("st_dir_15m", 0))
        adx_15m = float(row.get("adx_15m",    0))

        # Trend alignment: 5m + 15m + 1H all same direction as entry
        aligned_1h  = (st_1h  == st_dir)
        aligned_15m = (st_15m == st_dir)
        trend_aligned = int(aligned_1h and aligned_15m)

        # Candle pattern aligned with trade direction
        pinbar   = int(row.get("cdl_pinbar",   0))
        engulf   = int(row.get("cdl_engulfing",0))
        doji     = int(row.get("cdl_doji",     0))
        pattern_align = int(pinbar == side or engulf == side or doji == side)

        # Market structure aligned
        mkt_struct   = int(row.get("mkt_struct",  0))
        struct_align = int(mkt_struct == side)

        # Liquidity sweep aligned
        liq_sweep    = int(row.get("liq_sweep", 0))
        sweep_align  = int(liq_sweep == side)

        # Squeeze momentum direction aligned
        sq_mom_aligned = int((sq_mom > 0) == is_long)

        # Volume spike
        vol_spike = int(vol_r >= 1.5)

        # 1H DEMA cross
        dema_cross_1h = int(row.get("dema_fast_1h", 0) > row.get("dema_slow_1h", 0))

        records.append({
            "symbol":         symbol,
            # Core 5m features
            "side":           side,
            "st_dir":         int(st_dir),
            "adx":            round(adx, 2),
            "roc":            round(roc, 4),
            "roc_accel":      round(roc_acc, 5),
            "willr":          round(willr, 2),
            "atr_pct":        round(atr_pct, 4),
            "squeeze_on":     sq_on,
            "squeeze_mom":    round(sq_mom, 6),
            "sq_mom_aligned": sq_mom_aligned,
            "cvd_trend":      round(cvd_trend, 2),
            "vol_ratio":      round(vol_r, 3),
            "vol_spike":      vol_spike,
            "body_ratio":     round(body_r, 3),
            "consec":         consec,
            "dema_cross":     1 if dema_f > dema_s else -1,
            "vwap_dist":      round((entry - vwap) / entry * 100, 4),
            "hour_utc":       df.index[i].hour,
            "dow":            df.index[i].dayofweek,
            # MTF features
            "st_dir_1h":      int(st_1h),
            "adx_1h":         round(adx_1h, 2),
            "roc_1h":         round(roc_1h, 4),
            "dema_cross_1h":  dema_cross_1h,
            "st_dir_15m":     int(st_15m),
            "adx_15m":        round(adx_15m, 2),
            "trend_aligned":  trend_aligned,
            # Pattern & structure
            "pattern_align":  pattern_align,
            "struct_align":   struct_align,
            "sweep_align":    sweep_align,
            # Meta
            "hold_candles":   hold_candles,
            "label":          outcome,
        })

        i += max(hold_candles, 5)

    return records


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    all_records = []

    for sym in SYMBOLS:
        print(f"\n[{sym}]")

        df_5m  = fetch_tf(sym, "5m",  "5m ")
        df_15m = fetch_tf(sym, "15m", "15m")
        df_1h  = fetch_tf(sym, "1h",  "1h ")

        if df_5m.empty or len(df_5m) < 300:
            print("  Skipped — insufficient 5m data")
            continue

        print("  Computing & merging indicators ...", end="", flush=True)
        try:
            df_merged = compute_and_merge(df_5m, df_15m, df_1h)
        except Exception as e:
            print(f" ERROR: {e}")
            continue
        print(f" {len(df_merged):,} rows")

        recs = simulate(df_merged, sym)
        wins = sum(r["label"] for r in recs)
        wr   = wins / len(recs) * 100 if recs else 0
        aligned_pct = sum(r["trend_aligned"] for r in recs) / max(len(recs), 1) * 100
        print(f"  Signals: {len(recs):,} | Win rate: {wr:.1f}% | MTF aligned: {aligned_pct:.1f}%")
        all_records.extend(recs)
        time.sleep(1)

    if not all_records:
        print("\nNo records — check connectivity or symbol names")
        return

    out = pd.DataFrame(all_records)
    out.to_csv(OUT_PATH, index=False)

    print(f"\n{'='*55}")
    print(f"Saved {len(out):,} records → {OUT_PATH}")
    print(f"Overall win rate : {out['label'].mean()*100:.1f}%")
    aligned = out[out['trend_aligned'] == 1]
    print(f"MTF-aligned only : {len(aligned):,} samples | WR {aligned['label'].mean()*100:.1f}%")
    print(f"\nPer-symbol:")
    print(out.groupby("symbol")["label"].agg(trades="count", win_rate="mean").round(3).to_string())


if __name__ == "__main__":
    main()
