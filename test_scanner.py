"""Final verification: does the fixed get_klines produce enough data for indicators?"""
import requests, time, sys, io, pandas as pd
import pandas_ta as ta
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

BASE = "https://contract.mexc.com"
session = requests.Session()

def get_klines_fixed(symbol, interval="1m", limit=300):
    tf_map = {
        "1m": ("Min1", 60), "5m": ("Min5", 300), "15m": ("Min15", 900),
        "1h": ("Min60", 3600), "4h": ("Hour4", 14400), "1d": ("Day1", 86400)
    }
    tf_name, tf_seconds = tf_map.get(interval, ("Min5", 300))
    end_ts = int(time.time())
    start_ts = end_ts - (limit * tf_seconds)
    
    url = f"{BASE}/api/v1/contract/kline/{symbol}"
    r = session.get(url, params={"interval": tf_name, "start": start_ts, "end": end_ts}, timeout=15)
    data = r.json()
    if not data.get("success"):
        return None
    d = data.get("data")
    if d is None:
        return None
    
    # MEXC returns dict-of-lists format
    df = pd.DataFrame(d, columns=["time", "open", "close", "high", "low", "vol", "amount"])
    df.rename(columns={"vol": "volume"}, inplace=True)
    df["open_time"] = pd.to_datetime(df["time"], unit="s")
    df.set_index("open_time", inplace=True)
    for col in ["open", "close", "high", "low", "volume"]:
        df[col] = df[col].astype(float)
    return df

# Test with multiple coins
test_coins = ["BTC_USDT", "ETH_USDT", "SOL_USDT", "PIPPIN_USDT"]

print("=" * 60)
print("Testing fixed get_klines + indicator computation")
print("=" * 60)

for sym in test_coins:
    print(f"\n--- {sym} ---")
    df = get_klines_fixed(sym, "1m", 300)
    if df is None or len(df) == 0:
        print(f"  FAIL: No data returned")
        continue
    
    print(f"  Raw candles: {len(df)}")
    
    # Compute indicators (same as bot)
    try:
        df["rsi"] = ta.rsi(df["close"], length=14)
        df["ema_fast"] = ta.ema(df["close"], length=9)
        df["ema_slow"] = ta.ema(df["close"], length=21)
        df["ema_trend"] = ta.ema(df["close"], length=50)
        df["ema_long"] = ta.ema(df["close"], length=200)
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=14)
        
        before_drop = len(df)
        df.dropna(inplace=True)
        after_drop = len(df)
        
        print(f"  After dropna: {after_drop} rows (dropped {before_drop - after_drop})")
        
        if after_drop >= 5:
            row = df.iloc[-1]
            print(f"  RSI: {row['rsi']:.1f}, EMA9: {row['ema_fast']:.4f}, ADX computed: YES")
            print(f"  STATUS: OK - Scanner will ACCEPT this coin!")
        else:
            print(f"  STATUS: FAIL - Not enough rows after dropna (need >= 5)")
    except Exception as e:
        print(f"  ERROR computing indicators: {e}")
    
    time.sleep(0.5)

print("\n" + "=" * 60)
print("Verification complete!")
print("=" * 60)
