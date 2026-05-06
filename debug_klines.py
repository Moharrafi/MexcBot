import json
from mexc_xaut_botV2 import MEXCFuturesClient

def test_klines():
    c = MEXCFuturesClient("", "")
    # Test BTC_USDT manual price vs candles
    print("Testing BTC_USDT 3m Klines...")
    df = c.get_klines("BTC_USDT", "3m", limit=50)
    if df is not None:
        print(f"Success! Found {len(df)} candles.")
        print(df.tail(2))
    else:
        print("Failed to get BTC_USDT klines.")

if __name__ == "__main__":
    test_klines()
