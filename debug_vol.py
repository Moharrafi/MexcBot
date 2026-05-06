import json
import os
from mexc_xaut_botV2 import MEXCFuturesClient

def debug():
    client = MEXCFuturesClient("", "")
    tickers = client._request("GET", "/api/v1/contract/ticker")
    if not tickers:
        print("Gagal ambil ticker")
        return

    results = []
    for x in tickers:
        sym = x.get("symbol", "")
        if not sym.endswith("_USDT") or "TEST" in sym or sym.startswith("INDEX"):
            continue
        
        results.append({
            "symbol": sym,
            "amount24": float(x.get("amount24", 0)),
            "volume24": float(x.get("volume24", 0)),
            "lastPrice": float(x.get("lastPrice", 0))
        })
    
    # Sort by volume24 (USDT)
    results.sort(key=lambda x: x["volume24"], reverse=True)
    print("--- TOP 5 BY volume24 (USDT) ---")
    for r in results[:5]:
        print(f"{r['symbol']}: ${r['volume24']:,.0f}")

    # Sort by amount24 (Coin qty)
    results.sort(key=lambda x: x["amount24"], reverse=True)
    print("\n--- TOP 5 BY amount24 (Qty) ---")
    for r in results[:5]:
        print(f"{r['symbol']}: {r['amount24']:,.0f}")

if __name__ == "__main__":
    debug()
