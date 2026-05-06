import os
from mexc_xaut_botV2 import MEXCFuturesClient
from dotenv import load_dotenv

load_dotenv()
api_key = os.getenv("MEXC_API_KEY")
api_secret = os.getenv("MEXC_API_SECRET")

client = MEXCFuturesClient(api_key, api_secret)
res = client._request("GET", "/api/v1/private/account/asset/USDT")
print("--- RAW USDT RESPONSE ---")
print(res)

res_all = client._request("GET", "/api/v1/private/account/assets")
for asset in res_all:
    if asset.get("currency") == "USDT":
        print("\n--- USDT FROM ASSETS LIST ---")
        print(asset)
