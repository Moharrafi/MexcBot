import os
import json
import requests
import hashlib
import hmac
import time
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("MEXC_API_KEY")
API_SECRET = os.getenv("MEXC_API_SECRET")

def get_signature(params, secret):
    query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
    return hmac.new(secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

def request(method, path, params=None):
    url = f"https://contract.mexc.com{path}"
    ts = str(int(time.time() * 1000))
    
    headers = {
        "ApiKey": API_KEY,
        "Request-Time": ts,
        "Content-Type": "application/json"
    }
    
    sign_params = params.copy() if params else {}
    sign_params["api_key"] = API_KEY
    sign_params["req_time"] = ts
    
    # signature for contract v1 is different
    query_string = "&".join([f"{k}={v}" for k, v in sorted(sign_params.items())])
    signature = hmac.new(API_SECRET.encode('utf-8'), (API_KEY + ts + query_string).encode('utf-8'), hashlib.sha256).hexdigest()
    headers["Signature"] = signature
    
    if method == "GET":
        resp = requests.get(url, params=sign_params, headers=headers)
    else:
        resp = requests.post(url, json=params, headers=headers)
    return resp.json()

print("--- OPEN POSITIONS ---")
pos = request("GET", "/api/v1/private/position/open_positions")
print(json.dumps(pos, indent=4))

print("\n--- ASSET BALANCES ---")
bal = request("GET", "/api/v1/private/account/assets")
print(json.dumps(bal, indent=4))
