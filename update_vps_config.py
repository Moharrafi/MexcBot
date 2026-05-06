import json
import os

path = os.path.expanduser("~/mexc_xaut_v2/config.json")
if os.path.exists(path):
    with open(path, "r") as f:
        data = json.load(f)
    
    data["DRY_RUN"] = False
    data["POLL_INTERVAL"] = 15
    data["MIN_BULL_SCORE"] = 6
    data["MIN_BEAR_SCORE"] = 6
    
    with open(path, "w") as f:
        json.dump(data, f, indent=4)
    print("SUCCESS: Config updated to LIVE mode with 15s interval.")
else:
    print("ERROR: config.json not found.")
