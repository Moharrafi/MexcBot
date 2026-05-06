import json
try:
    with open("config.json", "r") as f:
        d = json.load(f)
    print("Old SL MULT:", d.get("ATR_SL_MULT"))
    d["ATR_SL_MULT"] = 2.0
    with open("config.json", "w") as f:
        json.dump(d, f, indent=4)
    print("Successfully updated config.json to ATR_SL_MULT = 2.0")
except Exception as e:
    print("Error:", e)
