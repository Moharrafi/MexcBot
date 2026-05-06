import websocket
import ssl
import json

def on_message(ws, message):
    print(f"Message: {message}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Closed")

def on_open(ws):
    print("Opened")
    sub = {"method": "SUBSCRIPTION", "params": ["spot@public.miniTickers.v3.api@BTCUSDT"]}
    ws.send(json.dumps(sub))

if __name__ == "__main__":
    url = "wss://wbs.mexc.com/ws"
    # url = "wss://echo.websocket.org" # Test echo first?
    
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    ws = websocket.WebSocketApp(url,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    
    print(f"Connecting to {url}...")
    ws.run_forever(sslopt={"context": ssl_context}, suppress_origin=True)
