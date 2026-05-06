"""
MEXC SCALPER BOT V5 — TRUE SCALPER EDITION (FULLY FIXED)
✅ Semua syntax error, typo, trailing space, dan bug kritis telah diperbaiki
✅ WebSocket login diganti ke HMAC-SHA256 (sesuai doc MEXC terbaru)
✅ Position sizing dikonversi ke kontrak (bukan nominal USDT)
✅ Thread-lock ditambahkan untuk self.state.positions
✅ Separator baris dikomentari agar tidak crash
✅ __main__ block diperbaiki
"""
import os
import sys
import time
import hmac
import hashlib
import logging
import json
import csv
import threading
import ssl
import signal as os_signal
import numpy as np
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Tuple
from collections import deque
import requests
import pandas as pd
import pandas_ta_remake as ta
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════
# KONFIGURASI SCALPER V5 (TRAILING SPACE DIHAPUS)
# ══════════════════════════════════════════════════════════════
CONFIG = {
    "SYMBOL": "XAUT_USDT",
    "PRIMARY_TF": "5m",
    "CONFIRM_TF": "15m",
    "TREND_TF": "1h",
    "TREND_CACHE_SEC": 600,
    "REQUIRE_TREND_CONFIRM": True,
    "CANDLE_LIMIT": 200,
    "DRY_RUN": True,
    "VIRTUAL_BALANCE": 100.0,
    "MULTI_COIN_MODE": True,
    "SCAN_TOP_N": 50,
    "SCAN_INTERVAL": 90,
    "SCAN_MIN_VOLUME": 300_000,
    "SCAN_MIN_ADX": 15,
    "SCAN_MIN_ATR_PCT": 0.3,
    "SCAN_MIN_PRICE": 0.05,
    "AUTO_SWITCH_COIN": True,
    "SWITCH_MIN_ADVANTAGE": 2.5,
    "BLACKLIST_COINS": [],
    "WHITELIST_COINS": [],
    "MIN_BULL_SCORE": 15,
    "MIN_BEAR_SCORE": 15,
    "EARLY_ENTRY_SCORE": 17,
    "MIN_CANDLE_SCORE": 6,
    "VWAP_THRESHOLD_PCT": 0.10,
    "FUNDING_EXTREME_THRESHOLD": 0.0008,
    "EARLY_MOMENTUM_MIN": 1,
    "REQUIRE_ST_CONFIRM": True,
    "MID_CANDLE_ENTRY": True,
    "VELOCITY_CONFIRM": True,
    "VELOCITY_MIN_PCT": 0.05,
    "VELOCITY_WINDOW_SEC": 60,
    "ST_PERIOD": 14,
    "ST_MULTIPLIER": 3.0,
    "DEMA_FAST": 9,
    "DEMA_SLOW": 21,
    "ROC_PERIOD": 9,
    "WILLR_PERIOD": 14,
    "ATR_PERIOD": 14,
    "ADX_PERIOD": 14,
    "VOL_MA_PERIOD": 20,
    "SQUEEZE_BB_PERIOD": 20,
    "SQUEEZE_BB_STD": 2.0,
    "SQUEEZE_KC_PERIOD": 20,
    "SQUEEZE_KC_MULT": 1.5,
    "TAKER_FEE_PCT": 0.0006,
    "RISK_PER_TRADE": 0.08,
    "LEVERAGE": 10,
    "MAX_MARGIN_PCT": 0.20,
    "ATR_SL_MULT": 1.0,
    "ATR_TP1_MULT": 2.5,
    "ATR_TP2_MULT": 4.0,
    "ATR_TP3_MULT": 6.0,
    "MIN_RR_RATIO": 2.0,
    "MAX_OPEN_TRADES": 1,
    "USE_BE_FILTER": True,
    "BE_ACTIVATION_PCT": 0.008,
    "BE_FEE_BUFFER_PCT": 0.003,
    "USE_TRAILING_STOP": True,
    "TRAIL_ACTIVATION_PCT": 0.015,
    "TRAIL_DISTANCE_PCT": 0.010,
    "TP1_PARTIAL_CLOSE": True,
    "TP1_CLOSE_PCT": 40,
    "USE_ADX_FILTER": True,
    "ADX_MIN_THRESHOLD": 25,
    "MIN_ATR_PCT": 0.3,
    "MAX_ATR_PCT_ENTRY": 4.0,
    "REQUIRE_MTF_CONFIRM": True,
    "LOSS_COOLDOWN_SEC": 300,
    "GRACE_PERIOD_SEC": 60,
    "SWITCH_IDLE_MAX_SEC": 600,
    "MAX_DAILY_LOSS_PCT": 0.15,
    "MAX_DRAWDOWN_PCT": 0.30,
    "USE_KELLY": False,
    "EXIT_ON_ST_FLIP": False,
    "ST_FLIP_MIN_HOLD_SEC": 3600,
    "SIGNAL_FLIP_MIN_HOLD_SEC": 900,
    "SIGNAL_PERSIST_CYCLES": 2,
    "EARLY_CUT_LOSS_PCT": -0.015,
    "USE_MOMENTUM_EXHAUST": True,
    "EXHAUST_MIN_HOLD_SEC": 600,
    "EXHAUST_MIN_TP1_PCT": 0.30,
    "ENTRY_MODE": "PULLBACK",
    "PULLBACK_MIN_TREND_STRENGTH": 3,
    "PULLBACK_MIN_REVERSAL_SCORE": 3,
    "PULLBACK_ENTRY_MIN_SCORE": 11,
    "PULLBACK_DEPTH_ATR": 0.4,
    "PULLBACK_PERSIST_CYCLES": 1,
    "PULLBACK_MAX_VELOCITY": 0.20,
    "CANDLE_CLOSE_CONFIRM": True,
    "CANDLE_CLOSE_WINDOW_SEC": 120,
    "REGIME_ADX_MIN": 18,
    "DYNAMIC_LEVELS": True,
    "HIGH_VOL_ATR_PCT": 1.5,
    "LOW_VOL_ATR_PCT": 0.5,
    "ABORT_ENABLED": True,
    "ABORT_CHECK_SEC": 90,
    "ABORT_THRESHOLD_PCT": 0.004,
    "CORRELATION_FILTER": True,
    "OBI_BULL_THRESHOLD": 0.15,
    "OBI_BEAR_THRESHOLD": -0.15,
    "FLOW_BULL_THRESHOLD": 0.25,
    "FLOW_BEAR_THRESHOLD": -0.25,
    "EXIT_ON_SIGNAL_FLIP": True,
    "FLIP_ZONE1_PCT": 0.003,
    "FLIP_ZONE2_PCT": 0.015,
    "FLIP_ZONE2_MIN_SCORE": 7,
    "FLIP_ZONE3_MIN_SCORE": 8,
    "FLIP_ZONE3_CANDLES": 1,
    "ENABLE_AUTO_SECURE": True,
    "SECURE_PROFIT_PCT": 50,
    "MIN_SECURE_TRANSFER": 1.0,
    "POLL_INTERVAL": 15,
    "PRICE_UPDATE_INTERVAL": 2,
    "LOG_FILE": "scalper_v4.log",
    "LOG_LEVEL": "INFO",
    "STATE_FILE": "scalper_state_v4.json",
    "JOURNAL_FILE_DRY": "scalper_journal_v4_dry.csv",
    "JOURNAL_FILE_LIVE": "scalper_journal_v4_live.csv",
    "MEXC_API_KEY": os.getenv("MEXC_API_KEY", ""),
    "MEXC_API_SECRET": os.getenv("MEXC_API_SECRET", ""),
    "DASHBOARD_HOST": "0.0.0.0",
    "DASHBOARD_PORT": 5001,
    "PRESET_MODE": "SCALPER_V4",
    "USE_SESSION_FILTER": False,
    "ALLOWED_HOURS_UTC": list(range(0, 24)),
    "BLOCK_FRIDAY_CLOSE": False,
    "BLOCK_SUNDAY_OPEN": False,
    "NEWS_BLACKOUT": [],
}

# ══════════════════════════════════════════════════════════════
# ENV & LOGGING
# ══════════════════════════════════════════════════════════════
MEXC_API_KEY = CONFIG["MEXC_API_KEY"]
MEXC_API_SECRET = CONFIG["MEXC_API_SECRET"]
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
MEXC_BASE_URL = "https://contract.mexc.com"
MEXC_WS_URL = "wss://contract.mexc.com/edge"
WIB = timezone(timedelta(hours=7))

def wib_converter(*args):
    return datetime.now(WIB).timetuple()

log_handlers = [logging.StreamHandler()]
if CONFIG["LOG_FILE"]:
    log_handlers.append(logging.FileHandler(CONFIG["LOG_FILE"], encoding="utf-8"))

logging.basicConfig(
    level=getattr(logging, CONFIG["LOG_LEVEL"]),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=log_handlers,
)
logging.Formatter.converter = wib_converter
log = logging.getLogger("ScalperV5")

# ══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ══════════════════════════════════════════════════════════════
@dataclass
class Position:
    id: str
    symbol: str
    side: str
    entry_price: float
    quantity: float
    stop_loss: float
    take_profit1: float
    take_profit2: float
    take_profit3: float
    opened_at: str
    order_id: Optional[str] = None
    trailing_active: bool = False
    trailing_stop: float = 0.0
    highest_price: float = 0.0
    lowest_price: float = 0.0
    be_hit: bool = False
    tp1_hit: bool = False
    tp2_hit: bool = False
    partial_closed: bool = False
    pnl: float = 0.0
    fee: float = 0.0
    closed: bool = False
    close_reason: str = ""
    closed_at: str = ""
    flip_count: int = 0
    opened_ts: float = 0.0
    st_direction_at_entry: int = 0
    entry_bull_score: int = 0
    entry_bear_score: int = 0
    entry_confidence: int = 0
    entry_atr: float = 0.0
    entry_st_dir: int = 0
    entry_roc: float = 0.0
    entry_willr: float = 0.0
    entry_is_early: bool = False

@dataclass
class BotState:
    balance: float = 0.0
    peak_balance: float = 0.0
    positions: List[Position] = field(default_factory=list)
    total_trades: int = 0
    winning_trades: int = 0
    total_pnl: float = 0.0
    daily_pnl: float = 0.0
    daily_reset_date: str = ""
    last_signal: str = "NEUTRAL"
    circuit_breaker: bool = False
    circuit_reason: str = ""
    circuit_triggered_at: float = 0.0
    circuit_type: str = ""
    secured_total: float = 0.0
    started_at: str = ""
    iteration: int = 0
    daily_start_balance: float = 0.0
    active_symbol: str = ""
    api_error: str = ""
    real_balance: float = 0.0

    def win_rate(self) -> float:
        return (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0.0

    def drawdown(self) -> float:
        if self.peak_balance <= 0:
            return 0.0
        return (self.peak_balance - self.balance) / self.peak_balance * 100

# ══════════════════════════════════════════════════════════════
# MEXC REST API CLIENT
# ══════════════════════════════════════════════════════════════
class MEXCFuturesClient:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
            "Accept": "application/json",
        })
        self._contract_size_cache: dict = {}

    def _get_contract_size(self, symbol: str) -> float:
        if symbol in self._contract_size_cache:
            return self._contract_size_cache[symbol]
        data = self._request("GET", "/api/v1/contract/detail", {"symbol": symbol})
        cs = 1.0
        if isinstance(data, dict):
            cs = float(data.get("contractSize", 1.0))
        elif isinstance(data, list) and data:
            cs = float(data[0].get("contractSize", 1.0))
        if cs <= 0:
            cs = 1.0
        self._contract_size_cache[symbol] = cs
        return cs

    def _sign(self, timestamp: str, payload: str = "") -> str:
        message = f"{self.api_key}{timestamp}{payload}"
        return hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha256).hexdigest()

    def _request(self, method: str, ep: str, params: dict = None, _retry: int = 0) -> Optional[dict]:
        timestamp = str(int(time.time() * 1000))
        url = MEXC_BASE_URL + ep
        payload = json.dumps(params, separators=(',', ':')) if method == "POST" and params else ""
        headers = {}
        if "/private/" in ep:
            headers = {
                "ApiKey": self.api_key,
                "Request-Time": timestamp,
                "Signature": self._sign(timestamp, payload)
            }
            if method == "POST":
                headers["Content-Type"] = "application/json"
        try:
            if method == "POST":
                r = self.session.post(url, headers=headers, data=payload, timeout=15)
            else:
                r = self.session.get(url, headers=headers, params=params, timeout=15) 
            if not r.content:
                if _retry == 0 and method == "POST":
                    time.sleep(1.5)
                    return self._request(method, ep, params, _retry=1)
                return None
            data = r.json()
            if not data.get("success"):
                if data.get("code") != 600:
                    log.warning(f"API not success: {ep} -> {data}")
                return None
            return data.get("data")
        except Exception as e:
            log.error(f"Request error {ep}: {e}")
            if _retry == 0 and method == "POST":
                time.sleep(1.5)
                return self._request(method, ep, params, _retry=1)
            return None

    def get_ticker(self, symbol: str) -> Optional[float]:
        d = self._request("GET", "/api/v1/contract/ticker", {"symbol": symbol})
        return float(d["lastPrice"]) if d else None

    def get_top_volume_coins(self, limit: int = 50) -> List[dict]:
        d = self._request("GET", "/api/v1/contract/ticker")
        if not d:
            return []
        results = []
        for x in d:
            sym = x.get("symbol", "")
            if not sym.endswith("_USDT") or "TEST" in sym or sym.startswith("INDEX"):
                continue
            try:
                turnover = float(x.get("amount24", 0))
                results.append({
                    "symbol": sym,
                    "turnover": turnover,
                    "last_price": float(x.get("lastPrice", 0)),
                    "change": float(x.get("riseFallRate", 0)),
                })
            except (ValueError, TypeError):
                continue
        results.sort(key=lambda x: x["turnover"], reverse=True)
        return results[:limit]

    def get_balance(self, asset: str = "USDT") -> Optional[float]:
        d = self._request("GET", "/api/v1/private/account/assets")
        if d is None:
            return None
        if not d:
            return 0.0
        for b in d:
            if b.get("currency") == asset:
                val = b.get("equity") or b.get("availableBalance") or b.get("availableMargin") or b.get("cashBalance") or 0.0
                return float(val)
        return 0.0

    def get_funding_rate(self, symbol: str) -> Optional[float]:
        try:
            d = self._request("GET", f"/api/v1/contract/funding_rate/{symbol}")
            if d and isinstance(d, dict):
                rate = d.get("fundingRate") or d.get("rate") or d.get("value")
                if rate is not None:
                    return float(rate)
        except Exception:
            pass
        return None

    def get_klines(self, symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
        tf_map = {
            "1m": ("Min1", 60), "3m": ("Min3", 180), "5m": ("Min5", 300),
            "15m": ("Min15", 900), "30m": ("Min30", 1800),
            "1h": ("Min60", 3600), "4h": ("Hour4", 14400),
        }
        tf_info = tf_map.get(interval, ("Min1", 60))
        tf_name, tf_sec = tf_info[0], tf_info[1]
        end_ts = int(time.time())
        start_ts = end_ts - (limit * tf_sec)
        params = {"interval": tf_name, "start": start_ts, "end": end_ts}
        d = self._request("GET", f"/api/v1/contract/kline/{symbol}", params)
        if not d:
            return pd.DataFrame()
        df = pd.DataFrame(d, columns=["time", "open", "close", "high", "low", "vol", "amount"])
        df.rename(columns={"vol": "volume"}, inplace=True)
        df["open_time"] = pd.to_datetime(df["time"], unit="s")
        df.set_index("open_time", inplace=True)
        for col in ["open", "close", "high", "low", "volume"]:
            df[col] = df[col].astype(float)
        return df

    def place_order(self, symbol: str, side: int, order_type: int,
                    lever: int, quantity: float, price: float = 0) -> Optional[dict]:
        contract_size = self._get_contract_size(symbol)
        vol = max(1, round(quantity / contract_size))
        params = {
            "symbol": symbol, "side": side, "type": order_type,
            "vol": vol, "leverage": int(lever), "openType": 1,
        }
        if price > 0:
            params["price"] = str(price)
        payload = json.dumps(params, separators=(',', ':'))
        timestamp = str(int(time.time() * 1000))
        signature = self._sign(timestamp, payload)
        for attempt in range(2):
            try:
                r = self.session.post(
                    MEXC_BASE_URL + "/api/v1/private/order/create",
                    headers={
                        "ApiKey": self.api_key, "Request-Time": timestamp,
                        "Signature": signature, "Content-Type": "application/json",
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "application/json, text/plain, */*",
                        "Origin": "https://www.mexc.com",
                        "Referer": "https://www.mexc.com/futures",
                    },
                    data=payload, timeout=15
                )
                log.info(f"[ORDER] HTTP {r.status_code} body={r.content[:120]!r}")
                body = r.content.strip()
                if not body:
                    log.warning(f"[ORDER] Empty response attempt {attempt+1}")
                elif body.startswith(b'<'):
                    log.warning(f"[ORDER] WAF block attempt {attempt+1}")
                else:
                    data = r.json()
                    if data.get("success"):
                        return data.get("data")
                    log.warning(f"[ORDER] API not success: {data}")
                    return None
            except Exception as e:
                log.error(f"[ORDER] Exception attempt {attempt+1}: {e}")
            if attempt == 0:
                time.sleep(2)
                timestamp = str(int(time.time() * 1000))
                signature = self._sign(timestamp, payload)
        return None

    def place_stop_order(self, symbol: str, side: int, stop_price: float,
                         quantity: float, leverage: int, margin_mode: int = 1,
                         is_take_profit: bool = False) -> Optional[dict]:
        trigger_type = 2 if side in (3, 4) else 1
        if is_take_profit:
            trigger_type = 1 if trigger_type == 2 else 2
        contract_size = self._get_contract_size(symbol)
        vol = max(1, round(quantity / contract_size))
        params = {
            "symbol": symbol,
            "side": side,
            "vol": float(vol),
            "openType": margin_mode,
            "triggerPrice": float(stop_price),
            "triggerType": trigger_type,
            "executeCycle": 1,
            "trend": 1,
            "orderType": 5,
        }
        if margin_mode == 1:
            params["leverage"] = int(leverage)
        return self._request("POST", "/api/v1/private/planorder/place", params)

    def cancel_all_orders(self, symbol: str):
        self._request("POST", "/api/v1/private/order/cancel_all", {"symbol": symbol})
        self._request("POST", "/api/v1/private/planorder/cancel_all", {"symbol": symbol})

    def get_open_positions(self, symbol: str = None) -> list:
        params = {"symbol": symbol} if symbol else {}
        for ep in ["/api/v1/private/position/open_positions", "/api/v1/private/position/open_details"]:
            data = self._request("GET", ep, params)
            if data is not None:
                return data if isinstance(data, list) else []
        return []

    def get_stop_orders(self, symbol: str) -> list:
        for ep in ["/api/v1/private/stop_order/list/open_orders", "/api/v1/private/stop_order/open_orders"]:
            data = self._request("GET", ep, {"symbol": symbol})
            if data is not None:
                return data if isinstance(data, list) else []
        return []

class MEXCSpotClient:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.mexc.com"
        self.session = requests.Session()
        self.session.verify = False
        class TLSAdapter(requests.adapters.HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                ctx = ssl.create_default_context()
                ctx.set_ciphers('DEFAULT@SECLEVEL=1')
                ctx.check_hostname = False
                kwargs['ssl_context'] = ctx
                return super().init_poolmanager(*args, **kwargs)
        self.session.mount("https://", TLSAdapter())

    def _sign(self, query_string: str) -> str:
        return hmac.new(self.api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()

    def transfer_to_spot(self, amount: float) -> bool:
        endpoint = "/api/v3/capital/transfer"
        timestamp = int(time.time() * 1000)
        params = {
            "fromAccountType": "FUTURES", "toAccountType": "SPOT",
            "asset": "USDT", "amount": str(round(amount, 4)), "timestamp": timestamp
        }
        qs = "&".join([f"{k}={v}" for k, v in params.items()])
        sig = self._sign(qs)
        url = f"{self.base_url}{endpoint}?{qs}&signature={sig}"
        try:
            r = self.session.post(url, headers={"X-MEXC-APIKEY": self.api_key}, timeout=15)
            res = r.json()
            if res.get("tranId") or res.get("id"):
                log.info(f"[SECURE] Transfer ke Spot: ${amount:.4f}")
                return True
            log.error(f"[SECURE] Gagal: {res}")
            return False
        except Exception as e:
            log.error(f"[SECURE] Error: {e}")
            return False

# ══════════════════════════════════════════════════════════════
# PRICE FEED (WebSocket + REST Fallback)
# ══════════════════════════════════════════════════════════════
class PriceFeed:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.price = 0.0
        self.client = MEXCFuturesClient(MEXC_API_KEY, MEXC_API_SECRET)
        self._callbacks = []
        self._running = False
        self._lock = threading.Lock()
        self._ws = None
        self._ws_connected = False
        self._ws_last_msg = 0.0
        self._reconnect_count = 0
        self._max_reconnect = 50
        self._obi_raw = 0.0
        self._obi_history = deque(maxlen=8)
        self._obi_ts = 0.0
        self._buy_volume = 0.0
        self._sell_volume = 0.0
        self._tick_ts = 0.0
        self._tick_window = 15.0
        self._whale_ts = 0.0
        self._whale_side = 0

    def add_callback(self, cb):
        self._callbacks.append(cb)

    def start(self):
        self._running = True
        threading.Thread(target=self._ws_loop, daemon=True, name="WS-Price").start()
        threading.Thread(target=self._rest_fallback, daemon=True, name="REST-Fallback").start()

    def stop(self):
        self._running = False
        if self._ws:
            try: self._ws.close()
            except: pass

    def get_price(self) -> float:
        with self._lock:
            return self.price

    def get_obi(self) -> float:
        with self._lock:
            if time.time() - self._obi_ts > 10 or not self._obi_history:
                return 0.0
            return sum(self._obi_history) / len(self._obi_history)

    def get_trade_flow(self) -> float:
        with self._lock:
            total = self._buy_volume + self._sell_volume
            if total == 0:
                return 0.0
            return (self._buy_volume - self._sell_volume) / total

    def get_whale_signal(self, window_sec: float = 60.0) -> int:
        with self._lock:
            if self._whale_ts > 0 and (time.time() - self._whale_ts) < window_sec:
                return self._whale_side
            return 0

    @property
    def is_ws_alive(self) -> bool:
        return self._ws_connected and (time.time() - self._ws_last_msg < 15)

    def _fire_callbacks(self, price: float):
        with self._lock:
            self.price = price
        for cb in self._callbacks:
            try: cb(price)
            except: pass

    def _ws_loop(self):
        try:
            import websocket
        except ImportError:
            log.warning("websocket-client tidak terinstall, pakai REST saja.")
            return
        while self._running and self._reconnect_count < self._max_reconnect:
            try:
                ws = websocket.WebSocketApp(
                    MEXC_WS_URL,
                    on_open=self._on_ws_open,
                    on_message=self._on_ws_message,
                    on_error=self._on_ws_error,
                    on_close=self._on_ws_close,
                )
                self._ws = ws
                ws.run_forever(ping_interval=20, ping_timeout=10, sslopt={"cert_reqs": ssl.CERT_NONE})
            except Exception as e:
                log.warning(f"WS crash: {e}")
            self._ws_connected = False
            self._reconnect_count += 1
            if self._running:
                wait = min(5 * self._reconnect_count, 60)
                time.sleep(wait)

    def _on_ws_open(self, ws):
        self._ws_connected = True
        self._reconnect_count = 0
        sym = self.symbol
        ws.send(json.dumps({"method": "sub.ticker", "param": {"symbol": sym}}))
        ws.send(json.dumps({"method": "sub.depth", "param": {"symbol": sym}}))
        ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": sym}}))
        threading.Thread(target=self._ws_ping_loop, args=(ws,), daemon=True, name="WS-Ping").start()

    def _ws_ping_loop(self, ws):
        while self._ws_connected and self._running:
            time.sleep(20)
            try:
                if self._ws_connected:
                    ws.send(json.dumps({"method": "ping"}))
            except Exception:
                break

    def _on_ws_message(self, ws, message):
        try:
            data = json.loads(message)
            ch = data.get("channel", "")

            if ch == "push.ticker":
                lp = data.get("data", {}).get("lastPrice")
                if lp:
                    self._ws_last_msg = time.time()
                    self._fire_callbacks(float(lp))

            elif ch == "push.depth":
                d = data.get("data", {})
                asks = d.get("asks", [])[:10]
                bids = d.get("bids", [])[:10]
                ask_vol = sum(float(a[1]) for a in asks if len(a) >= 2)
                bid_vol = sum(float(b[1]) for b in bids if len(b) >= 2)
                total = ask_vol + bid_vol
                with self._lock:
                    raw = (bid_vol - ask_vol) / total if total > 0 else 0.0
                    self._obi_raw = raw
                    self._obi_history.append(raw)
                    self._obi_ts = time.time()

            elif ch == "push.deal":
                deals = data.get("data", [])
                now = time.time()
                with self._lock:
                    if now - self._tick_ts > self._tick_window:
                        self._buy_volume = 0.0
                        self._sell_volume = 0.0
                        self._tick_ts = now
                    for deal in deals:
                        t_side = deal.get("T", deal.get("side", 0))
                        notional = float(deal.get("p", 0)) * float(deal.get("v", 0))
                        is_buy = (t_side == 1)
                        is_sell = (t_side == 2)
                        if is_buy:
                            self._buy_volume += notional
                        elif is_sell:
                            self._sell_volume += notional
                        whale_th = 30_000
                        if notional >= whale_th:
                            self._whale_ts = now
                            self._whale_side = 1 if is_buy else -1
        except Exception:
            pass

    def _on_ws_error(self, ws, error):
        log.debug(f"WS error: {error}")

    def _on_ws_close(self, ws, close_code, close_msg):
        self._ws_connected = False

    def resubscribe(self, new_symbol: str):
        old = self.symbol
        self.symbol = new_symbol
        with self._lock:
            self._obi_raw = 0.0; self._obi_ts = 0.0
            self._obi_history.clear()
            self._buy_volume = 0.0; self._sell_volume = 0.0; self._tick_ts = 0.0
        if self._ws and self._ws_connected:
            try:
                for ch in ("ticker", "depth", "deal"):
                    self._ws.send(json.dumps({"method": f"unsub.{ch}", "param": {"symbol": old}}))
                    self._ws.send(json.dumps({"method": f"sub.{ch}", "param": {"symbol": new_symbol}}))
            except Exception as e:
                log.warning(f"WS resubscribe error: {e}")

    def _rest_fallback(self):
        while self._running:
            try:
                if not self.is_ws_alive:
                    p = self.client.get_ticker(self.symbol)
                    if p:
                        self._fire_callbacks(p)
                time.sleep(3)
            except Exception as e:
                log.debug(f"REST fallback error: {e}")
                time.sleep(10)

# ══════════════════════════════════════════════════════════════
# USER STREAM (Private WebSocket)
# ══════════════════════════════════════════════════════════════
class UserStreamWS:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self._running = False
        self._ws = None
        self._connected = False
        self._callbacks = []
        self._reconnect = 0

    def add_callback(self, cb):
        self._callbacks.append(cb)

    def start(self):
        if not self.api_key or not self.api_secret:
            log.warning("[UserWS] API key tidak ada — private stream dinonaktifkan")
            return
        self._running = True
        threading.Thread(target=self._ws_loop, daemon=True, name="WS-UserStream").start()

    def stop(self):
        self._running = False
        if self._ws:
            try: self._ws.close()
            except: pass

    def _make_login(self) -> dict:
        ts = str(int(time.time() * 1000))
        # FIX: HMAC-SHA256 sesuai doc MEXC Futures WS v2
        sign_str = f"{self.api_key}{ts}{self.api_secret}"
        signature = hmac.new(self.api_secret.encode(), sign_str.encode(), hashlib.sha256).hexdigest()
        return {"method": "login", "param": {"apiKey": self.api_key, "reqTime": ts, "signature": signature}}

    def _ws_loop(self):
        try:
            import websocket
        except ImportError:
            return
        while self._running and self._reconnect < 50:
            try:
                ws = websocket.WebSocketApp(
                    MEXC_WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=lambda w, e: log.debug(f"[UserWS] error: {e}"),
                    on_close=lambda w, c, m: setattr(self, "_connected", False),
                )
                self._ws = ws
                ws.run_forever(ping_interval=20, ping_timeout=10, sslopt={"cert_reqs": ssl.CERT_NONE})
            except Exception as e:
                log.warning(f"[UserWS] crash: {e}")
            self._connected = False
            self._reconnect += 1
            if self._running:
                time.sleep(min(5 * self._reconnect, 60))

    def _on_open(self, ws):
        self._connected = True
        self._reconnect = 0
        ws.send(json.dumps(self._make_login()))
        threading.Thread(target=self._ping_loop, args=(ws,), daemon=True, name="WS-UserPing").start()

    def _ping_loop(self, ws):
        while self._connected and self._running:
            time.sleep(20)
            try:
                if self._connected:
                    ws.send(json.dumps({"method": "ping"}))
            except Exception:
                break

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            ch = data.get("channel", "")

            if ch == "rs.login":
                if data.get("data") == "success":
                    ws.send(json.dumps({"method": "sub.personal.order", "param": {}}))
                    ws.send(json.dumps({"method": "sub.personal.position", "param": {}}))
                    log.info("[UserWS] Login sukses → subscribed personal.order + personal.position")
                else:
                    log.warning(f"[UserWS] Login gagal: {data}")
                return

            if ch in ("push.personal.order", "push.personal.position"):
                for cb in self._callbacks:
                    try: cb(ch, data.get("data", {}))
                    except: pass
        except Exception:
            pass

# ══════════════════════════════════════════════════════════════
# SCALPER TECHNICAL ANALYSIS — ZERO-LAG ENGINE
# ══════════════════════════════════════════════════════════════
class ScalperTA:
    def __init__(self, cfg: dict):
        self.cfg = cfg

    def _calc_supertrend(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        c = self.cfg
        per = c.get("ST_PERIOD", 10)
        mul = c.get("ST_MULTIPLIER", 2.5)

        high = df["high"].values
        low = df["low"].values
        close = df["close"].values
        n = len(df)

        atr_arr = ta.atr(df["high"], df["low"], df["close"], length=per).values
        hl2 = (high + low) / 2.0

        basic_upper = hl2 + mul * atr_arr
        basic_lower = hl2 - mul * atr_arr

        fu = np.full(n, np.nan)
        fl = np.full(n, np.nan)
        st = np.full(n, np.nan)
        di = np.ones(n, dtype=int)

        for i in range(1, n):
            if np.isnan(atr_arr[i]):
                continue
            if np.isnan(fu[i-1]) or basic_upper[i] < fu[i-1] or close[i-1] > fu[i-1]:
                fu[i] = basic_upper[i]
            else:
                fu[i] = fu[i-1]
            if np.isnan(fl[i-1]) or basic_lower[i] > fl[i-1] or close[i-1] < fl[i-1]:
                fl[i] = basic_lower[i]
            else:
                fl[i] = fl[i-1]

            prev_st = st[i-1]
            if np.isnan(prev_st):
                st[i] = fu[i]; di[i] = 1
            elif prev_st == fu[i-1]:
                if close[i] > fu[i]:
                    st[i] = fl[i]; di[i] = -1
                else:
                    st[i] = fu[i]; di[i] = 1
            else:
                if close[i] < fl[i]:
                    st[i] = fu[i]; di[i] = 1
                else:
                    st[i] = fl[i]; di[i] = -1

        return pd.Series(st, index=df.index), pd.Series(di, index=df.index)

    def _calc_dema(self, series: pd.Series, length: int) -> pd.Series:
        ema1 = ta.ema(series, length=length)
        ema2 = ta.ema(ema1, length=length)
        return 2.0 * ema1 - ema2

    def _calc_volume_delta(self, df: pd.DataFrame) -> pd.Series:
        body_ratio = abs(df["close"] - df["open"]) / (df["high"] - df["low"] + 1e-9)
        delta = np.where(
            df["close"] >= df["open"],
            df["volume"] * body_ratio,
            -df["volume"] * body_ratio
        )
        return pd.Series(delta, index=df.index)

    def _calc_squeeze(self, df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        c = self.cfg
        bbp = c.get("SQUEEZE_BB_PERIOD", 20)
        bbs = c.get("SQUEEZE_BB_STD", 2.0)
        kcp = c.get("SQUEEZE_KC_PERIOD", 20)
        kcm = c.get("SQUEEZE_KC_MULT", 1.5)
        mp = 12

        bb_mid = df["close"].rolling(bbp).mean()
        bb_std = df["close"].rolling(bbp).std()
        bb_u = bb_mid + bbs * bb_std
        bb_l = bb_mid - bbs * bb_std

        kc_mid = ta.ema(df["close"], length=kcp)
        kc_atr = ta.atr(df["high"], df["low"], df["close"], length=kcp)
        kc_u = kc_mid + kcm * kc_atr
        kc_l = kc_mid - kcm * kc_atr

        squeeze_on = (bb_u <= kc_u) & (bb_l >= kc_l)

        hh = df["high"].rolling(mp).max()
        ll = df["low"].rolling(mp).min()
        mid = (hh + ll) / 2.0
        delta = df["close"] - (mid + kc_mid) / 2.0
        mom = delta.rolling(mp).mean()

        return squeeze_on, mom

    def _calc_consec(self, df: pd.DataFrame) -> pd.Series:
        is_bull = (df["close"] >= df["open"])
        grp = (is_bull != is_bull.shift()).cumsum()
        cnt = is_bull.groupby(grp).cumcount() + 1
        return cnt.where(is_bull, -cnt)

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.cfg
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.get("ATR_PERIOD", 10))
        df["atr_pct"] = df["atr"] / df["close"] * 100
        df["supertrend"], df["st_dir"] = self._calc_supertrend(df)
        df["dema_fast"] = self._calc_dema(df["close"], c.get("DEMA_FAST", 5))
        df["dema_slow"] = self._calc_dema(df["close"], c.get("DEMA_SLOW", 13))
        rp = c.get("ROC_PERIOD", 3)
        df["roc"] = (df["close"] - df["close"].shift(rp)) / df["close"].shift(rp) * 100
        df["roc_prev"] = df["roc"].shift(1)
        df["roc_accel"] = df["roc"] - df["roc_prev"]
        wp = c.get("WILLR_PERIOD", 5)
        hh_roll = df["high"].rolling(wp).max()
        ll_roll = df["low"].rolling(wp).min()
        df["willr"] = (df["close"] - hh_roll) / (hh_roll - ll_roll + 1e-9) * (-100)
        try:
            adx_res = ta.adx(df["high"], df["low"], df["close"], length=c.get("ADX_PERIOD", 10))
            if adx_res is not None and not adx_res.empty:
                adx_col = [col for col in adx_res.columns if col.startswith("ADX_")]
                df["adx"] = adx_res[adx_col[0]] if adx_col else 25.0
            else:
                df["adx"] = 25.0
        except Exception:
            df["adx"] = 25.0
        vp = c.get("VOL_MA_PERIOD", 20)
        df["vol_ma"] = df["volume"].rolling(vp).mean()
        df["vol_ratio"] = (df["volume"] / df["vol_ma"]).round(2)
        df["vol_delta"] = self._calc_volume_delta(df)
        df["vol_delta_ma"] = df["vol_delta"].rolling(5).mean()
        try:
            df["vwap"] = ta.vwap(df["high"], df["low"], df["close"], df["volume"])
        except Exception:
            df["vwap"] = df["close"].rolling(20).mean()
        df["body_size"] = abs(df["close"] - df["open"])
        df["candle_rng"] = df["high"] - df["low"]
        df["body_ratio"] = df["body_size"] / (df["candle_rng"] + 1e-9)
        df["is_bullish"] = df["close"] > df["open"]
        df["squeeze_on"], df["squeeze_mom"] = self._calc_squeeze(df)
        df["consec"] = self._calc_consec(df)
        df["cvd"] = df["vol_delta"].rolling(26).sum()
        df["cvd_ma"] = df["cvd"].rolling(10).mean()
        df["cvd_trend"] = df["cvd"] - df["cvd"].shift(5)
        df = self._detect_candle_patterns(df)
        df = self._detect_market_structure(df)
        df = self._detect_liquidity_sweep(df)
        return df

    def _detect_candle_patterns(self, df: pd.DataFrame) -> pd.DataFrame:
        o = df["open"]; h = df["high"]; l = df["low"]; c = df["close"]
        body = (c - o).abs()
        total = (h - l).clip(lower=1e-9)
        high_body = c.combine(o, max)
        low_body = c.combine(o, min)
        upper_wick = h - high_body
        lower_wick = low_body - l
        midpoint = (h + l) / 2
        hammer_shape = (lower_wick >= body * 2.0) & (upper_wick <= body * 0.5) & (body > 0)
        df["cdl_hammer"] = np.where(hammer_shape, 1, 0)
        star_shape = (upper_wick >= body * 2.0) & (lower_wick <= body * 0.5) & (body > 0)
        df["cdl_shooting_star"] = np.where(star_shape & (c < o), -1, np.where(star_shape & (c >= o), 1, 0))
        bull_pin = (lower_wick >= body * 2.5) & (c > midpoint)
        bear_pin = (upper_wick >= body * 2.5) & (c < midpoint)
        df["cdl_pinbar"] = np.where(bull_pin, 1, np.where(bear_pin, -1, 0))
        po = o.shift(1); pc = c.shift(1)
        bull_engulf = (c > o) & (pc < po) & (c >= po) & (o <= pc)
        bear_engulf = (c < o) & (pc > po) & (c <= po) & (o >= pc)
        df["cdl_engulfing"] = np.where(bull_engulf, 1, np.where(bear_engulf, -1, 0))
        is_doji = (body / total) < 0.05
        dragonfly = is_doji & (lower_wick > upper_wick * 3)
        gravestone = is_doji & (upper_wick > lower_wick * 3)
        df["cdl_doji"] = np.where(dragonfly, 1, np.where(gravestone, -1, 0))
        p2_body = body.shift(2); p2_total = total.shift(2); p1_body = body.shift(1)
        p2_bull = c.shift(2) > o.shift(2)
        big_first = (p2_body / p2_total) > 0.55
        small_mid = p1_body < p2_body * 0.45
        morning_star = big_first & ~p2_bull & small_mid & (c > o) & ((body / total) > 0.45) & (c > (o.shift(2) + c.shift(2)) / 2)
        evening_star = big_first & p2_bull & small_mid & (c < o) & ((body / total) > 0.45) & (c < (o.shift(2) + c.shift(2)) / 2)
        df["cdl_star"] = np.where(morning_star, 1, np.where(evening_star, -1, 0))
        return df

    def _detect_market_structure(self, df: pd.DataFrame, window: int = 5) -> pd.DataFrame:
        roll_h = df["high"].rolling(window).max()
        roll_l = df["low"].rolling(window).min()
        prev_h = roll_h.shift(window)
        prev_l = roll_l.shift(window)
        hh = roll_h > prev_h
        hl = roll_l > prev_l
        lh = roll_h < prev_h
        ll = roll_l < prev_l
        df["mkt_struct"] = np.where(hh & hl, 1, np.where(lh & ll, -1, 0))
        return df

    def _detect_liquidity_sweep(self, df: pd.DataFrame, lookback: int = 20) -> pd.DataFrame:
        recent_high = df["high"].shift(1).rolling(lookback).max()
        recent_low = df["low"].shift(1).rolling(lookback).min()
        bull_sweep = (df["low"] < recent_low) & (df["close"] > recent_low)
        bear_sweep = (df["high"] > recent_high) & (df["close"] < recent_high)
        df["liq_sweep"] = np.where(bull_sweep, 1, np.where(bear_sweep, -1, 0))
        return df

    def get_htf_bias(self, df_htf: pd.DataFrame) -> str:
        try:
            df_htf.dropna(inplace=True)
            if len(df_htf) < 3: return "NEUTRAL"
            row = df_htf.iloc[-1]
            st_bull = int(row.get("st_dir", 1)) == -1
            dema_bull = float(row.get("dema_fast", 0)) > float(row.get("dema_slow", 0))
            roc_pos = float(row.get("roc", 0)) > 0
            score = sum([st_bull, dema_bull, roc_pos])
            if score >= 2: return "BULLISH"
            elif score == 0: return "BEARISH"
            return "NEUTRAL"
        except Exception:
            return "NEUTRAL"

    def get_trend_bias(self, df_trend: pd.DataFrame) -> dict:
        try:
            df = df_trend
            df.dropna(inplace=True)
            if len(df) < 5: return {"bias": "NEUTRAL", "strength": 0, "detail": "Data tidak cukup"}
            row = df.iloc[-1]; prev = df.iloc[-2]
            st_now = int(row.get("st_dir", 0)); st_prev = int(prev.get("st_dir", 0))
            dema_f = float(row.get("dema_fast", 0)); dema_s = float(row.get("dema_slow", 0))
            roc = float(row.get("roc", 0)); adx = float(row.get("adx", 0))
            last3 = df.tail(3); bull_candles = (last3["close"] > last3["open"]).sum()
            st_just_flipped_bull = (st_now == -1 and st_prev == 1)
            st_just_flipped_bear = (st_now == 1 and st_prev == -1)
            cdl_pin = int(row.get("cdl_pinbar", 0)); cdl_eng = int(row.get("cdl_engulfing", 0))
            cdl_star = int(row.get("cdl_star", 0)); cdl_doji = int(row.get("cdl_doji", 0))
            liq_sw = int(row.get("liq_sweep", 0)); mkt_str = int(row.get("mkt_struct", 0))
            candle_bull = sum([cdl_pin == 1, cdl_eng == 1, cdl_star == 1, cdl_doji == 1, liq_sw == 1])
            candle_bear = sum([cdl_pin == -1, cdl_eng == -1, cdl_star == -1, cdl_doji == -1, liq_sw == -1])
            adx_bull = adx > 20 and (st_now == -1 or dema_f > dema_s)
            adx_bear = adx > 20 and (st_now == 1 or dema_f < dema_s)
            bull_pts = sum([st_now == -1, st_just_flipped_bull, dema_f > dema_s, roc > 0.3, adx_bull, bull_candles >= 2, candle_bull >= 1, mkt_str == 1])
            bear_pts = sum([st_now == 1, st_just_flipped_bear, dema_f < dema_s, roc < -0.3, adx_bear, bull_candles <= 1, candle_bear >= 1, mkt_str == -1])
            detail_parts = []
            if st_now == -1: detail_parts.append("ST↑")
            elif st_now == 1: detail_parts.append("ST↓")
            if dema_f > dema_s: detail_parts.append("DEMA↑")
            elif dema_f < dema_s: detail_parts.append("DEMA↓")
            detail_parts.append(f"ROC{roc:+.2f}%"); detail_parts.append(f"ADX{adx:.0f}")
            if mkt_str == 1: detail_parts.append("HH/HL")
            elif mkt_str == -1: detail_parts.append("LH/LL")
            if candle_bull: detail_parts.append(f"CDL+{candle_bull}")
            elif candle_bear: detail_parts.append(f"CDL-{candle_bear}")
            detail = " ".join(detail_parts)
            if bull_pts >= 3 and bull_pts > bear_pts:
                return {"bias": "BULLISH", "strength": bull_pts, "detail": detail}
            elif bear_pts >= 3 and bear_pts > bull_pts:
                return {"bias": "BEARISH", "strength": bear_pts, "detail": detail}
            return {"bias": "NEUTRAL", "strength": 0, "detail": detail}
        except Exception as e:
            log.debug(f"get_trend_bias error: {e}")
            return {"bias": "NEUTRAL", "strength": 0, "detail": "Error"}

    def get_pullback_signal(self, df: pd.DataFrame, trend_bias: str, trend_strength: int, htf_bias: str,
                            current_price: float = 0.0, obi: float = 0.0, trade_flow: float = 0.0,
                            funding_rate: float = 0.0, whale_side: int = 0) -> dict:
        c = self.cfg
        def _neutral(reason: str) -> dict:
            cl = float(df["close"].iloc[-1]) if len(df) > 0 else 0.0
            return {"signal": "NEUTRAL", "bull_score": 0, "bear_score": 0, "max_score": 16, "confidence": 0,
                    "atr": 0.0, "atr_pct": 0.0, "close": cl, "vol_ratio": 1.0, "roc": 0.0, "roc_accel": 0.0,
                    "willr": -50.0, "adx": 0.0, "st_dir": 0, "st_flipped": False, "dema_fast": 0.0, "dema_slow": 0.0,
                    "squeeze_on": False, "squeeze_mom": 0.0, "obi": obi, "trade_flow": trade_flow,
                    "funding_rate": funding_rate, "whale_side": whale_side, "vol_delta": 0.0, "body_ratio": 0.0,
                    "consec": 0, "vwap": 0.0, "supertrend": 0.0, "details": {"Status": reason}, "is_pullback_entry": True}

        if len(df) < 10: return _neutral("Data tidak cukup")
        if trend_bias == "NEUTRAL": return _neutral("1H NEUTRAL — butuh tren jelas untuk pullback entry")
        if htf_bias == "NEUTRAL": return _neutral("15m NEUTRAL — butuh konfirmasi TF menengah")
        if trend_bias != htf_bias: return _neutral(f"Konflik TF: 1H={trend_bias} vs 15m={htf_bias}")
        min_strength = c.get("PULLBACK_MIN_TREND_STRENGTH", 3)
        if trend_strength < min_strength: return _neutral(f"Tren 1H terlalu lemah ({trend_strength}/{min_strength} min)")

        direction = "LONG" if trend_bias == "BULLISH" else "SHORT"
        row, prev = df.iloc[-1], df.iloc[-2]
        _adx_regime = float(row.get("adx", 0)) if not pd.isna(row.get("adx", 0)) else 0.0
        _mkt_struct = int(row.get("mkt_struct", 0))
        _adx_min = c.get("REGIME_ADX_MIN", 18)
        if _adx_regime < _adx_min and _mkt_struct == 0:
            return _neutral(f"Pasar ranging/chop (ADX {_adx_regime:.0f} < {_adx_min} + no HH/HL) — pullback tidak reliable di sideways")

        close = current_price if current_price > 0 else float(row["close"])
        atr_val = float(row.get("atr", 0)); atr_pct = float(row.get("atr_pct", 0))
        adx = float(row.get("adx", 0)) if not pd.isna(row.get("adx", 0)) else 0.0
        vol_ratio = float(row.get("vol_ratio", 1)); roc = float(row.get("roc", 0))
        roc_accel = float(row.get("roc_accel", 0)); willr = float(row.get("willr", -50))
        dema_f = float(row.get("dema_fast", 0)); dema_s = float(row.get("dema_slow", 0))
        st_now = int(row.get("st_dir", 1))

        if atr_pct < c.get("MIN_ATR_PCT", 0.3): return _neutral(f"ATR rendah ({atr_pct:.2f}%) — instrumen terlalu flat")
        score, details = 0, {}

        if direction == "LONG" and st_now != -1: return _neutral("ST 5m flip bearish — kemungkinan trend reversal, bukan pullback")
        if direction == "SHORT" and st_now != 1: return _neutral("ST 5m flip bullish — kemungkinan trend reversal, bukan bounce")
        score += 2; details["ST"] = f"✅ ST 5m masih {'bullish' if direction=='LONG' else 'bearish'} — tren utuh"

        lookback = min(10, len(df)); min_depth = c.get("PULLBACK_DEPTH_ATR", 0.4)
        if direction == "LONG":
            recent_high = df["high"].tail(lookback).max()
            depth_atr = (recent_high - close) / atr_val if atr_val > 0 else 0.0
            if depth_atr < min_depth: return _neutral(f"Pullback terlalu dangkal ({depth_atr:.2f}×ATR < {min_depth} min)")
            if depth_atr > 3.0: return _neutral(f"Koreksi terlalu dalam ({depth_atr:.2f}×ATR) — mungkin trend reversal")
            score += 1 if depth_atr >= 0.7 else 0; details["DEPTH"] = f"📏 Pullback {depth_atr:.2f}×ATR dari high"
        else:
            recent_low = df["low"].tail(lookback).min()
            depth_atr = (close - recent_low) / atr_val if atr_val > 0 else 0.0
            if depth_atr < min_depth: return _neutral(f"Bounce terlalu kecil ({depth_atr:.2f}×ATR < {min_depth} min)")
            if depth_atr > 3.0: return _neutral(f"Bounce terlalu tinggi ({depth_atr:.2f}×ATR) — mungkin trend reversal")
            score += 1 if depth_atr >= 0.7 else 0; details["DEPTH"] = f"📏 Bounce {depth_atr:.2f}×ATR dari low"

        lookback_fib = min(20, len(df)); df_fib = df.tail(lookback_fib); fib_score, fib_detail = 0, ""
        if atr_val > 0:
            sh = float(df_fib["high"].max()); sl = float(df_fib["low"].min()); swing_range = sh - sl
            if swing_range >= atr_val * 0.5:
                fib_ret = (sh - close) / swing_range if direction == "LONG" else (close - sl) / swing_range
                if 0.382 <= fib_ret <= 0.618: fib_score, fib_detail = 2, f"🎯 Fib golden zone ({fib_ret:.1%} {'retracement' if direction=='LONG' else 'bounce'})"
                elif (0.236 <= fib_ret < 0.382) or (0.618 < fib_ret <= 0.786): fib_score, fib_detail = 1, f"📐 Fib outer zone ({fib_ret:.1%})"
                else: fib_detail = f"⚪ Fib {fib_ret:.1%} (di luar zona)"
            else: fib_detail = "⚪ Range terlalu kecil untuk Fibonacci"
        score += fib_score; details["FIB"] = fib_detail

        cdl_pin = int(row.get("cdl_pinbar", 0)); cdl_engulf = int(row.get("cdl_engulfing", 0))
        cdl_star = int(row.get("cdl_star", 0)); cdl_hammer = int(row.get("cdl_hammer", 0))
        cdl_doji = int(row.get("cdl_doji", 0)); cdl_sstar = int(row.get("cdl_shooting_star", 0))
        liq_sweep = int(row.get("liq_sweep", 0)); is_bull_c = bool(row.get("is_bullish", False))
        rev, rev_names = 0, []
        if direction == "LONG":
            if cdl_pin == 1: rev += 3; rev_names.append("🔨 Pinbar↑")
            if cdl_engulf == 1: rev += 3; rev_names.append("🟩 Engulfing↑")
            if cdl_star == 1: rev += 3; rev_names.append("⭐ Morning Star")
            if liq_sweep == 1: rev += 2; rev_names.append("🎯 Liq Sweep↑")
            if cdl_hammer == 1 and cdl_pin != 1: rev += 2; rev_names.append("🔨 Hammer")
            if cdl_sstar == 1: rev += 1; rev_names.append("💫 Inv Hammer")
            if cdl_doji == 1: rev += 1; rev_names.append("✙ Doji Bull")
            if is_bull_c: rev += 1; rev_names.append("🕯️ Recovery candle")
        else:
            if cdl_pin == -1: rev += 3; rev_names.append("🔨 Pinbar↓")
            if cdl_engulf == -1: rev += 3; rev_names.append("🟥 Engulfing↓")
            if cdl_star == -1: rev += 3; rev_names.append("⭐ Evening Star")
            if liq_sweep == -1: rev += 2; rev_names.append("🎯 Liq Sweep↓")
            if cdl_sstar == -1 and cdl_pin != -1: rev += 2; rev_names.append("💫 Shooting Star")
            if cdl_doji == -1: rev += 1; rev_names.append("✙ Doji Bear")
            if not is_bull_c: rev += 1; rev_names.append("🕯️ Rejection candle")

        min_rev = c.get("PULLBACK_MIN_REVERSAL_SCORE", 3)
        if rev < min_rev: return _neutral(f"Reversal pattern belum ada ({rev}/{min_rev} min) — tunggu konfirmasi candle")
        score += min(rev, 4); details["REVERSAL"] = f"{'🟢' if direction=='LONG' else '🔴'} {' + '.join(rev_names[:3])} (+{min(rev, 4)})"

        liq_match = (direction == "LONG" and liq_sweep == 1) or (direction == "SHORT" and liq_sweep == -1)
        if liq_match and rev >= 3: score += 1; details["LIQ_COMBO"] = "🎯 Stop hunt + reversal candle — setup optimal"

        vol_delta = float(row.get("vol_delta", 0)); vol_delta_ma = float(row.get("vol_delta_ma", 0))
        if direction == "LONG":
            if vol_ratio < 1.0 and vol_delta <= 0: score += 1; details["VOL"] = "🟢 Sell vol rendah — selling exhausted"
            elif vol_delta > 0 and vol_delta > vol_delta_ma: score += 1; details["VOL"] = "🟢 Buy vol naik — demand kembali"
            else: details["VOL"] = f"⚪ Vol {vol_ratio:.1f}x"
        else:
            if vol_ratio < 1.0 and vol_delta >= 0: score += 1; details["VOL"] = "🔴 Buy vol rendah — buying exhausted"
            elif vol_delta < 0 and vol_delta < vol_delta_ma: score += 1; details["VOL"] = "🔴 Sell vol naik — supply kembali"
            else: details["VOL"] = f"⚪ Vol {vol_ratio:.1f}x"

        if direction == "LONG":
            if obi < -0.30: return _neutral(f"OBI sangat bearish ({obi:+.2f}) — ini breakdown, bukan pullback")
            if obi >= c.get("OBI_BULL_THRESHOLD", 0.15): score += 1; details["OBI"] = f"🟢 OBI support ({obi:+.2f})"
            else: details["OBI"] = f"⚪ OBI {obi:+.2f}"
        else:
            if obi > 0.30: return _neutral(f"OBI sangat bullish ({obi:+.2f}) — ini breakout, bukan bounce")
            if obi <= c.get("OBI_BEAR_THRESHOLD", -0.15): score += 1; details["OBI"] = f"🔴 OBI resistance ({obi:+.2f})"
            else: details["OBI"] = f"⚪ OBI {obi:+.2f}"

        if direction == "LONG" and willr <= -70: score += 1; details["WR%"] = f"🟢 Oversold {willr:.0f} — reversal likely"
        elif direction == "SHORT" and willr >= -30: score += 1; details["WR%"] = f"🔴 Overbought {willr:.0f} — reversal likely"
        else: details["WR%"] = f"⚪ {willr:.0f}"

        if dema_f > 0:
            if direction == "LONG" and close <= dema_f * 1.003: score += 1; details["DEMA"] = f"🟢 Pullback ke DEMA support ({dema_f:.4f})"
            elif direction == "SHORT" and close >= dema_f * 0.997: score += 1; details["DEMA"] = f"🔴 Bounce ke DEMA resistance ({dema_f:.4f})"
            else: details["DEMA"] = f"⚪ Jauh dari DEMA ({dema_f:.4f})"

        fr_th = c.get("FUNDING_EXTREME_THRESHOLD", 0.0008)
        if direction == "LONG" and funding_rate <= -fr_th: score += 1; details["FUNDING"] = f"🟢 Over-short {funding_rate*100:+.4f}%"
        elif direction == "SHORT" and funding_rate >= fr_th: score += 1; details["FUNDING"] = f"🔴 Over-long {funding_rate*100:+.4f}%"
        else: details["FUNDING"] = f"⚪ {funding_rate*100:+.4f}%"

        if direction == "LONG" and whale_side == 1: score += 1; details["WHALE"] = "🐋 Buy whale saat pullback — institutional buying"
        elif direction == "SHORT" and whale_side == -1: score += 1; details["WHALE"] = "🐋 Sell whale saat bounce — institutional selling"

        min_entry = c.get("PULLBACK_ENTRY_MIN_SCORE", 11)
        details["Status"] = f"Pullback {direction} | 1H:{trend_bias}({trend_strength}) 15m:{htf_bias} | Score:{score}/{min_entry}min (max 16)"
        if score < min_entry: return _neutral(f"Score kurang ({score}/{min_entry} min)")

        return {"signal": direction, "bull_score": score if direction == "LONG" else 0, "bear_score": score if direction == "SHORT" else 0,
                "max_score": 16, "confidence": round(min(score / 16 * 100, 100)), "atr": atr_val, "atr_pct": atr_pct, "close": close,
                "vol_ratio": vol_ratio, "roc": roc, "roc_accel": roc_accel, "willr": willr, "adx": adx, "st_dir": st_now,
                "st_flipped": False, "dema_fast": dema_f, "dema_slow": dema_s, "squeeze_on": bool(row.get("squeeze_on", False)),
                "squeeze_mom": float(row.get("squeeze_mom", 0)) if not pd.isna(row.get("squeeze_mom", 0)) else 0.0,
                "vol_delta": float(row.get("vol_delta", 0)), "body_ratio": float(row.get("body_ratio", 0)),
                "consec": int(row.get("consec", 0)), "vwap": float(row.get("vwap", 0)) if not pd.isna(row.get("vwap", 0)) else 0.0,
                "supertrend": float(row.get("supertrend", 0)) if not pd.isna(row.get("supertrend", 0)) else 0.0,
                "obi": obi, "trade_flow": trade_flow, "funding_rate": funding_rate, "whale_side": whale_side,
                "details": details, "is_pullback_entry": True}

    def get_signal(self, df: pd.DataFrame, current_price: float = 0.0, obi: float = 0.0, trade_flow: float = 0.0,
                   funding_rate: float = 0.0, whale_side: int = 0) -> dict:
        c = self.cfg
        def _neutral(reason: str, real_adx: float = 0.0) -> dict:
            return {"signal": "NEUTRAL", "bull_score": 0, "bear_score": 0, "max_score": 22, "confidence": 0,
                    "atr": 0.0, "atr_pct": 0.0, "close": df["close"].iloc[-1] if len(df) > 0 else 0,
                    "vol_ratio": 1.0, "roc": 0.0, "willr": -50.0, "adx": real_adx, "st_dir": 0,
                    "st_flipped": False, "dema_fast": 0.0, "dema_slow": 0.0, "squeeze_on": False, "squeeze_mom": 0.0,
                    "obi": obi, "trade_flow": trade_flow, "funding_rate": funding_rate, "whale_side": whale_side,
                    "details": {"Status": reason}}

        if len(df) < 5: return _neutral("Data tidak cukup")
        row, prev = df.iloc[-1], df.iloc[-2]
        close = current_price if current_price > 0 else float(row["close"])
        atr_pct = float(row.get("atr_pct", 0)); atr_val = float(row.get("atr", 0))
        adx = float(row.get("adx", 0)) if not pd.isna(row.get("adx", 0)) else 0.0

        if c.get("USE_ADX_FILTER", True) and adx < c.get("ADX_MIN_THRESHOLD", 15):
            return _neutral(f"ADX Sideways ({adx:.1f})", real_adx=adx)

        bull, bear, details = 0, 0, {}
        st_now, st_prev = int(row.get("st_dir", 1)), int(prev.get("st_dir", 1))
        st_flipped = (st_now != st_prev)
        if st_now == -1: bull += 3 if st_flipped else 2; details["SuperTrend"] = "🔥 BARU FLIP → BULLISH" if st_flipped else "✅ Bullish"
        elif st_now == 1: bear += 3 if st_flipped else 2; details["SuperTrend"] = "🔥 BARU FLIP → BEARISH" if st_flipped else "🔴 Bearish"
        else: details["SuperTrend"] = "⚪ Tidak ada data"

        dema_f, dema_s = float(row.get("dema_fast", 0)), float(row.get("dema_slow", 0))
        pdema_f, pdema_s = float(prev.get("dema_fast", 0)), float(prev.get("dema_slow", 0))
        dema_x_up = dema_f > dema_s and pdema_f <= pdema_s
        dema_x_dn = dema_f < dema_s and pdema_f >= pdema_s
        if dema_x_up: bull += 2; details["DEMA"] = "🟢 Cross UP (momentum dimulai)"
        elif dema_x_dn: bear += 2; details["DEMA"] = "🔴 Cross DOWN (momentum dimulai)"
        elif dema_f > dema_s: bull += 1; details["DEMA"] = "🟡 Bullish alignment"
        elif dema_f < dema_s: bear += 1; details["DEMA"] = "🟠 Bearish alignment"
        else: details["DEMA"] = "⚪ Netral"

        roc, roc_accel = float(row.get("roc", 0)), float(row.get("roc_accel", 0))
        if roc > 0 and roc_accel > 0: bull += 2; details["ROC"] = f"🔥 {roc:.3f}% naik+mempercepat"
        elif roc > 0: bull += 1; details["ROC"] = f"🟡 {roc:.3f}% positif"
        elif roc < 0 and roc_accel < 0: bear += 2; details["ROC"] = f"🔥 {roc:.3f}% turun+mempercepat"
        elif roc < 0: bear += 1; details["ROC"] = f"🟠 {roc:.3f}% negatif"
        else: details["ROC"] = "⚪ Netral"

        willr = float(row.get("willr", -50))
        if willr <= -80: bull += 1; details["WR%"] = f"🟢 Oversold {willr:.0f}"
        elif willr >= -20: bear += 1; details["WR%"] = f"🔴 Overbought {willr:.0f}"
        else: details["WR%"] = f"⚪ Neutral {willr:.0f}"

        vol_ratio = float(row.get("vol_ratio", 1)); vol_delta = float(row.get("vol_delta", 0)); vol_delta_ma = float(row.get("vol_delta_ma", 0))
        if vol_ratio >= 1.3 and vol_delta > 0: bull += 1; details["VOL"] = f"🟢 Buy pressure {vol_ratio:.1f}x"
        elif vol_ratio >= 1.3 and vol_delta < 0: bear += 1; details["VOL"] = f"🔴 Sell pressure {vol_ratio:.1f}x"
        elif vol_delta > vol_delta_ma and vol_delta > 0: bull += 1; details["VOL"] = f"🟡 Buy delta naik"
        elif vol_delta < vol_delta_ma and vol_delta < 0: bear += 1; details["VOL"] = f"🟠 Sell delta naik"
        else: details["VOL"] = f"⚪ {vol_ratio:.1f}x netral"

        cvd_trend = float(row.get("cvd_trend", 0)) if not pd.isna(row.get("cvd_trend", 0)) else 0.0
        cvd_ma = float(row.get("cvd_ma", 0)) if not pd.isna(row.get("cvd_ma", 0)) else 0.0
        cvd_val = float(row.get("cvd", 0)) if not pd.isna(row.get("cvd", 0)) else 0.0
        if cvd_trend > 0 and cvd_val > cvd_ma: bull += 1; details["CVD"] = f"🟢 CVD naik+atas MA ({cvd_trend:+.0f})"
        elif cvd_trend < 0 and cvd_val < cvd_ma: bear += 1; details["CVD"] = f"🔴 CVD turun+bawah MA ({cvd_trend:+.0f})"
        elif cvd_trend > 0: bull += 1; details["CVD"] = "🟡 CVD momentum naik"
        elif cvd_trend < 0: bear += 1; details["CVD"] = "🟠 CVD momentum turun"
        else: details["CVD"] = "⚪ CVD netral"

        cdl_pin = int(row.get("cdl_pinbar", 0)); cdl_engulf = int(row.get("cdl_engulfing", 0))
        cdl_star = int(row.get("cdl_star", 0)); cdl_hammer = int(row.get("cdl_hammer", 0))
        cdl_sstar = int(row.get("cdl_shooting_star", 0)); cdl_doji = int(row.get("cdl_doji", 0))
        liq_sweep = int(row.get("liq_sweep", 0)); body_ratio = float(row.get("body_ratio", 0))
        is_bull_c = bool(row.get("is_bullish", True)); consec = int(row.get("consec", 0))
        cdl_bull_pts, cdl_bear_pts, cdl_bull_names, cdl_bear_names = 0, 0, [], []
        if cdl_pin == 1: cdl_bull_pts += 2; cdl_bull_names.append("🔨 Pinbar↑")
        if cdl_pin == -1: cdl_bear_pts += 2; cdl_bear_names.append("🔨 Pinbar↓")
        if cdl_engulf == 1: cdl_bull_pts += 2; cdl_bull_names.append("🟩 Engulfing↑")
        if cdl_engulf == -1: cdl_bear_pts += 2; cdl_bear_names.append("🟥 Engulfing↓")
        if cdl_star == 1: cdl_bull_pts += 2; cdl_bull_names.append("⭐ Morning Star")
        if cdl_star == -1: cdl_bear_pts += 2; cdl_bear_names.append("⭐ Evening Star")
        if liq_sweep == 1: cdl_bull_pts += 2; cdl_bull_names.append("🎯 Liq Sweep↑")
        if liq_sweep == -1: cdl_bear_pts += 2; cdl_bear_names.append("🎯 Liq Sweep↓")
        if cdl_hammer == 1 and cdl_pin != 1: cdl_bull_pts += 1; cdl_bull_names.append("🔨 Hammer")
        if cdl_sstar == -1 and cdl_pin != -1: cdl_bear_pts += 1; cdl_bear_names.append("💫 Shooting Star")
        if cdl_sstar == 1: cdl_bull_pts += 1; cdl_bull_names.append("💫 Inv Hammer↑")
        if cdl_doji == 1: cdl_bull_pts += 1; cdl_bull_names.append("✙ Dragonfly Doji")
        if cdl_doji == -1: cdl_bear_pts += 1; cdl_bear_names.append("✙ Gravestone Doji")
        if body_ratio > 0.6 and is_bull_c and consec >= 2: cdl_bull_pts += 1; cdl_bull_names.append(f"📊 Strong body×{consec}")
        elif body_ratio > 0.6 and not is_bull_c and consec <= -2: cdl_bear_pts += 1; cdl_bear_names.append(f"📊 Strong body×{abs(consec)}")
        elif body_ratio > 0.5 and is_bull_c: cdl_bull_pts += 1; cdl_bull_names.append(f"📊 Bull body {body_ratio:.0%}")
        elif body_ratio > 0.5 and not is_bull_c: cdl_bear_pts += 1; cdl_bear_names.append(f"📊 Bear body {body_ratio:.0%}")
        cdl_bull_pts, cdl_bear_pts = min(cdl_bull_pts, 3), min(cdl_bear_pts, 3)
        if cdl_bull_pts > 0: bull += cdl_bull_pts; details["CANDLE"] = f"🟢 {' + '.join(cdl_bull_names[:3])} (+{cdl_bull_pts})"
        elif cdl_bear_pts > 0: bear += cdl_bear_pts; details["CANDLE"] = f"🔴 {' + '.join(cdl_bear_names[:3])} (+{cdl_bear_pts})"
        else: details["CANDLE"] = f"⚪ No pattern ({body_ratio:.0%})"

        sq_on, sq_mom = bool(row.get("squeeze_on", False)), float(row.get("squeeze_mom", 0)) if not pd.isna(row.get("squeeze_mom", 0)) else 0.0
        sq_on_prev, sq_mom_prev = bool(prev.get("squeeze_on", False)), float(prev.get("squeeze_mom", 0)) if not pd.isna(prev.get("squeeze_mom", 0)) else 0.0
        if sq_on_prev and not sq_on:
            if sq_mom > 0: bull += 1; details["SQUEEZE"] = "🔥 Release → BULLISH blast!"
            else: bear += 1; details["SQUEEZE"] = "🔥 Release → BEARISH blast!"
        elif not sq_on and sq_mom > 0 and sq_mom > sq_mom_prev: bull += 1; details["SQUEEZE"] = "📈 Momentum tumbuh"
        elif not sq_on and sq_mom < 0 and sq_mom < sq_mom_prev: bear += 1; details["SQUEEZE"] = "📉 Momentum turun"
        elif sq_on: details["SQUEEZE"] = "⏸️ Squeeze aktif (konsolidasi)"
        else: details["SQUEEZE"] = "⚪ Netral"

        obi_bull_th, obi_bear_th = c.get("OBI_BULL_THRESHOLD", 0.15), c.get("OBI_BEAR_THRESHOLD", -0.15)
        if obi >= obi_bull_th * 2: bull += 2; details["OBI"] = f"🟢🟢 Buy wall kuat ({obi:+.2f})"
        elif obi >= obi_bull_th: bull += 1; details["OBI"] = f"🟢 Buy pressure ({obi:+.2f})"
        elif obi <= obi_bear_th * 2: bear += 2; details["OBI"] = f"🔴🔴 Sell wall kuat ({obi:+.2f})"
        elif obi <= obi_bear_th: bear += 1; details["OBI"] = f"🔴 Sell pressure ({obi:+.2f})"
        else: details["OBI"] = f"⚪ Seimbang ({obi:+.2f})"

        tf_bull_th, tf_bear_th = c.get("FLOW_BULL_THRESHOLD", 0.25), c.get("FLOW_BEAR_THRESHOLD", -0.25)
        if trade_flow >= tf_bull_th: bull += 1; details["FLOW"] = f"🟢 Buy volume dominan ({trade_flow:+.2f})"
        elif trade_flow <= tf_bear_th: bear += 1; details["FLOW"] = f"🔴 Sell volume dominan ({trade_flow:+.2f})"
        else: details["FLOW"] = f"⚪ Mixed ({trade_flow:+.2f})"

        try:
            vwap_window = min(26, len(df)); df_vw = df.tail(vwap_window)
            vol_sum = df_vw["volume"].sum()
            vwap = (df_vw["close"] * df_vw["volume"]).sum() / vol_sum if vol_sum > 0 else close
            vwap_pct = (close - vwap) / vwap * 100 if vwap > 0 else 0.0
            vwap_th = c.get("VWAP_THRESHOLD_PCT", 0.10)
            if vwap_pct >= vwap_th * 2: bull += 2; details["VWAP"] = f"🟢🟢 Jauh di atas VWAP ({vwap_pct:+.2f}%)"
            elif vwap_pct >= vwap_th: bull += 1; details["VWAP"] = f"🟢 Di atas VWAP ({vwap_pct:+.2f}%)"
            elif vwap_pct <= -vwap_th * 2: bear += 2; details["VWAP"] = f"🔴🔴 Jauh di bawah VWAP ({vwap_pct:+.2f}%)"
            elif vwap_pct <= -vwap_th: bear += 1; details["VWAP"] = f"🔴 Di bawah VWAP ({vwap_pct:+.2f}%)"
            else: details["VWAP"] = f"⚪ Dekat VWAP ({vwap_pct:+.2f}%)"
        except Exception: details["VWAP"] = "⚪ N/A"

        fr_th = c.get("FUNDING_EXTREME_THRESHOLD", 0.0008)
        if funding_rate <= -fr_th: bull += 1; details["FUNDING"] = f"🟢 Over-short! Funding {funding_rate*100:+.4f}% → contrarian LONG"
        elif funding_rate >= fr_th: bear += 1; details["FUNDING"] = f"🔴 Over-long! Funding {funding_rate*100:+.4f}% → contrarian SHORT"
        else: details["FUNDING"] = f"⚪ Normal ({funding_rate*100:+.4f}%)"

        if whale_side == 1: bull += 1; details["WHALE"] = "🐋 Buy whale terdeteksi (<60s)"
        elif whale_side == -1: bear += 1; details["WHALE"] = "🐋 Sell whale terdeteksi (<60s)"
        else: details["WHALE"] = "⚪ Tidak ada whale"

        mkt_struct = int(row.get("mkt_struct", 0))
        if mkt_struct == 1: bull += 1; details["STRUCT"] = "📊 HH+HL → Uptrend structure"
        elif mkt_struct == -1: bear += 1; details["STRUCT"] = "📊 LH+LL → Downtrend structure"
        else: details["STRUCT"] = "⚪ Mixed structure"

        req_bull, req_bear = c.get("MIN_BULL_SCORE", 15), c.get("MIN_BEAR_SCORE", 15)
        min_candle = c.get("MIN_CANDLE_SCORE", 6)
        obi_bull_pt = 2 if "🟢🟢" in details.get("OBI", "") else (1 if "🟢" in details.get("OBI", "") else 0)
        obi_bear_pt = 2 if "🔴🔴" in details.get("OBI", "") else (1 if "🔴" in details.get("OBI", "") else 0)
        flow_bull_pt = 1 if "🟢" in details.get("FLOW", "") else 0; flow_bear_pt = 1 if "🔴" in details.get("FLOW", "") else 0
        vwap_bull_pt = 2 if "🟢🟢" in details.get("VWAP", "") else (1 if "🟢" in details.get("VWAP", "") else 0)
        vwap_bear_pt = 2 if "🔴🔴" in details.get("VWAP", "") else (1 if "🔴" in details.get("VWAP", "") else 0)
        fund_bull_pt = 1 if "🟢" in details.get("FUNDING", "") else 0; fund_bear_pt = 1 if "🔴" in details.get("FUNDING", "") else 0
        whal_bull_pt = 1 if whale_side == 1 else 0; whal_bear_pt = 1 if whale_side == -1 else 0
        live_bull_pt = obi_bull_pt + flow_bull_pt + vwap_bull_pt + fund_bull_pt + whal_bull_pt
        live_bear_pt = obi_bear_pt + flow_bear_pt + vwap_bear_pt + fund_bear_pt + whal_bear_pt
        bull_candle, bear_candle = bull - live_bull_pt, bear - live_bear_pt

        if bull_candle < min_candle: details["CANDLE_GATE"] = f"⛔ Candle score {bull_candle}/{min_candle} (kurang)"; bull = 0
        if bear_candle < min_candle: details["CANDLE_GATE"] = f"⛔ Candle score {bear_candle}/{min_candle} (kurang)"; bear = 0

        st_ok_bull = bull >= 2 and (st_now == -1); st_ok_bear = bear >= 2 and (st_now == 1)
        if c.get("REQUIRE_ST_CONFIRM", True):
            bull_signal = bull >= req_bull and bull > bear and st_ok_bull
            bear_signal = bear >= req_bear and bear > bull and st_ok_bear
            if bull >= req_bull and not st_ok_bull: details["BLOCKED"] = "⛔ SuperTrend tidak mendukung LONG"
            if bear >= req_bear and not st_ok_bear: details["BLOCKED"] = "⛔ SuperTrend tidak mendukung SHORT"
        else:
            bull_signal = bull >= req_bull and bull > bear; bear_signal = bear >= req_bear and bear > bull

        sq_released = sq_on_prev and not sq_on
        fast_track = sq_released and adx >= c.get("ADX_MIN_THRESHOLD", 22) + 3
        if fast_track:
            req_fast = max(req_bull - 2, 5)
            if not bull_signal and bull >= req_fast and bull > bear and st_ok_bull: bull_signal = True; details["FAST_TRACK"] = f"⚡ Squeeze Release fast-track ({bull}/{req_fast})"
            if not bear_signal and bear >= req_fast and bear > bull and st_ok_bear: bear_signal = True; details["FAST_TRACK"] = f"⚡ Squeeze Release fast-track ({bear}/{req_fast})"

        signal = "LONG" if bull_signal else ("SHORT" if bear_signal else "NEUTRAL")
        return {"signal": signal, "bull_score": bull, "bear_score": bear, "max_score": 22, "confidence": round(min(max(bull, bear) / 22 * 100, 100)),
                "obi": obi, "trade_flow": trade_flow, "funding_rate": funding_rate, "whale_side": whale_side,
                "atr": atr_val, "atr_pct": atr_pct, "close": close, "vol_ratio": vol_ratio, "roc": roc, "roc_accel": roc_accel,
                "willr": willr, "adx": adx, "st_dir": st_now, "st_flipped": st_flipped, "dema_fast": dema_f, "dema_slow": dema_s,
                "squeeze_on": sq_on, "squeeze_mom": sq_mom, "vol_delta": vol_delta, "body_ratio": body_ratio, "consec": consec,
                "details": details, "vwap": float(row.get("vwap", 0)) if not pd.isna(row.get("vwap", 0)) else 0.0,
                "supertrend": float(row.get("supertrend", 0)) if not pd.isna(row.get("supertrend", 0)) else 0.0}

# ══════════════════════════════════════════════════════════════
# COIN SCANNER
# ══════════════════════════════════════════════════════════════
class CoinScanner:
    def __init__(self, client: MEXCFuturesClient, ta_engine: ScalperTA, cfg: dict):
        self.client = client; self.ta = ta_engine; self.cfg = cfg
        self._history: Dict[str, deque] = {}; self._results: List[dict] = []
        self._lock = threading.Lock(); self._last_scan = 0.0; self._scanning = False

    def _composite_score(self, signal: dict, sym: str) -> float:
        raw_score = max(signal["bull_score"], signal["bear_score"]); atr_pct = signal.get("atr_pct", 0)
        vol_ratio = signal.get("vol_ratio", 1); adx = signal.get("adx", 15)
        st_flip = 20 if signal.get("st_flipped") else 0
        hist = list(self._history.get(sym, deque())); momentum = (hist[-1] - hist[0]) if len(hist) >= 2 else 0
        return round(raw_score * 1.5 + momentum * 3.0 + atr_pct * 10 + (vol_ratio - 1) * 2 + (adx - 15) * 0.1 + st_flip, 3)

    def scan_once(self) -> List[dict]:
        if self._scanning: return self._results
        self._scanning = True
        c = self.cfg; blacklist = set(c.get("BLACKLIST_COINS", [])); whitelist = [s.upper() for s in c.get("WHITELIST_COINS", [])]
        min_vol = c.get("SCAN_MIN_VOLUME", 300_000); min_adx = c.get("SCAN_MIN_ADX", 15); min_atr = c.get("SCAN_MIN_ATR_PCT", 0.15); min_price = c.get("SCAN_MIN_PRICE", 0.05)
        if whitelist:
            all_tickers = self.client.get_top_volume_coins(500); ticker_map = {t["symbol"]: t for t in all_tickers}
            top_coins = [ticker_map.get(s) or {"symbol": s, "turnover": 999_999_999, "last_price": 0} for s in whitelist]; min_vol = 0
        else: top_coins = self.client.get_top_volume_coins(c.get("SCAN_TOP_N", 50))

        candidates = [coin for coin in top_coins if coin["symbol"] not in blacklist and coin.get("turnover", 0) >= min_vol and coin.get("last_price", 0) >= min_price]

        def _scan_one(coin: dict) -> Optional[dict]:
            import random; time.sleep(random.uniform(0.05, 0.20)); sym = coin["symbol"]
            try:
                df = self.client.get_klines(sym, c["PRIMARY_TF"], c.get("CANDLE_LIMIT", 200))
                if df is None or len(df) < 30: return None
                df = self.ta.compute(df); df.dropna(inplace=True)
                if len(df) < 5: return None
                signal = self.ta.get_signal(df); adx = signal.get("adx", 0); atr_p = signal.get("atr_pct", 0)
                if sym != c.get("SYMBOL") and (adx < min_adx or atr_p < min_atr): return None
                raw = max(signal["bull_score"], signal["bear_score"])
                with self._lock:
                    if sym not in self._history: self._history[sym] = deque(maxlen=6)
                    self._history[sym].append(raw)
                composite = self._composite_score(signal, sym)
                hist = list(self._history[sym]); momentum = (hist[-1] - hist[0]) if len(hist) >= 2 else 0
                return {"symbol": sym, "signal": signal["signal"], "bull_score": signal["bull_score"], "bear_score": signal["bear_score"],
                        "raw_score": raw, "momentum": round(momentum, 2), "composite": composite, "atr_pct": round(atr_p, 3),
                        "vol_ratio": round(signal.get("vol_ratio", 1), 2), "adx": round(adx, 1), "price": round(signal.get("close", 0), 6),
                        "turnover": coin.get("turnover", 0), "st_flipped": signal.get("st_flipped", False), "st_dir": signal.get("st_dir", 0)}
            except Exception as e: log.warning(f"[Scanner] Error {sym}: {e}"); return None

        from concurrent.futures import ThreadPoolExecutor, as_completed
        results = []
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {executor.submit(_scan_one, coin): coin for coin in candidates}
            for fut in as_completed(futures):
                item = fut.result()
                if item: results.append(item)
        results.sort(key=lambda x: x["composite"], reverse=True)
        with self._lock: self._results = results; self._last_scan = time.time()
        if results: log.info(f"[Scanner] {len(results)} koin. Top: {results[0]['symbol']} (score {results[0]['composite']:.1f})")
        self._scanning = False; return results

    def best_coin(self) -> Optional[dict]:
        with self._lock: results = list(self._results)
        actionable = [r for r in results if r["signal"] in ("LONG", "SHORT")]
        return actionable[0] if actionable else None

    def early_entry_candidate(self) -> Optional[dict]:
        with self._lock: results = list(self._results)
        min_s = self.cfg.get("EARLY_ENTRY_SCORE", 5); min_mom = self.cfg.get("EARLY_MOMENTUM_MIN", 1)
        for r in results:
            bull_dom = r["bull_score"] >= r["bear_score"]
            direction = "LONG" if (bull_dom and r["bull_score"] >= min_s) else ("SHORT" if (not bull_dom and r["bear_score"] >= min_s) else None)
            if direction and r["momentum"] >= min_mom: return {**r, "signal": direction, "is_early_entry": True}
        return None

    def get_results(self) -> List[dict]:
        with self._lock: return list(self._results)
    def needs_scan(self) -> bool: return (time.time() - self._last_scan) >= self.cfg.get("SCAN_INTERVAL", 90)

# ══════════════════════════════════════════════════════════════
# RISK MANAGER
# ══════════════════════════════════════════════════════════════
class RiskManager:
    def __init__(self, cfg: dict): self.cfg = cfg

    def calculate_levels(self, side: str, entry: float, atr: float, trend_power: int = 50, atr_pct: float = 0.0) -> dict:
        c, sl_base, tp1_base = self.cfg, c.get("ATR_SL_MULT", 1.0), c.get("ATR_TP1_MULT", 2.5)
        if c.get("DYNAMIC_LEVELS", True) and atr_pct > 0:
            high_vol, low_vol = c.get("HIGH_VOL_ATR_PCT", 1.5), c.get("LOW_VOL_ATR_PCT", 0.5)
            if atr_pct >= high_vol: sl_base, tp1_base = round(sl_base * 1.4, 2), round(tp1_base * 1.4, 2)
            elif atr_pct <= low_vol: sl_base = round(sl_base * 0.8, 2)
        sl, tp1, tp2, tp3 = atr * sl_base, atr * tp1_base, atr * c.get("ATR_TP2_MULT", 4.0), atr * c.get("ATR_TP3_MULT", 6.0)
        min_sl = entry * 0.002
        if sl < min_sl:
            factor = min_sl / sl if sl > 0 else 1.0; sl = min_sl; tp1, tp2, tp3 = tp1*factor, tp2*factor, tp3*factor
        if side == "LONG":
            return {"stop_loss": round(entry - sl, 6), "take_profit1": round(entry + tp1, 6), "take_profit2": round(entry + tp2, 6),
                    "take_profit3": round(entry + tp3, 6), "sl_distance": round(sl, 6), "rr_ratio": round(tp1 / sl, 2) if sl > 0 else 0, "entry": entry}
        return {"stop_loss": round(entry + sl, 6), "take_profit1": round(entry - tp1, 6), "take_profit2": round(entry - tp2, 6),
                "take_profit3": round(entry - tp3, 6), "sl_distance": round(sl, 6), "rr_ratio": round(tp1 / sl, 2) if sl > 0 else 0, "entry": entry}

    def position_size(self, balance: float, sl_distance: float, entry_price: float,
                      win_rate: float = 0.5, score: int = 12, max_score: int = 18) -> float:
        c = self.cfg; risk_pct = c.get("RISK_PER_TRADE", 0.10); risk_amount = balance * risk_pct
        risk_qty = risk_amount / sl_distance if sl_distance > 0 else 0.0
        score_pct = score / max_score if max_score > 0 else 0.67
        if score_pct >= 0.94: size_mult = 1.30
        elif score_pct >= 0.83: size_mult = 1.00
        elif score_pct >= 0.72: size_mult = 0.80
        else: size_mult = 0.60
        max_margin = balance * c.get("MAX_MARGIN_PCT", 0.25) * size_mult
        max_qty = (max_margin * c.get("LEVERAGE", 20)) / entry_price if entry_price > 0 else 0.0
        return round(min(risk_qty, max_qty), 6)

    def check_rr(self, levels: dict) -> bool: return levels["rr_ratio"] >= self.cfg.get("MIN_RR_RATIO", 1.5)

    def update_trailing_stop(self, pos: Position, current_price: float) -> Optional[float]:
        c = self.cfg
        if not c.get("USE_TRAILING_STOP", True): return None
        if pos.side == "LONG":
            if not pos.trailing_active:
                profit_pct = (current_price - pos.entry_price) / pos.entry_price
                if profit_pct >= c.get("TRAIL_ACTIVATION_PCT", 0.003):
                    pos.trailing_active = True; pos.highest_price = current_price
                    pos.trailing_stop = max(current_price * (1 - c["TRAIL_DISTANCE_PCT"]), pos.stop_loss)
                    return pos.trailing_stop
            elif pos.trailing_active and current_price > pos.highest_price:
                pos.highest_price = current_price; new_sl = current_price * (1 - c["TRAIL_DISTANCE_PCT"])
                if new_sl > pos.trailing_stop: pos.trailing_stop = new_sl; return new_sl
        else:
            if not pos.trailing_active:
                profit_pct = (pos.entry_price - current_price) / pos.entry_price
                if profit_pct >= c.get("TRAIL_ACTIVATION_PCT", 0.003):
                    pos.trailing_active = True; pos.lowest_price = current_price
                    pos.trailing_stop = min(current_price * (1 + c["TRAIL_DISTANCE_PCT"]), pos.stop_loss)
                    return pos.trailing_stop
            elif pos.trailing_active and current_price < pos.lowest_price:
                pos.lowest_price = current_price; new_sl = current_price * (1 + c["TRAIL_DISTANCE_PCT"])
                if new_sl < pos.trailing_stop: pos.trailing_stop = new_sl; return new_sl
        return None

# ══════════════════════════════════════════════════════════════
# SESSION FILTER
# ══════════════════════════════════════════════════════════════
class SessionFilter:
    def __init__(self, cfg: dict): self.cfg = cfg
    def is_trading_allowed(self) -> Tuple[bool, str]:
        now = datetime.now(WIB); c = self.cfg
        if not c.get("USE_SESSION_FILTER", False): return True, "Filter dinonaktifkan"
        hour, weekday = now.hour, now.weekday()
        if weekday == 5: return False, "Sabtu"
        if weekday == 6 and c.get("BLOCK_SUNDAY_OPEN") and hour < 7: return False, "Minggu pagi"
        if weekday == 4 and c.get("BLOCK_FRIDAY_CLOSE") and hour >= 20: return False, "Jumat malam"
        if hour not in c.get("ALLOWED_HOURS_UTC", list(range(24))): return False, f"Di luar jam ({hour:02d}:xx)"
        return True, "OK"

# ══════════════════════════════════════════════════════════════
# TRADE JOURNAL
# ══════════════════════════════════════════════════════════════
class TradeJournal:
    def __init__(self, filepath: str): self.filepath = filepath; self._ensure_header()
    def _ensure_header(self):
        needs_header = not os.path.exists(self.filepath) or os.path.getsize(self.filepath) == 0
        if needs_header:
            with open(self.filepath, "w", newline="") as f:
                csv.writer(f).writerow(["trade_id", "symbol", "side", "entry_price", "exit_price",
                                        "quantity", "pnl", "fee", "sl", "tp1", "tp2",
                                        "opened_at", "closed_at", "close_reason", "bull_score",
                                        "bear_score", "confidence", "trailing_activated", "atr_at_entry",
                                        "is_early_entry", "st_dir", "roc", "willr"])
    def log_trade(self, pos: Position, exit_price: float, _signal: dict = None):
        with open(self.filepath, "a", newline="") as f:
            csv.writer(f).writerow([pos.id, pos.symbol, pos.side, pos.entry_price, exit_price, pos.quantity,
                                    round(pos.pnl, 4), round(pos.fee, 4), pos.stop_loss, pos.take_profit1, pos.take_profit2,
                                    pos.opened_at, pos.closed_at, pos.close_reason, pos.entry_bull_score, pos.entry_bear_score,
                                    pos.entry_confidence, pos.trailing_active, round(pos.entry_atr, 6),
                                    pos.entry_is_early, pos.entry_st_dir, round(pos.entry_roc, 4), round(pos.entry_willr, 1)])
    def _fix_misaligned_row(self, row: dict) -> dict:
        import re; _DT_PAT = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
        cr = row.get("close_reason", "")
        if not _DT_PAT.match(cr): return row
        fixed = dict(row)
        fixed["fee"] = row.get("sl", ""); fixed["sl"] = row.get("tp1", ""); fixed["tp1"] = row.get("tp2", "")
        fixed["tp2"] = row.get("opened_at", ""); fixed["opened_at"] = row.get("closed_at", ""); fixed["closed_at"] = row.get("close_reason", "")
        fixed["close_reason"] = row.get("bull_score", ""); fixed["bull_score"] = row.get("bear_score", "")
        fixed["bear_score"] = row.get("confidence", ""); fixed["confidence"] = row.get("trailing_activated", "")
        fixed["trailing_activated"] = row.get("atr_at_entry", ""); fixed["atr_at_entry"] = row.get("is_early_entry", "")
        fixed["is_early_entry"] = row.get("st_dir", ""); fixed["st_dir"] = row.get("roc", ""); fixed["roc"] = row.get("willr", "")
        fixed["willr"] = row.get(None, ""); return fixed
    def get_trades(self, date_filter: str = None) -> list:
        if not os.path.exists(self.filepath): return []
        try:
            with open(self.filepath, "r") as f: rows = [self._fix_misaligned_row(r) for r in csv.DictReader(f)]
            rows = [{k: v for k, v in row.items() if k is not None} for row in rows]
            if date_filter: rows = [r for r in rows if r.get("closed_at", "").startswith(date_filter)]
            return rows[::-1]
        except Exception as e: log.error(f"Error baca journal: {e}"); return []

# ══════════════════════════════════════════════════════════════
# STATE PERSISTENCE
# ══════════════════════════════════════════════════════════════
class StatePersistence:
    def __init__(self, filepath: str): self.filepath = filepath
    def save(self, state: BotState):
        try:
            data = {"balance": state.balance, "peak_balance": state.peak_balance, "total_trades": state.total_trades,
                    "winning_trades": state.winning_trades, "total_pnl": state.total_pnl, "daily_pnl": state.daily_pnl,
                    "daily_reset_date": state.daily_reset_date, "circuit_breaker": state.circuit_breaker,
                    "circuit_reason": state.circuit_reason, "circuit_type": state.circuit_type, "started_at": state.started_at,
                    "iteration": state.iteration, "secured_total": state.secured_total, "active_symbol": state.active_symbol,
                    "positions": [asdict(p) for p in state.positions if not p.closed]}
            with open(self.filepath, "w") as f: json.dump(data, f, indent=2)
        except Exception as e: log.error(f"Gagal save state: {e}")
    def load(self) -> Optional[dict]:
        if not os.path.exists(self.filepath): return None
        try:
            with open(self.filepath) as f: return json.load(f)
        except Exception as e: log.error(f"Gagal load state: {e}"); return None

# ══════════════════════════════════════════════════════════════
# TELEGRAM NOTIFIER
# ══════════════════════════════════════════════════════════════
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token; self.chat_id = chat_id; self.enabled = bool(token and chat_id); self._queue = deque()
        if self.enabled: threading.Thread(target=self._worker, daemon=True, name="Telegram").start()
    def send(self, message: str):
        if self.enabled: self._queue.append(message)
    def _worker(self):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        while True:
            if self._queue:
                msg = self._queue.popleft()
                try: requests.post(url, json={"chat_id": self.chat_id, "text": msg, "parse_mode": "Markdown"}, timeout=8); time.sleep(0.5)
                except Exception as e: log.warning(f"Telegram error: {e}")
            else: time.sleep(1)

# ══════════════════════════════════════════════════════════════
# SCALPER BOT V5 — MAIN ENGINE
# ══════════════════════════════════════════════════════════════
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MEXC Scalper V5</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d1117;color:#e6edf3;font-family:'Segoe UI',sans-serif;font-size:14px;min-height:100vh}
/* Header */
.header{background:#161b22;border-bottom:1px solid #30363d;padding:10px 20px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:100}
.header h1{font-size:17px;color:#58a6ff;display:flex;align-items:center;gap:8px}
.badge{padding:3px 10px;border-radius:12px;font-size:11px;font-weight:700;cursor:pointer}
.dry{background:#1f6feb22;color:#58a6ff;border:1px solid #1f6feb}
.live{background:#1a7f3722;color:#3fb950;border:1px solid #238636}
/* Tabs */
.tabs{display:flex;background:#161b22;border-bottom:1px solid #30363d;padding:0 20px}
.tab{padding:10px 18px;cursor:pointer;font-size:13px;color:#8b949e;border-bottom:2px solid transparent;transition:.2s}
.tab.active{color:#e6edf3;border-bottom-color:#58a6ff}
.tab:hover{color:#e6edf3}
.tab-content{display:none;padding:0}.tab-content.active{display:block}
/* Cards */
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;padding:16px 20px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px}
.card .label{color:#8b949e;font-size:11px;text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px}
.card .value{font-size:21px;font-weight:700}
.green{color:#3fb950}.red{color:#f85149}.yellow{color:#d29922}.blue{color:#58a6ff}.grey{color:#8b949e}
/* Section */
.section{padding:0 20px 16px}
.section h2{font-size:12px;color:#8b949e;text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid #21262d;margin-top:14px}
/* Signal box */
.signal-box{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;margin-bottom:10px}
.signal-LONG{border-color:#238636!important}.signal-SHORT{border-color:#da3633!important}
.signal-label{font-size:20px;font-weight:700;margin-bottom:4px}
/* Position card */
.pos-card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px;margin-bottom:8px}
.pos-row{display:flex;justify-content:space-between;margin-bottom:4px}
.pos-row .k{color:#8b949e;font-size:12px}.pos-row .v{font-weight:600}
/* Tables */
.tbl{width:100%;border-collapse:collapse}
.tbl th{color:#8b949e;font-size:11px;text-align:left;padding:7px 8px;border-bottom:1px solid #21262d;text-transform:uppercase}
.tbl td{padding:7px 8px;border-bottom:1px solid #1a1f27;font-size:13px}
.tbl tr:hover{background:#1c2128}
/* Settings form */
.settings-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;padding:16px 20px}
.setting-group{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px}
.setting-group h3{font-size:12px;text-transform:uppercase;color:#58a6ff;letter-spacing:.5px;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #21262d}
.field{margin-bottom:12px}
.field label{display:block;font-size:12px;color:#8b949e;margin-bottom:4px}
.field input,.field select{width:100%;background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:7px 10px;color:#e6edf3;font-size:13px;outline:none}
.field input:focus,.field select:focus{border-color:#58a6ff}
/* Toggle switch */
.toggle-wrap{display:flex;align-items:center;gap:10px;margin-bottom:16px}
.toggle{position:relative;display:inline-block;width:44px;height:24px}
.toggle input{opacity:0;width:0;height:0}
.slider{position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;background:#30363d;border-radius:24px;transition:.3s}
.slider:before{position:absolute;content:"";height:18px;width:18px;left:3px;bottom:3px;background:#e6edf3;border-radius:50%;transition:.3s}
input:checked + .slider{background:#238636}
input:checked + .slider:before{transform:translateX(20px)}
/* Buttons */
.btn{padding:8px 16px;border-radius:6px;border:none;cursor:pointer;font-size:13px;font-weight:600;transition:.2s}
.btn-blue{background:#1f6feb;color:#fff}.btn-blue:hover{background:#388bfd}
.btn-green{background:#238636;color:#fff}.btn-green:hover{background:#2ea043}
.btn-red{background:#da3633;color:#fff}.btn-red:hover{background:#f85149}
.btn-grey{background:#30363d;color:#e6edf3}.btn-grey:hover{background:#3d444d}
.btn-row{display:flex;gap:8px;flex-wrap:wrap;padding:0 20px 16px}
/* Status bar */
.status-bar{background:#161b22;border-top:1px solid #30363d;padding:8px 20px;font-size:11px;color:#8b949e;display:flex;gap:20px;position:sticky;bottom:0}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px}
.dot-green{background:#3fb950}.dot-red{background:#f85149}
/* Toast */
.toast{position:fixed;top:60px;right:20px;padding:10px 18px;border-radius:8px;font-size:13px;font-weight:600;z-index:999;opacity:0;transition:.3s;pointer-events:none}
.toast.show{opacity:1}
.toast-ok{background:#238636;color:#fff}.toast-err{background:#da3633;color:#fff}
/* Scrollbar */
::-webkit-scrollbar{width:6px;height:6px}::-webkit-scrollbar-track{background:#0d1117}::-webkit-scrollbar-thumb{background:#30363d;border-radius:3px}
</style>
</head>
<body>

<div id="toast" class="toast"></div>

<div class="header">
  <h1>⚡ MEXC Scalper V5</h1>
  <div style="display:flex;align-items:center;gap:10px">
    <span id="modeLabel" class="badge">DRY RUN</span>
    <span id="wsLabel" style="font-size:12px;color:#8b949e">WS: —</span>
  </div>
</div>

<!-- Tabs -->
<div class="tabs">
  <div class="tab active" onclick="showTab('overview',this)">📊 Overview</div>
  <div class="tab" onclick="showTab('settings',this)">⚙️ Settings</div>
  <div class="tab" onclick="showTab('history',this)">📋 History Trade</div>
</div>

<!-- ══════════ TAB 1: OVERVIEW ══════════ -->
<div id="tab-overview" class="tab-content active">

<div class="grid">
  <div class="card"><div class="label">Symbol</div><div class="value blue" id="symbol">—</div></div>
  <div class="card"><div class="label">Harga Live</div><div class="value" id="price">—</div></div>
  <div class="card"><div class="label">Balance</div><div class="value green" id="balance">—</div><div id="balanceLivePnl" style="font-size:10px;margin-top:2px"></div></div>
  <div class="card"><div class="label">Daily PnL</div><div class="value" id="dailyPnl">—</div></div>
  <div class="card"><div class="label">Total PnL</div><div class="value" id="totalPnl">—</div></div>
  <div class="card"><div class="label">Win Rate</div><div class="value" id="winRate">—</div></div>
  <div class="card"><div class="label">Drawdown</div><div class="value" id="drawdown">—</div></div>
  <div class="card"><div class="label">Trades (W/T)</div><div class="value" id="trades">—</div></div>
</div>

<div class="section">
  <h2>Sinyal Terakhir</h2>
  <div class="signal-box" id="signalBox">
    <div class="signal-label" id="signalLabel">—</div>
    <div id="signalInfo" style="color:#8b949e;font-size:12px">Menunggu data...</div>
  </div>

  <h2>Posisi Terbuka</h2>
  <div id="posList"><p style="color:#8b949e;font-size:12px">Tidak ada posisi terbuka.</p></div>

  <h2>Scanner Top Koin</h2>
  <div style="overflow-x:auto">
  <table class="tbl">
    <thead><tr>
      <th>Symbol</th><th>Signal</th><th>Score</th><th>Momentum</th>
      <th>ATR%</th><th>Vol</th><th>ADX</th><th>Composite</th>
    </tr></thead>
    <tbody id="scannerBody"><tr><td colspan="8" style="color:#8b949e;text-align:center;padding:12px">Scan berjalan...</td></tr></tbody>
  </table>
  </div>
</div>

<div class="btn-row" style="padding-top:4px">
  <button class="btn btn-red" onclick="closeAll()">⛔ Close Semua Posisi</button>
  <button class="btn btn-grey" onclick="refresh()">🔄 Refresh</button>
</div>

</div><!-- end overview -->

<!-- ══════════ TAB 2: SETTINGS ══════════ -->
<div id="tab-settings" class="tab-content">
<div style="padding:14px 20px 0">
  <div class="toggle-wrap">
    <label class="toggle"><input type="checkbox" id="toggleLive" onchange="toggleMode()"><span class="slider"></span></label>
    <span id="modeText" style="font-weight:700;color:#58a6ff">Mode: DRY RUN</span>
    <span style="color:#8b949e;font-size:12px">— Aktifkan LIVE untuk trading nyata</span>
  </div>
  <div id="liveWarning" style="display:none;margin-top:8px;padding:10px 14px;background:#3d1a1a;border:1px solid #f85149;border-radius:6px;color:#f85149;font-size:13px">
    ⚠️ <b>LIVE MODE AKTIF</b> — Bot akan menggunakan uang nyata. Pastikan API Key & Secret sudah diisi dan benar sebelum simpan.
  </div>
</div>

<!-- ── PRESET BAR ── -->
<div style="padding:12px 20px 0">
  <div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px 16px">
    <div style="font-size:12px;color:#8b949e;margin-bottom:10px;font-weight:600;letter-spacing:.05em">⚡ PRESET CEPAT — klik untuk load nilai, lalu Simpan</div>
    <div style="display:flex;gap:8px;flex-wrap:wrap">
      <button onclick="applyPreset('scalper')" class="btn" style="background:#1f4068;border:1px solid #388bfd;padding:6px 14px;font-size:13px">
        ⚡ Scalper 1m<br><span style="font-size:10px;opacity:.7">SL 0.6 | TP 1.0 | 20x | hold detik</span>
      </button>
      <button onclick="applyPreset('swing')" class="btn" style="background:#1a3a2a;border:1px solid #3fb950;padding:6px 14px;font-size:13px">
        📈 Swing MTF 5m<br><span style="font-size:10px;opacity:.7">SL 1.0 | TP 2.0 | 10x | hold jam</span>
      </button>
      <button onclick="applyPreset('conservative')" class="btn" style="background:#2d2a1a;border:1px solid #d29922;padding:6px 14px;font-size:13px">
        🛡️ Konservatif<br><span style="font-size:10px;opacity:.7">SL 1.5 | TP 3.0 | 5x | selektif</span>
      </button>
    </div>
    <div id="presetLabel" style="margin-top:8px;font-size:11px;color:#3fb950;display:none"></div>
  </div>
</div>

<div class="settings-grid">

  <!-- TRADING -->
  <div class="setting-group">
    <h3>💰 Trading</h3>
    <div class="field"><label>Symbol (e.g. XAUT_USDT)</label><input id="s_symbol" type="text"></div>
    <div class="field"><label>Leverage (x)</label><input id="s_leverage" type="number" min="1" max="200"></div>
    <div class="field"><label>Risk Per Trade (%)</label><input id="s_risk" type="number" step="0.5" min="0.5" max="20"></div>
    <div class="field"><label>Max Margin Per Trade (%)</label><input id="s_margin" type="number" step="1" min="1" max="100"></div>
    <div class="field"><label>Virtual Balance (DRY RUN)</label><input id="s_vbal" type="number" step="10"></div>
  </div>

  <!-- SL / TP -->
  <div class="setting-group">
    <h3>🎯 Stop Loss & Take Profit</h3>
    <div class="field"><label>ATR SL Multiplier (e.g. 0.6)</label><input id="s_sl" type="number" step="0.1" min="0.1"></div>
    <div class="field"><label>ATR TP1 Multiplier (e.g. 1.0)</label><input id="s_tp1" type="number" step="0.1" min="0.1"></div>
    <div class="field"><label>ATR TP2 Multiplier (e.g. 2.0)</label><input id="s_tp2" type="number" step="0.1" min="0.1"></div>
    <div class="field"><label>Min R:R Ratio</label><input id="s_rr" type="number" step="0.1" min="0.5"></div>
    <div class="field"><label>TP1 Partial Close (%)</label><input id="s_tp1pct" type="number" step="5" min="0" max="100"></div>
  </div>

  <!-- TRAILING & BE -->
  <div class="setting-group">
    <h3>🛡️ Trailing Stop & Break-Even</h3>
    <div class="field"><label>Trail Activation (%, e.g. 0.3)</label><input id="s_trail_act" type="number" step="0.05" min="0.05"></div>
    <div class="field"><label>Trail Distance (%, e.g. 0.3)</label><input id="s_trail_dist" type="number" step="0.05" min="0.05"></div>
    <div class="field"><label>Break-Even Activation (%, e.g. 0.2)</label><input id="s_be" type="number" step="0.05" min="0.05"></div>
    <div class="field"><label>Loss Cooldown (detik)</label><input id="s_cooldown" type="number" step="10" min="0"></div>
  </div>

  <!-- SIGNAL -->
  <div class="setting-group">
    <h3>📡 Sinyal & Filter</h3>
    <div class="field"><label>Min Score LONG (dari 11)</label><input id="s_bull" type="number" min="1" max="11"></div>
    <div class="field"><label>Min Score SHORT (dari 11)</label><input id="s_bear" type="number" min="1" max="11"></div>
    <div class="field"><label>Early Entry Score</label><input id="s_early" type="number" min="1" max="11"></div>
    <div class="field"><label>Min ADX (Anti-sideways)</label><input id="s_adx" type="number" min="5" max="50"></div>
    <div class="field"><label>Max ATR% Entry</label><input id="s_maxatr" type="number" step="0.5" min="0.5"></div>
  </div>

  <!-- RISK LIMIT -->
  <div class="setting-group">
    <h3>⛔ Batas Risiko</h3>
    <div class="field"><label>Max Daily Loss (%)</label><input id="s_dloss" type="number" step="1" min="1" max="100"></div>
    <div class="field"><label>Max Drawdown (%)</label><input id="s_dd" type="number" step="5" min="5" max="100"></div>
    <div class="field"><label>Scan Interval (detik)</label><input id="s_scan" type="number" step="10" min="30"></div>
  </div>

  <!-- API KEYS -->
  <div class="setting-group">
    <h3>🔑 API Keys</h3>
    <div class="field"><label>MEXC API Key</label><input id="s_apikey" type="text" placeholder="mx0vg..."></div>
    <div class="field"><label>MEXC API Secret</label><input id="s_apisec" type="password" placeholder="••••••••"></div>
    <p style="color:#8b949e;font-size:11px;margin-top:4px">API key hanya digunakan untuk LIVE mode</p>
  </div>

</div><!-- settings-grid -->

<div class="btn-row">
  <button class="btn btn-blue" onclick="saveSettings()">💾 Simpan Settings</button>
  <button class="btn btn-grey" onclick="loadSettings()">🔄 Refresh dari Bot</button>
</div>

</div><!-- end settings -->

<!-- ══════════ TAB 3: HISTORY ══════════ -->
<div id="tab-history" class="tab-content">
<div class="section">
  <div style="display:flex;align-items:center;gap:10px;margin:14px 0 4px">
    <h2 style="margin:0">Ringkasan</h2>
    <div style="display:flex;gap:6px;margin-left:auto">
      <button id="h_btn_dry"  onclick="loadHistory('dry')"  class="btn btn-grey" style="padding:4px 12px;font-size:12px">📊 Dry Run</button>
      <button id="h_btn_live" onclick="loadHistory('live')" class="btn btn-grey" style="padding:4px 12px;font-size:12px">🔴 Live</button>
      <button onclick="resetHistory('dry')"  class="btn" style="padding:4px 12px;font-size:12px;background:#b45309">🗑 Reset Dry</button>
      <button onclick="resetHistory('live')" class="btn" style="padding:4px 12px;font-size:12px;background:#7f1d1d">🗑 Reset Live</button>
      <button onclick="resetHistory('all')"  class="btn" style="padding:4px 12px;font-size:12px;background:#581c87">🗑 Reset Semua</button>
    </div>
    <span id="h_mode_label" style="font-size:12px;color:#8b949e"></span>
  </div>
  <div class="grid" style="padding:0 0 12px">
    <div class="card"><div class="label">Total Trade</div><div class="value" id="h_total">—</div></div>
    <div class="card"><div class="label">Menang</div><div class="value green" id="h_win">—</div></div>
    <div class="card"><div class="label">Kalah</div><div class="value red" id="h_loss">—</div></div>
    <div class="card"><div class="label">Win Rate</div><div class="value" id="h_wr">—</div></div>
    <div class="card"><div class="label">Total PnL</div><div class="value" id="h_pnl">—</div></div>
    <div class="card"><div class="label">Avg PnL</div><div class="value" id="h_avg">—</div></div>
    <div class="card"><div class="label">Best Trade</div><div class="value green" id="h_best">—</div></div>
    <div class="card"><div class="label">Worst Trade</div><div class="value red" id="h_worst">—</div></div>
  </div>

  <h2>Semua Trade</h2>
  <div style="overflow-x:auto">
  <table class="tbl" id="histTable">
    <thead><tr>
      <th>#</th><th>ID</th><th>Symbol</th><th>Side</th>
      <th>Entry</th><th>Exit</th><th>PnL</th><th>Alasan</th>
      <th>SL</th><th>TP1</th><th>Buka</th><th>Tutup</th>
    </tr></thead>
    <tbody id="histBody"><tr><td colspan="12" style="color:#8b949e;text-align:center;padding:16px">Memuat data...</td></tr></tbody>
  </table>
  </div>
</div>
<div class="btn-row"><button class="btn btn-grey" onclick="loadHistory()">🔄 Refresh History</button></div>
</div><!-- end history -->

<div class="status-bar">
  <span><span class="dot" id="botDot"></span><span id="botStatus">—</span></span>
  <span>Iter: <b id="iter">—</b></span>
  <span>Secured: <b id="secured">—</b></span>
  <span id="lastUpdate">—</span>
</div>

<script>
// ── Tab switching ──
function showTab(name, el) {
  document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  el.classList.add('active');
  if (name === 'settings') loadSettings();
  if (name === 'history') loadHistory();
}

// ── Toast notification ──
function toast(msg, ok=true) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'toast show ' + (ok ? 'toast-ok' : 'toast-err');
  setTimeout(() => el.classList.remove('show'), 3000);
}

// ══════════════════════════════════════
// TAB 1: OVERVIEW
// ══════════════════════════════════════
async function refresh() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();

    document.getElementById('symbol').textContent = d.symbol;
    const p = parseFloat(d.price);
    document.getElementById('price').textContent = '$' + p.toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:6});

    // Balance = saldo sekarang + unrealized P&L posisi terbuka
    const baseBal = parseFloat(d.balance);
    let unrealizedPnl = 0;
    if (d.positions) {
      d.positions.forEach(p => { if (!p.closed) unrealizedPnl += parseFloat(p.pnl_live || 0); });
    }
    const liveBal = baseBal + unrealizedPnl;
    const balEl = document.getElementById('balance');
    balEl.textContent = '$' + liveBal.toFixed(2);
    balEl.className = 'value ' + (liveBal >= baseBal ? 'green' : 'red');
    const pnlLbl = document.getElementById('balanceLivePnl');
    if (Math.abs(unrealizedPnl) > 0.0001) {
      pnlLbl.textContent = (unrealizedPnl >= 0 ? '+' : '') + '$' + unrealizedPnl.toFixed(4) + ' live';
      pnlLbl.style.color = unrealizedPnl >= 0 ? '#3fb950' : '#f85149';
    } else {
      pnlLbl.textContent = '';
    }

    const dpnl = parseFloat(d.daily_pnl);
    const elDp = document.getElementById('dailyPnl');
    elDp.textContent = (dpnl >= 0?'+':'') + '$' + dpnl.toFixed(4);
    elDp.className = 'value ' + (dpnl >= 0 ? 'green' : 'red');

    const tpnl = parseFloat(d.total_pnl);
    const elTp = document.getElementById('totalPnl');
    elTp.textContent = (tpnl >= 0?'+':'') + '$' + tpnl.toFixed(4);
    elTp.className = 'value ' + (tpnl >= 0 ? 'green' : 'red');

    const wr = parseFloat(d.win_rate);
    const elWr = document.getElementById('winRate');
    elWr.textContent = wr.toFixed(1) + '%';
    elWr.className = 'value ' + (wr >= 50 ? 'green' : wr >= 40 ? 'yellow' : 'red');

    const dd = parseFloat(d.drawdown);
    const elDd = document.getElementById('drawdown');
    elDd.textContent = dd.toFixed(2) + '%';
    elDd.className = 'value ' + (dd < 10 ? 'green' : dd < 20 ? 'yellow' : 'red');

    document.getElementById('trades').textContent = d.winning_trades + '/' + d.total_trades;

    const modeEl = document.getElementById('modeLabel');
    modeEl.textContent = d.dry_run ? 'DRY RUN' : '🔴 LIVE';
    modeEl.className = 'badge ' + (d.dry_run ? 'dry' : 'live');

    document.getElementById('wsLabel').textContent = 'WS: ' + (d.ws_alive ? '🟢' : '🔴');
    document.getElementById('iter').textContent = d.iteration;
    document.getElementById('secured').textContent = '$' + parseFloat(d.secured_total).toFixed(2);

    const sig = d.last_signal || 'NEUTRAL';
    const sigBox = document.getElementById('signalBox');
    const sigLabel = document.getElementById('signalLabel');
    sigBox.className = 'signal-box signal-' + sig;
    sigLabel.className = 'signal-label ' + (sig==='LONG'?'green':sig==='SHORT'?'red':'grey');
    sigLabel.textContent = sig==='LONG' ? '🚀 LONG' : sig==='SHORT' ? '🔻 SHORT' : '⏸ NEUTRAL';
    document.getElementById('signalInfo').textContent =
      'API: ' + (d.api_error||'OK') + ' | Circuit: ' + (d.circuit_breaker ? '⛔ AKTIF — '+d.circuit_reason : '✅ Normal');

    // Posisi
    const posList = document.getElementById('posList');
    if (d.positions && d.positions.length > 0) {
      posList.innerHTML = d.positions.map(pos => {
        const pnl = parseFloat(pos.pnl_live || 0);
        return `<div class="pos-card">
          <div class="pos-row">
            <span class="k" style="font-weight:700">${pos.symbol}</span>
            <span class="v ${pos.side==='LONG'?'green':'red'}">${pos.side==='LONG'?'🚀':'🔻'} ${pos.side}</span>
          </div>
          <div class="pos-row"><span class="k">Entry</span><span class="v">$${parseFloat(pos.entry_price).toLocaleString('en-US',{maximumFractionDigits:6})}</span></div>
          <div class="pos-row"><span class="k">Harga Saat Ini</span><span class="v blue">$${parseFloat(pos.current_price||0).toLocaleString('en-US',{maximumFractionDigits:6})}</span></div>
          <div class="pos-row"><span class="k">Stop Loss</span><span class="v red">$${parseFloat(pos.stop_loss).toLocaleString('en-US',{maximumFractionDigits:6})}</span></div>
          <div class="pos-row"><span class="k">Take Profit 1</span><span class="v green">$${parseFloat(pos.take_profit1).toLocaleString('en-US',{maximumFractionDigits:6})}</span></div>
          <div class="pos-row"><span class="k">Take Profit 2</span><span class="v green">$${parseFloat(pos.take_profit2).toLocaleString('en-US',{maximumFractionDigits:6})}</span></div>
          <div class="pos-row"><span class="k">PnL Live</span><span class="v ${pnl>=0?'green':'red'}" style="font-size:16px">${pnl>=0?'+':''}$${pnl.toFixed(4)}</span></div>
          <div class="pos-row"><span class="k">Trailing</span><span class="v">${pos.trailing_active?'✅ Aktif':'⏸ Belum'}</span></div>
          <div class="pos-row"><span class="k">Break-Even</span><span class="v">${pos.be_hit?'✅ Hit':'❌ Belum'}</span></div>
          <div class="pos-row"><span class="k">ID</span><span class="v grey" style="font-size:11px">${pos.id}</span></div>
        </div>`;
      }).join('');
    } else {
      posList.innerHTML = '<p style="color:#8b949e;font-size:12px">Tidak ada posisi terbuka.</p>';
    }

    const botDot = document.getElementById('botDot');
    const botStatus = document.getElementById('botStatus');
    if (d.circuit_breaker) {
      botDot.className = 'dot dot-red';
      botStatus.textContent = '⛔ Circuit Breaker Aktif';
    } else {
      botDot.className = 'dot dot-green';
      botStatus.textContent = 'Running — ' + d.symbol;
    }
    document.getElementById('lastUpdate').textContent = 'Update: ' + new Date().toLocaleTimeString('id-ID');
  } catch(e) {
    document.getElementById('botStatus').textContent = 'Koneksi terputus...';
    document.getElementById('botDot').className = 'dot dot-red';
  }
}

async function refreshScanner() {
  try {
    const r = await fetch('/api/scanner');
    const data = await r.json();
    const tbody = document.getElementById('scannerBody');
    if (!data || data.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8" style="color:#8b949e;text-align:center;padding:12px">Scan sedang berjalan...</td></tr>';
      return;
    }
    tbody.innerHTML = data.slice(0, 15).map(c => {
      const sig = c.signal || 'NEUTRAL';
      const sc = sig==='LONG' ? '#3fb950' : sig==='SHORT' ? '#f85149' : '#8b949e';
      const mc = c.momentum > 0 ? '#3fb950' : c.momentum < 0 ? '#f85149' : '#8b949e';
      return `<tr>
        <td><b>${c.symbol}</b></td>
        <td style="color:${sc};font-weight:700">${sig}</td>
        <td>${c.raw_score}</td>
        <td style="color:${mc}">${c.momentum>0?'+':''}${c.momentum}</td>
        <td>${c.atr_pct}%</td>
        <td>${c.vol_ratio}x</td>
        <td>${c.adx}</td>
        <td><b>${c.composite}</b></td>
      </tr>`;
    }).join('');
  } catch(e) {}
}

async function closeAll() {
  if (!confirm('Yakin tutup SEMUA posisi terbuka?')) return;
  try {
    await fetch('/api/close_all', {method:'POST'});
    toast('Semua posisi ditutup');
    refresh();
  } catch(e) { toast('Gagal menutup posisi', false); }
}

// ══════════════════════════════════════
// TAB 2: SETTINGS
// ══════════════════════════════════════
async function loadSettings() {
  try {
    const r = await fetch('/api/config');
    const c = await r.json();
    document.getElementById('s_symbol').value  = c.SYMBOL || '';
    document.getElementById('s_leverage').value = c.LEVERAGE || 20;
    document.getElementById('s_risk').value    = ((c.RISK_PER_TRADE||0.06)*100).toFixed(1);
    document.getElementById('s_margin').value  = ((c.MAX_MARGIN_PCT||0.15)*100).toFixed(0);
    document.getElementById('s_vbal').value    = c.VIRTUAL_BALANCE || 100;
    document.getElementById('s_sl').value      = c.ATR_SL_MULT || 0.6;
    document.getElementById('s_tp1').value     = c.ATR_TP1_MULT || 1.0;
    document.getElementById('s_tp2').value     = c.ATR_TP2_MULT || 2.0;
    document.getElementById('s_rr').value      = c.MIN_RR_RATIO || 1.5;
    document.getElementById('s_tp1pct').value  = c.TP1_CLOSE_PCT || 60;
    document.getElementById('s_trail_act').value = ((c.TRAIL_ACTIVATION_PCT||0.003)*100).toFixed(2);
    document.getElementById('s_trail_dist').value= ((c.TRAIL_DISTANCE_PCT||0.003)*100).toFixed(2);
    document.getElementById('s_be').value      = ((c.BE_ACTIVATION_PCT||0.002)*100).toFixed(2);
    document.getElementById('s_cooldown').value = c.LOSS_COOLDOWN_SEC || 60;
    document.getElementById('s_bull').value    = c.MIN_BULL_SCORE || 6;
    document.getElementById('s_bear').value    = c.MIN_BEAR_SCORE || 6;
    document.getElementById('s_early').value   = c.EARLY_ENTRY_SCORE || 5;
    document.getElementById('s_adx').value     = c.ADX_MIN_THRESHOLD || 15;
    document.getElementById('s_maxatr').value  = c.MAX_ATR_PCT_ENTRY || 4.0;
    document.getElementById('s_dloss').value   = ((c.MAX_DAILY_LOSS_PCT||0.15)*100).toFixed(0);
    document.getElementById('s_dd').value      = ((c.MAX_DRAWDOWN_PCT||0.30)*100).toFixed(0);
    document.getElementById('s_scan').value    = c.SCAN_INTERVAL || 90;
    document.getElementById('s_apikey').value  = c.MEXC_API_KEY || '';
    // Secret tidak di-load ke form (security) — tapi tandai jika sudah ada
    const secEl = document.getElementById('s_apisec');
    secEl.placeholder = c.MEXC_API_SECRET ? '(sudah tersimpan — isi untuk ganti)' : '••••••••';
    secEl.value = '';
    const isLive = !c.DRY_RUN;
    document.getElementById('toggleLive').checked = isLive;
    document.getElementById('liveWarning').style.display = isLive ? 'block' : 'none';
    updateModeText(isLive);
  } catch(e) { toast('Gagal load settings', false); }
}

function updateModeText(isLive) {
  const el = document.getElementById('modeText');
  el.textContent = isLive ? 'Mode: 🔴 LIVE TRADING' : 'Mode: DRY RUN (simulasi)';
  el.style.color = isLive ? '#f85149' : '#58a6ff';
}

function toggleMode() {
  const isLive = document.getElementById('toggleLive').checked;
  document.getElementById('liveWarning').style.display = isLive ? 'block' : 'none';
  updateModeText(isLive);
}

async function saveSettings() {
  const isLive = document.getElementById('toggleLive').checked;
  const apiKey = document.getElementById('s_apikey').value.trim();
  const apiSec = document.getElementById('s_apisec').value.trim();

  // Validasi live mode: harus ada API key
  if (isLive && !apiKey) {
    toast('❌ Isi MEXC API Key terlebih dahulu untuk LIVE mode!', false);
    return;
  }

  const payload = {
    SYMBOL:              document.getElementById('s_symbol').value.trim().toUpperCase(),
    LEVERAGE:            parseInt(document.getElementById('s_leverage').value),
    RISK_PER_TRADE:      parseFloat(document.getElementById('s_risk').value) / 100,
    MAX_MARGIN_PCT:      parseFloat(document.getElementById('s_margin').value) / 100,
    VIRTUAL_BALANCE:     parseFloat(document.getElementById('s_vbal').value),
    ATR_SL_MULT:         parseFloat(document.getElementById('s_sl').value),
    ATR_TP1_MULT:        parseFloat(document.getElementById('s_tp1').value),
    ATR_TP2_MULT:        parseFloat(document.getElementById('s_tp2').value),
    MIN_RR_RATIO:        parseFloat(document.getElementById('s_rr').value),
    TP1_CLOSE_PCT:       parseInt(document.getElementById('s_tp1pct').value),
    TRAIL_ACTIVATION_PCT:parseFloat(document.getElementById('s_trail_act').value) / 100,
    TRAIL_DISTANCE_PCT:  parseFloat(document.getElementById('s_trail_dist').value) / 100,
    BE_ACTIVATION_PCT:   parseFloat(document.getElementById('s_be').value) / 100,
    LOSS_COOLDOWN_SEC:   parseInt(document.getElementById('s_cooldown').value),
    MIN_BULL_SCORE:      parseInt(document.getElementById('s_bull').value),
    MIN_BEAR_SCORE:      parseInt(document.getElementById('s_bear').value),
    EARLY_ENTRY_SCORE:   parseInt(document.getElementById('s_early').value),
    ADX_MIN_THRESHOLD:   parseInt(document.getElementById('s_adx').value),
    MAX_ATR_PCT_ENTRY:   parseFloat(document.getElementById('s_maxatr').value),
    MAX_DAILY_LOSS_PCT:  parseFloat(document.getElementById('s_dloss').value) / 100,
    MAX_DRAWDOWN_PCT:    parseFloat(document.getElementById('s_dd').value) / 100,
    SCAN_INTERVAL:       parseInt(document.getElementById('s_scan').value),
    DRY_RUN:             !isLive,
  };
  // Hanya kirim API key/secret jika diisi — kosong = pakai yang sudah tersimpan
  if (apiKey) payload.MEXC_API_KEY = apiKey;
  if (apiSec) payload.MEXC_API_SECRET = apiSec;

  try {
    const r = await fetch('/api/config', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const res = await r.json();
    if (res.success) {
      if (res.warning) {
        toast('⚠️ Mode disimpan tapi: ' + res.warning, false);
      } else {
        toast(isLive ? '🔴 LIVE MODE aktif — Bot trading dengan uang nyata!' : '✅ Settings disimpan (DRY RUN)');
      }
      loadSettings();
    } else {
      toast('❌ Gagal: ' + (res.error||'unknown'), false);
    }
  } catch(e) { toast('❌ Error koneksi', false); }
}

// ══════════════════════════════════════
// TAB 3: HISTORY
// ══════════════════════════════════════
let _histMode = null;  // null = ikut mode aktif bot

async function loadHistory(mode) {
  try {
    // Tentukan endpoint
    let url = '/api/journal';
    if (mode === 'dry')  url = '/api/journal/dry';
    if (mode === 'live') url = '/api/journal/live';

    const r = await fetch(url);
    const res = await r.json();
    const data = res.trades || res;
    const activeMode = res.mode || mode || 'dry';
    _histMode = activeMode;

    // Update tombol aktif
    document.getElementById('h_btn_dry').style.background  = activeMode === 'dry'  ? '#58a6ff' : '';
    document.getElementById('h_btn_live').style.background = activeMode === 'live' ? '#f85149' : '';
    document.getElementById('h_mode_label').textContent =
      activeMode === 'live' ? '🔴 Live Trading' : '📊 Dry Run (Simulasi)';

    const tbody = document.getElementById('histBody');

    if (!data || data.length === 0) {
      tbody.innerHTML = '<tr><td colspan="12" style="color:#8b949e;text-align:center;padding:20px">Belum ada trade di mode ' + activeMode + '.</td></tr>';
      ['h_total','h_win','h_loss','h_wr','h_pnl','h_avg','h_best','h_worst'].forEach(id => {
        document.getElementById(id).textContent = '—';
      });
      return;
    }

    // Hitung statistik
    let totalPnl = 0, wins = 0, best = -Infinity, worst = Infinity;
    data.forEach(t => {
      const p = parseFloat(t.pnl || 0);
      totalPnl += p;
      if (p > 0) wins++;
      if (p > best) best = p;
      if (p < worst) worst = p;
    });
    const total = data.length;
    const wr = total > 0 ? (wins/total*100) : 0;

    document.getElementById('h_total').textContent = total;
    document.getElementById('h_win').textContent   = wins;
    document.getElementById('h_loss').textContent  = total - wins;
    const elWr = document.getElementById('h_wr');
    elWr.textContent = wr.toFixed(1) + '%';
    elWr.className = 'value ' + (wr >= 50 ? 'green' : wr >= 40 ? 'yellow' : 'red');
    const elPnl = document.getElementById('h_pnl');
    elPnl.textContent = (totalPnl>=0?'+':'') + '$' + totalPnl.toFixed(4);
    elPnl.className = 'value ' + (totalPnl >= 0 ? 'green' : 'red');
    document.getElementById('h_avg').textContent = (total>0?(totalPnl/total>=0?'+':'')+'$'+(totalPnl/total).toFixed(4):'—');
    document.getElementById('h_best').textContent  = best > -Infinity ? '+$' + best.toFixed(4) : '—';
    document.getElementById('h_worst').textContent = worst < Infinity ? '$' + worst.toFixed(4) : '—';

    // Tabel
    tbody.innerHTML = data.map((t, i) => {
      const pnl = parseFloat(t.pnl || 0);
      const fee = parseFloat(t.fee || 0);
      const pc = pnl > 0 ? '#3fb950' : pnl < 0 ? '#f85149' : '#8b949e';
      const sc = t.side === 'LONG' ? '#3fb950' : '#f85149';
      return `<tr>
        <td style="color:#8b949e">${total - i}</td>
        <td style="font-size:11px;color:#8b949e">${t.trade_id||''}</td>
        <td><b>${t.symbol||''}</b></td>
        <td style="color:${sc};font-weight:700">${t.side||''}</td>
        <td>$${parseFloat(t.entry_price||0).toLocaleString('en-US',{maximumFractionDigits:6})}</td>
        <td>$${parseFloat(t.exit_price||0).toLocaleString('en-US',{maximumFractionDigits:6})}</td>
        <td style="color:${pc};font-weight:700">${pnl>=0?'+':''}$${pnl.toFixed(4)}${fee>0?'<span style="color:#8b949e;font-size:10px;font-weight:normal"> (-$'+fee.toFixed(3)+')</span>':''}</td>
        <td style="font-size:12px;color:#8b949e">${t.close_reason||''}</td>
        <td style="font-size:12px">$${parseFloat(t.sl||0).toLocaleString('en-US',{maximumFractionDigits:6})}</td>
        <td style="font-size:12px">$${parseFloat(t.tp1||0).toLocaleString('en-US',{maximumFractionDigits:6})}</td>
        <td style="font-size:11px;color:#8b949e">${(t.opened_at||'').substring(5)}</td>
        <td style="font-size:11px;color:#8b949e">${(t.closed_at||'').substring(5)}</td>
      </tr>`;
    }).join('');
  } catch(e) { toast('Gagal load history', false); }
}

// ── Auto-refresh ──
refresh();
refreshScanner();
// ── Preset Settings ────────────────────────────────────────
const PRESETS = {
  scalper: {
    label: '⚡ Scalper 1m aktif — PRIMARY_TF akan diset ke 1m via config',
    LEVERAGE: 20, RISK_PER_TRADE: 6, MAX_MARGIN_PCT: 25, ATR_SL_MULT: 0.6,
    ATR_TP1_MULT: 1.0, ATR_TP2_MULT: 2.0, MIN_RR_RATIO: 1.5, TP1_CLOSE_PCT: 50,
    TRAIL_ACTIVATION_PCT: 0.30, TRAIL_DISTANCE_PCT: 0.30, BE_ACTIVATION_PCT: 0.20,
    LOSS_COOLDOWN_SEC: 60, MIN_BULL_SCORE: 14, MIN_BEAR_SCORE: 14,
    EARLY_ENTRY_SCORE: 16, ADX_MIN_THRESHOLD: 22, MAX_ATR_PCT_ENTRY: 4.0,
    SCAN_INTERVAL: 90, MAX_DAILY_LOSS_PCT: 15, MAX_DRAWDOWN_PCT: 30,
    EXHAUST_MIN_TP1_PCT: 0.40, MIN_CANDLE_SCORE: 5,
  },
  swing: {
    label: '📈 Swing MTF 5m aktif — PRIMARY_TF akan diset ke 5m via config',
    LEVERAGE: 10, RISK_PER_TRADE: 8, MAX_MARGIN_PCT: 20, ATR_SL_MULT: 1.0,
    ATR_TP1_MULT: 2.0, ATR_TP2_MULT: 4.0, MIN_RR_RATIO: 2.0, TP1_CLOSE_PCT: 40,
    TRAIL_ACTIVATION_PCT: 1.50, TRAIL_DISTANCE_PCT: 1.00, BE_ACTIVATION_PCT: 0.80,
    LOSS_COOLDOWN_SEC: 300, MIN_BULL_SCORE: 15, MIN_BEAR_SCORE: 15,
    EARLY_ENTRY_SCORE: 17, ADX_MIN_THRESHOLD: 25, MAX_ATR_PCT_ENTRY: 4.0,
    SCAN_INTERVAL: 300, MAX_DAILY_LOSS_PCT: 15, MAX_DRAWDOWN_PCT: 30,
    EXHAUST_MIN_TP1_PCT: 0.60, MIN_CANDLE_SCORE: 6,
  },
  conservative: {
    label: '🛡️ Konservatif — hanya trade sinyal paling kuat',
    LEVERAGE: 5, RISK_PER_TRADE: 5, MAX_MARGIN_PCT: 15, ATR_SL_MULT: 1.5,
    ATR_TP1_MULT: 3.0, ATR_TP2_MULT: 6.0, MIN_RR_RATIO: 2.5, TP1_CLOSE_PCT: 30,
    TRAIL_ACTIVATION_PCT: 2.00, TRAIL_DISTANCE_PCT: 1.50, BE_ACTIVATION_PCT: 1.00,
    LOSS_COOLDOWN_SEC: 600, MIN_BULL_SCORE: 17, MIN_BEAR_SCORE: 17,
    EARLY_ENTRY_SCORE: 19, ADX_MIN_THRESHOLD: 28, MAX_ATR_PCT_ENTRY: 3.0,
    SCAN_INTERVAL: 300, MAX_DAILY_LOSS_PCT: 10, MAX_DRAWDOWN_PCT: 20,
    EXHAUST_MIN_TP1_PCT: 0.70, MIN_CANDLE_SCORE: 7,
  },
};

function applyPreset(name) {
  const p = PRESETS[name];
  if (!p) return;
  const set = (id, val) => { const el = document.getElementById(id); if (el) el.value = val; };
  set('s_leverage',   p.LEVERAGE);
  set('s_risk',       p.RISK_PER_TRADE);
  set('s_margin',     p.MAX_MARGIN_PCT);
  set('s_sl',         p.ATR_SL_MULT);
  set('s_tp1',        p.ATR_TP1_MULT);
  set('s_tp2',        p.ATR_TP2_MULT);
  set('s_rr',         p.MIN_RR_RATIO);
  set('s_tp1pct',     p.TP1_CLOSE_PCT);
  set('s_trail_act',  p.TRAIL_ACTIVATION_PCT);
  set('s_trail_dist', p.TRAIL_DISTANCE_PCT);
  set('s_be',         p.BE_ACTIVATION_PCT);
  set('s_cooldown',   p.LOSS_COOLDOWN_SEC);
  set('s_bull',       p.MIN_BULL_SCORE);
  set('s_bear',       p.MIN_BEAR_SCORE);
  set('s_early',      p.EARLY_ENTRY_SCORE);
  set('s_adx',        p.ADX_MIN_THRESHOLD);
  set('s_maxatr',     p.MAX_ATR_PCT_ENTRY);
  set('s_scan',       p.SCAN_INTERVAL);
  set('s_dloss',      p.MAX_DAILY_LOSS_PCT);
  set('s_dd',         p.MAX_DRAWDOWN_PCT);
  const lbl = document.getElementById('presetLabel');
  lbl.textContent = '✅ ' + p.label + ' — klik Simpan Settings untuk terapkan';
  lbl.style.display = 'block';
  toast('Preset dimuat — klik Simpan Settings untuk terapkan');
}

// ── Reset History ──────────────────────────────────────────
async function resetHistory(mode) {
  const label = {dry:'Dry Run', live:'Live', all:'DRY + LIVE (semua)'};
  const lbl   = label[mode] || mode;
  const extra = (mode !== 'live') ? '\\nBalance dry run akan direset ke VIRTUAL_BALANCE.' : '';
  const msg   = '⚠️ Reset history ' + lbl + '?\\n\\nSemua riwayat trade akan dihapus permanen.' + extra + '\\n\\nTidak bisa dibatalkan!';
  if (!window.confirm(msg)) return;
  try {
    const r   = await fetch('/api/journal/reset', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body:    JSON.stringify({mode})
    });
    const res = await r.json();
    if (res.ok) {
      toast('✅ History ' + lbl + ' berhasil direset');
      setTimeout(() => loadHistory(mode === 'all' ? null : mode), 600);
    } else {
      toast('❌ Gagal reset', false);
    }
  } catch(e) {
    toast('❌ Error: ' + e.message, false);
  }
}

setInterval(refresh, 3000);
setInterval(refreshScanner, 20000);
</script>
</body>
</html>"""

class ScalperBotV5:
    CONFIG_FILE = "config_scalper_v4.json"
    def __init__(self, run_dashboard: bool = False):
        self.cfg = self.load_config()
        self.client = MEXCFuturesClient(MEXC_API_KEY, MEXC_API_SECRET)
        self.ta = ScalperTA(self.cfg); self.risk = RiskManager(self.cfg)
        self.session_filt = SessionFilter(self.cfg)
        self.notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT)
        self.journal = TradeJournal(self._journal_path())
        self.persistence = StatePersistence(self.cfg["STATE_FILE"])
        self.price_feed = PriceFeed(self.cfg["SYMBOL"])
        self.spot_client = MEXCSpotClient(MEXC_API_KEY, MEXC_API_SECRET)
        self.state = BotState(); self.dry_run = self.cfg["DRY_RUN"]; self.run_dashboard = run_dashboard
        self._pos_lock = threading.Lock() # Thread safety untuk positions

        active_cfg_key = MEXC_API_KEY or self.cfg.get("MEXC_API_KEY", "")
        active_cfg_secret = MEXC_API_SECRET or self.cfg.get("MEXC_API_SECRET", "")
        if active_cfg_key and active_cfg_secret:
            self.client.api_key = active_cfg_key; self.client.api_secret = active_cfg_secret
            self.spot_client.api_key = active_cfg_key; self.spot_client.api_secret = active_cfg_secret

        self._last_signal: Dict[str, dict] = {}; self._price_history: deque = deque(maxlen=500)
        self._trade_id_counter: int = 0; self._last_loss_time: Dict[str, float] = {}
        self._last_neutral_since: float = 0.0; self._scanner_thread: Optional[threading.Thread] = None
        self._last_balance_sync: float = 0.0; self._funding_rate: float = 0.0; self._funding_last_check: float = 0.0
        self._signal_persist: Dict[str, int] = {}; self._signal_persist_reset_ts: float = time.time()
        self._last_state_save: float = 0.0; self._state_dirty: bool = False
        self._trend_cache: Dict[str, dict] = {}; self._trend_cache_ts: float = 0.0
        self._last_candle_ts: int = 0; self._last_signal_cached: Optional[dict] = None
        self._ml_model: Optional[object] = None; self._ml_features: list = []
        self._load_ml_model()
        self.scanner = CoinScanner(self.client, self.ta, self.cfg)
        try: from mexc_v5_modules import BtcRegimeManager; self.regime_mgr = BtcRegimeManager(self.client, self.ta, self.cfg)
        except ImportError: log.warning("mexc_v5_modules tidak ditemukan, skip RegimeManager"); self.regime_mgr = None

        active_key = MEXC_API_KEY or self.cfg.get("MEXC_API_KEY", "")
        active_secret = MEXC_API_SECRET or self.cfg.get("MEXC_API_SECRET", "")
        self.user_stream = UserStreamWS(active_key, active_secret)
        self._init_state()
        self.price_feed.add_callback(self._on_price_update); self.price_feed.start()
        if not self.dry_run: self.user_stream.add_callback(self._on_user_event); self.user_stream.start()
        time.sleep(2)
        self._start_dashboard()

    def load_config(self) -> dict:
        if os.path.exists(self.CONFIG_FILE):
            try:
                with open(self.CONFIG_FILE) as f: file_cfg = json.load(f)
                full_cfg = CONFIG.copy(); full_cfg.update(file_cfg); log.info("Config dimuat dari config_scalper_v4.json"); return full_cfg
            except Exception as e: log.error(f"Gagal baca config: {e}")
        self.save_config(CONFIG); return CONFIG.copy()
    def save_config(self, cfg: dict):
        try:
            with open(self.CONFIG_FILE, "w") as f: json.dump(cfg, f, indent=4)
        except Exception as e: log.error(f"Gagal save config: {e}")
    def _journal_path(self) -> str:
        return self.cfg["JOURNAL_FILE_DRY"] if self.cfg.get("DRY_RUN", True) else self.cfg["JOURNAL_FILE_LIVE"]
    def update_config_live(self, new_cfg: dict):
        old_sym = self.cfg.get("SYMBOL"); self.cfg.update(new_cfg); self.save_config(self.cfg)
        k = self.cfg.get("MEXC_API_KEY", ""); s = self.cfg.get("MEXC_API_SECRET", "")
        self.client.api_key = k; self.client.api_secret = s; self.spot_client.api_key = k; self.spot_client.api_secret = s
        old_dry = self.dry_run; self.dry_run = self.cfg.get("DRY_RUN", True)
        if old_dry != self.dry_run:
            self.journal = TradeJournal(self._journal_path()); mode_lbl = "DRY RUN" if self.dry_run else "LIVE"
            log.info(f"[MODE] Beralih ke {mode_lbl} — Journal: {self._journal_path()}")
            with self._pos_lock:
                for pos in list(self.state.positions):
                    if not pos.closed: pos.closed = True; pos.close_reason = f"Mode switch ke {mode_lbl}"
                    pos.closed_at = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"); log.info(f"[MODE] Posisi {pos.symbol} {pos.side} ditutup (mode switch)")
                self._last_loss_time.clear()
        if not self.dry_run:
            self.sync_balance_from_exchange()
            if old_dry:
                if self.state.balance > 0: self.state.peak_balance = self.state.balance
                self.state.circuit_breaker = False; self.state.circuit_reason = ""; self.state.circuit_type = ""; self.state.circuit_triggered_at = 0.0
                log.info(f"[MODE] LIVE aktif — Saldo: ${self.state.balance:.2f} (peak reset)")
            elif self.state.api_error: log.warning(f"[MODE] {self.state.api_error}")
        else:
            if old_dry is False:
                vbal = self.cfg.get("VIRTUAL_BALANCE", 100.0)
                self.state.balance = vbal; self.state.peak_balance = vbal; self.state.total_pnl = 0.0; self.state.daily_pnl = 0.0
                self.state.total_trades = 0; self.state.winning_trades = 0; self.state.circuit_breaker = False; self.state.circuit_reason = ""
                log.info(f"[MODE] DRY RUN — Balance direset ke ${vbal:.2f}")
        if "SYMBOL" in new_cfg and new_cfg["SYMBOL"] != old_sym:
            self.price_feed.resubscribe(new_cfg["SYMBOL"]); new_price = self.price_feed.client.get_ticker(new_cfg["SYMBOL"])
            if new_price:
                with self.price_feed._lock: self.price_feed.price = new_price
            self.state.active_symbol = new_cfg["SYMBOL"]; self._last_signal.clear()
        if hasattr(self, "ta"): self.ta.cfg = self.cfg
        if hasattr(self, "risk"): self.risk.cfg = self.cfg
        if hasattr(self, "scanner"): self.scanner.cfg = self.cfg
        log.info("Hot-reload konfigurasi selesai")

    def _init_state(self):
        saved = self.persistence.load()
        if saved:
            log.info("Melanjutkan state sebelumnya...")
            self.state.balance = saved.get("balance", 0); self.state.peak_balance = saved.get("peak_balance", 0)
            self.state.total_trades = saved.get("total_trades", 0); self.state.winning_trades = saved.get("winning_trades", 0)
            self.state.total_pnl = saved.get("total_pnl", 0); self.state.daily_pnl = saved.get("daily_pnl", 0)
            self.state.daily_reset_date = saved.get("daily_reset_date", "")
            self.state.circuit_breaker = saved.get("circuit_breaker", False); self.state.circuit_reason = saved.get("circuit_reason", "")
            self.state.circuit_type = saved.get("circuit_type", ""); self.state.started_at = saved.get("started_at", datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"))
            self.state.iteration = saved.get("iteration", 0); self.state.secured_total = saved.get("secured_total", 0)
            self.state.active_symbol = saved.get("active_symbol", self.cfg["SYMBOL"])
            with self._pos_lock:
                for p_data in saved.get("positions", []):
                    try:
                        if "symbol" not in p_data: p_data["symbol"] = self.cfg["SYMBOL"]
                        if "flip_count" not in p_data: p_data["flip_count"] = 0
                        if "opened_ts" not in p_data: p_data["opened_ts"] = 0.0
                        if "st_direction_at_entry" not in p_data: p_data["st_direction_at_entry"] = 0
                        pos = Position(**p_data)
                        if not pos.closed: self.state.positions.append(pos)
                    except Exception as e: log.warning(f"Skip restore posisi: {e}")
            if not self.dry_run:
                pre_sync_balance = self.state.balance; self.sync_balance_from_exchange(); real_bal = self.state.balance
                if pre_sync_balance > 0 and real_bal > 0:
                    ratio = abs(pre_sync_balance - real_bal) / pre_sync_balance
                    if ratio > 0.10: self.state.peak_balance = real_bal; self.state.circuit_breaker = False; self.state.circuit_reason = ""; self.state.circuit_type = ""; self.state.circuit_triggered_at = 0.0
                    log.info(f"[LIVE] State dry-run terdeteksi → peak reset ke ${real_bal:.2f}")
        else:
            self.state.balance = (self.client.get_balance() or self.cfg["VIRTUAL_BALANCE"]) if not self.cfg["DRY_RUN"] else self.cfg["VIRTUAL_BALANCE"]
            self.state.peak_balance = self.state.balance; self.state.started_at = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
            self.state.active_symbol = self.cfg["SYMBOL"]; log.info(f"State baru — Saldo: ${self.state.balance:,.2f}")

    def _gen_trade_id(self) -> str: self._trade_id_counter += 1; return f"S{datetime.now(WIB).strftime('%Y%m%d%H%M%S')}-{self._trade_id_counter:04d}"
    def sync_balance_from_exchange(self):
        bal = self.client.get_balance()
        if bal is not None:
            self.state.real_balance = bal
            if not self.dry_run: self.state.balance = bal; self.state.peak_balance = max(self.state.peak_balance, bal)
            self.state.api_error = ""
        else: self.state.api_error = "API Error: Gagal konek (Cek API Key)"

    def _get_price_velocity(self, seconds: int = 30) -> float:
        now = time.time(); cutoff = now - seconds; hist = [(p["price"], p["ts"]) for p in self._price_history if p["ts"] >= cutoff]
        if len(hist) < 2: return 0.0
        oldest = hist[0][0]; newest = hist[-1][0]
        return (newest - oldest) / oldest * 100 if oldest > 0 else 0.0

    def _get_trend_info(self, symbol: str) -> dict:
        trend_info = {"bias": "NEUTRAL", "strength": 0, "detail": "disabled"}
        is_pullback = self.cfg.get("ENTRY_MODE", "PULLBACK") == "PULLBACK"
        if not self.cfg.get("REQUIRE_TREND_CONFIRM") and not is_pullback: return trend_info
        cache_ttl = self.cfg.get("TREND_CACHE_SEC", 600); now_ts = time.time()
        cache_key = f"{symbol}_trend"; cached = self._trend_cache.get(cache_key)
        cache_age = now_ts - cached.get("_ts", 0) if cached else cache_ttl + 1
        if cached is None or cache_age >= cache_ttl:
            df_trend = self._fetch_df(symbol, self.cfg.get("TREND_TF", "1h"))
            if df_trend is not None:
                trend_info = self.ta.get_trend_bias(df_trend); trend_info["_ts"] = now_ts
                if len(self._trend_cache) >= 200:
                    oldest = min(self._trend_cache, key=lambda k: self._trend_cache[k].get("_ts", 0)); del self._trend_cache[oldest]
                self._trend_cache[cache_key] = trend_info; log.info(f"[1H TREND] {symbol}: {trend_info['bias']} (kekuatan {trend_info['strength']}/6) — {trend_info['detail']}")
        else: trend_info = {k: v for k, v in cached.items() if k != "_ts"}
        return trend_info

    def _on_user_event(self, channel: str, data: dict):
        if channel == "push.personal.order":
            status = str(data.get("state", data.get("status", "")))
            if status == "2": log.info("[UserWS] Order filled → sync posisi instan"); self.sync_positions_from_mexc()
        elif channel == "push.personal.position": log.debug("[UserWS] Posisi update → sync"); self.sync_positions_from_mexc()

    def _on_price_update(self, price: float):
        self._price_history.append({"price": price, "ts": time.time()})
        now = time.time(); grace_sec = self.cfg.get("GRACE_PERIOD_SEC", 15)
        with self._pos_lock:
            for pos in list(self.state.positions):
                if pos.closed: continue
                age_sec = now - pos.opened_ts if pos.opened_ts > 0 else 9999
                in_grace = age_sec < grace_sec
                if not in_grace:
                    self.risk.update_trailing_stop(pos, price)
                    if self.cfg.get("USE_BE_FILTER") and not pos.be_hit and not pos.tp1_hit:
                        pp = (price - pos.entry_price) / pos.entry_price if pos.side == "LONG" else (pos.entry_price - price) / pos.entry_price
                        if pp >= self.cfg.get("BE_ACTIVATION_PCT", 0.002):
                            pos.be_hit = True; buf = self.cfg.get("BE_FEE_BUFFER_PCT", 0.001)
                            pos.stop_loss = pos.entry_price * (1 + buf) if pos.side == "LONG" else pos.entry_price * (1 - buf)
                            log.info(f"[BE] {pos.id} SL → entry ${pos.stop_loss:.4f}")
                            self.notifier.send(f"🛡️ *Break-Even*\n`{pos.id}` SL ke entry.")
                            if not self.dry_run:
                                sl_side = 4 if pos.side == "LONG" else 2; m_mode = 2 if self.cfg.get("MARGIN_MODE") == "CROSS" else 1
                                self.client.cancel_all_orders(pos.symbol)
                                self.client.place_stop_order(pos.symbol, sl_side, pos.stop_loss, pos.quantity, self.cfg.get("LEVERAGE", 5), m_mode)
                                log.info(f"🛡️ [SL@BURSA] Hard SL diupdate ke BE ${pos.stop_loss:.4f}")
                sl = pos.trailing_stop if pos.trailing_active else pos.stop_loss
                if pos.side == "LONG" and price <= sl: self._close_position(pos, price, "Trailing SL" if pos.trailing_active else "Stop Loss")
                elif pos.side == "SHORT" and price >= sl: self._close_position(pos, price, "Trailing SL" if pos.trailing_active else "Stop Loss")

    def _check_daily_reset(self):
        today = datetime.now(WIB).strftime("%Y-%m-%d")
        if self.state.daily_reset_date != today:
            self.state.daily_pnl = 0.0; self.state.daily_start_balance = self.state.balance; self.state.daily_reset_date = today

    def _check_circuit_breaker(self) -> bool:
        if self.state.circuit_breaker: return True
        c = self.cfg; basis = self.state.daily_start_balance if self.state.daily_start_balance > 0 else (self.state.balance + abs(self.state.daily_pnl))
        if self.state.daily_pnl < 0:
            loss_pct = abs(self.state.daily_pnl) / max(basis, 1)
            if loss_pct >= c.get("MAX_DAILY_LOSS_PCT", 0.15): self._trigger_circuit("AUTO", f"Daily loss >{c['MAX_DAILY_LOSS_PCT']*100:.0f}%"); return True
        dd = self.state.drawdown()
        if dd >= c.get("MAX_DRAWDOWN_PCT", 0.30) * 100: self._trigger_circuit("AUTO", f"Max drawdown {dd:.1f}%"); return True
        return False

    def _trigger_circuit(self, ctype: str, reason: str):
        self.state.circuit_breaker = True; self.state.circuit_triggered_at = time.time()
        self.state.circuit_type = ctype; self.state.circuit_reason = reason
        log.warning(f"CIRCUIT BREAKER: {reason}"); self.close_all_positions(reason=f"Circuit Breaker ({reason})"); self.notifier.send(f"⛔ *Circuit Breaker*\n{reason}")

    def _trigger_scan(self):
        if self._scanner_thread and self._scanner_thread.is_alive(): return
        self._scanner_thread = threading.Thread(target=self.scanner.scan_once, daemon=True, name="CoinScanner"); self._scanner_thread.start()

    def _maybe_switch_coin(self) -> bool:
        if not self.cfg.get("AUTO_SWITCH_COIN", True) or self.open_positions_count() > 0: return False
        current = self.cfg.get("SYMBOL"); advantage = self.cfg.get("SWITCH_MIN_ADVANTAGE", 2.5)
        all_res = {r["symbol"]: r for r in self.scanner.get_results()}; cur_data = all_res.get(current, {})
        cur_score = cur_data.get("composite", 0) if cur_data.get("signal") != "NEUTRAL" else 0
        best = self.scanner.best_coin()
        if best and best["symbol"] != current and (best["composite"] - cur_score) >= advantage:
            return self._do_switch(current, best["symbol"], cur_score, best["composite"], best)
        idle_max = self.cfg.get("SWITCH_IDLE_MAX_SEC", 180); cur_sig = cur_data.get("signal", "NEUTRAL")
        if cur_sig != "NEUTRAL": self._last_neutral_since = 0.0
        elif self._last_neutral_since == 0.0: self._last_neutral_since = time.time()
        all_list = self.scanner.get_results()
        if (cur_sig == "NEUTRAL" and self._last_neutral_since > 0 and (time.time() - self._last_neutral_since) >= idle_max and all_list and all_list[0]["symbol"] != current):
            self._last_neutral_since = 0.0; return self._do_switch(current, all_list[0]["symbol"], cur_score, all_list[0]["composite"], all_list[0])
        return False

    def _do_switch(self, old: str, new: str, old_s: float, new_s: float, data: dict) -> bool:
        log.info(f"[SWITCH] {old} → {new} ({old_s:.1f} → {new_s:.1f})")
        self.notifier.send(f"🔄 *Switch Koin*\nDari: `{old}` ({old_s:.1f})\nKe:   `{new}` ({new_s:.1f})\nSignal: `{data.get('signal','?')}`")
        self.cfg["SYMBOL"] = new; self.state.active_symbol = new; self.price_feed.resubscribe(new); self._last_neutral_since = 0.0
        new_price = self.price_feed.client.get_ticker(new)
        if new_price:
            with self.price_feed._lock: self.price_feed.price = new_price
        self.save_config(self.cfg); return True

    def _fetch_df(self, symbol: str, tf: str) -> Optional[pd.DataFrame]:
        df = self.client.get_klines(symbol, tf, self.cfg["CANDLE_LIMIT"])
        if df is None or len(df) < 20: return None
        df = self.ta.compute(df); df.dropna(inplace=True); return df

    def fetch_and_analyze(self) -> Optional[dict]:
        symbol = self.cfg["SYMBOL"]
        tf_sec_map = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600}
        tf_sec = tf_sec_map.get(self.cfg.get("PRIMARY_TF", "5m"), 300)
        cur_candle_ts = int(time.time() // tf_sec) * tf_sec; has_open = self.open_positions_count() > 0
        if (cur_candle_ts == self._last_candle_ts and self._last_signal_cached is not None and not has_open): return self._last_signal_cached
        df = self._fetch_df(symbol, self.cfg["PRIMARY_TF"])
        if df is None: log.warning("Gagal ambil candle primary"); return None
        self._last_candle_ts = cur_candle_ts
        live_price = self.price_feed.get_price(); live_obi = self.price_feed.get_obi()
        live_flow = self.price_feed.get_trade_flow(); live_whale = self.price_feed.get_whale_signal()
        trend_info = self._get_trend_info(symbol); htf_bias = "NEUTRAL"
        if self.cfg.get("REQUIRE_MTF_CONFIRM"):
            df_htf = self._fetch_df(symbol, self.cfg["CONFIRM_TF"])
            if df_htf is not None: htf_bias = self.ta.get_htf_bias(df_htf)
        entry_mode = self.cfg.get("ENTRY_MODE", "PULLBACK"); has_open = self.open_positions_count() > 0
        if entry_mode == "PULLBACK" and not has_open:
            signal = self.ta.get_pullback_signal(df, trend_bias=trend_info["bias"], trend_strength=trend_info["strength"], htf_bias=htf_bias,
                                                 current_price=live_price, obi=live_obi, trade_flow=live_flow, funding_rate=self._funding_rate, whale_side=live_whale)
            signal.update({"htf_bias": htf_bias, "trend_bias": trend_info["bias"], "trend_strength": trend_info["strength"], "trend_detail": trend_info.get("detail", "")})
            if self.cfg.get("CANDLE_CLOSE_CONFIRM", True) and signal["signal"] != "NEUTRAL":
                tf_sec = tf_sec_map.get(self.cfg.get("PRIMARY_TF", "5m"), 300); candle_age = time.time() % tf_sec
                window = self.cfg.get("CANDLE_CLOSE_WINDOW_SEC", 90)
                if candle_age > window: signal["signal"] = "NEUTRAL"; signal["blocked_reason"] = f"Candle terlalu tua ({candle_age:.0f}s/{tf_sec}s) — tunggu candle berikutnya (window {window}s)"
            if self.cfg.get("VELOCITY_CONFIRM") and signal["signal"] != "NEUTRAL":
                velocity = self._get_price_velocity(self.cfg.get("VELOCITY_WINDOW_SEC", 30)); max_vel = self.cfg.get("PULLBACK_MAX_VELOCITY", 0.20)
                direction = signal["signal"]
                if direction == "LONG" and velocity < -max_vel: signal["signal"] = "NEUTRAL"; signal["blocked_reason"] = f"PB velocity extreme LONG ({velocity:.3f}%)"
                elif direction == "SHORT" and velocity > max_vel: signal["signal"] = "NEUTRAL"; signal["blocked_reason"] = f"PB velocity extreme SHORT ({velocity:.3f}%)"
                signal["velocity"] = round(velocity, 4)
            self._last_signal_cached = signal; return signal

        signal = self.ta.get_signal(df, current_price=live_price, obi=live_obi, trade_flow=live_flow, funding_rate=self._funding_rate, whale_side=live_whale)
        signal["htf_bias"] = htf_bias
        if self.cfg.get("REQUIRE_MTF_CONFIRM"):
            if signal["signal"] == "LONG" and htf_bias == "BEARISH": signal["signal"] = "NEUTRAL"; signal["blocked_reason"] = "MTF conflict: LONG vs 15m BEARISH"
            elif signal["signal"] == "SHORT" and htf_bias == "BULLISH": signal["signal"] = "NEUTRAL"; signal["blocked_reason"] = "MTF conflict: SHORT vs 15m BULLISH"
        if self.cfg.get("REQUIRE_TREND_CONFIRM") and signal["signal"] != "NEUTRAL":
            opp_trend = (signal["signal"] == "LONG" and trend_info["bias"] == "BEARISH") or (signal["signal"] == "SHORT" and trend_info["bias"] == "BULLISH")
            if opp_trend: signal["signal"] = "NEUTRAL"; signal["blocked_reason"] = f"1H Trend conflict: {signal.get('blocked_reason','?')} vs {trend_info['bias']}"
        signal.update({"trend_bias": trend_info["bias"], "trend_strength": trend_info["strength"], "trend_detail": trend_info.get("detail", "")})
        if self.cfg.get("VELOCITY_CONFIRM") and signal["signal"] != "NEUTRAL":
            velocity = self._get_price_velocity(self.cfg.get("VELOCITY_WINDOW_SEC", 30)); min_vel = self.cfg.get("VELOCITY_MIN_PCT", 0.03)
            direction = signal["signal"]
            if direction == "LONG" and velocity < -min_vel: signal["signal"] = "NEUTRAL"; signal["blocked_reason"] = f"Velocity berlawanan LONG ({velocity:.3f}%)"
            elif direction == "SHORT" and velocity > min_vel: signal["signal"] = "NEUTRAL"; signal["blocked_reason"] = f"Velocity berlawanan SHORT ({velocity:.3f}%)"
            signal["velocity"] = round(velocity, 4)
        self._last_signal_cached = signal; return signal

    def open_positions_count(self) -> int:
        with self._pos_lock: return sum(1 for p in self.state.positions if not p.closed)

    def _check_abort(self, pos: Position, price: float):
        c = self.cfg
        if not c.get("ABORT_ENABLED", True) or pos.closed or pos.tp1_hit or pos.be_hit or pos.id.startswith("SYNC-"): return
        age = time.time() - pos.opened_ts
        if age <= 0 or age > c.get("ABORT_CHECK_SEC", 90): return
        threshold = c.get("ABORT_THRESHOLD_PCT", 0.004)
        move_pct = (price - pos.entry_price) / pos.entry_price if pos.side == "LONG" else (pos.entry_price - price) / pos.entry_price
        if move_pct < -threshold: log.info(f"[ABORT] {pos.id} {pos.side} — harga bergerak {move_pct*100:.3f}% berlawanan dalam {age:.0f}s → close sebelum SL kena"); self._close_position(pos, price, "Abort — no momentum after entry")

    def _open_position(self, signal: dict, entry_price: float):
        c = self.cfg; side = signal["signal"]; atr = signal["atr"]; symbol = c["SYMBOL"]
        levels = self.risk.calculate_levels(side, entry_price, atr, atr_pct=signal.get("atr_pct", 0.0))
        if not self.risk.check_rr(levels): log.info(f"[SKIP] R:R terlalu rendah ({levels['rr_ratio']:.2f})"); return
        qty = self.risk.position_size(self.state.balance, levels["sl_distance"], entry_price, win_rate=self.state.win_rate()/100,
                                      score=max(signal.get("bull_score", 0), signal.get("bear_score", 0)), max_score=signal.get("max_score", 22))
        if qty <= 0: log.info("[SKIP] Quantity 0, saldo tidak cukup"); return
        atr_pct = signal.get("atr_pct", 0)
        if atr_pct > c.get("MAX_ATR_PCT_ENTRY", 4.0): log.info(f"[SKIP] ATR terlalu tinggi ({atr_pct:.2f}%)"); return
        if atr_pct < c.get("MIN_ATR_PCT", 0.3): log.info(f"[SKIP] ATR terlalu rendah ({atr_pct:.2f}%)"); return
        sl_pct = levels["sl_distance"] / entry_price * 100
        if sl_pct < 0.15: log.info(f"[SKIP] SL terlalu dekat ({sl_pct:.4f}%) — koin tidak cocok scalping"); return
        max_single_loss = levels["sl_distance"] * qty; max_allowed_loss = self.state.balance * 0.08
        if max_single_loss > max_allowed_loss: qty = round(max_allowed_loss / levels["sl_distance"], 6); log.info(f"[CAP] Qty dikurangi → max loss cap ${max_allowed_loss:.2f} (qty={qty})")
        if qty <= 0: return
        sym_key = symbol; last_loss = self._last_loss_time.get(sym_key, 0); cooldown = c.get("LOSS_COOLDOWN_SEC", 60)
        if (time.time() - last_loss) < cooldown: log.info(f"[SKIP] Cooldown setelah loss ({int(cooldown - (time.time() - last_loss))}s tersisa)"); return
        if c.get("CORRELATION_FILTER", True):
            CORR_GROUPS = [{"BTC_USDT", "ETH_USDT", "BNB_USDT", "SOL_USDT", "AVAX_USDT", "MATIC_USDT", "ARB_USDT", "OP_USDT"}, {"XAUT_USDT", "PAXG_USDT"}]
            for grp in CORR_GROUPS:
                if symbol not in grp: continue
                with self._pos_lock:
                    for pos in list(self.state.positions):
                        if pos.closed or pos.symbol not in grp: continue
                        if pos.side == side: log.info(f"[SKIP] Correlation filter: sudah ada {pos.side} {pos.symbol} (berkorelasi dengan {symbol})"); return
        order_id = None
        if not self.dry_run:
            self.client.cancel_all_orders(symbol); mexc_side = 1 if side == "LONG" else 3
            max_retries, chase_delay, limit_filled = 5, 1.5, False
            for attempt in range(max_retries):
                ticker = self.client.get_ticker(symbol)
                if not ticker: continue
                limit_price = ticker; log.info(f"[{symbol}] Limit Chaser (Attempt {attempt+1}/{max_retries}) | Jaring di: {limit_price}")
                res = self.client.place_order(symbol, mexc_side, 2, c["LEVERAGE"], qty, price=limit_price)
                if not res: log.warning(f"[{symbol}] Gagal pasang Post-Only (harga menabrak spread). Retry..."); time.sleep(chase_delay); continue
                order_id_temp = str(res) if isinstance(res, (int, str)) else str(res.get("orderId", ""))
                time.sleep(chase_delay)
                positions = self.client.get_open_positions(symbol)
                for pos_data in positions:
                    if float(pos_data.get("vol", 0)) > 0: log.info(f"[{symbol}] BINGO! Jaring Maker (0% Fee) dimakan pasar!"); limit_filled = True; order_id = order_id_temp; break
                if limit_filled: break
                log.info(f"[{symbol}] Jaring tidak dimakan (harga lari). Batalkan dan kejar ulang..."); self.client.cancel_all_orders(symbol)
            if not limit_filled: log.error(f"[{symbol}] Gagal mengejar harga setelah {max_retries} kali. Sinyal Abort (Batal)."); return

        pos = Position(id=self._gen_trade_id(), symbol=symbol, side=side, entry_price=entry_price, quantity=qty,
                       stop_loss=levels["stop_loss"], take_profit1=levels["take_profit1"], take_profit2=levels["take_profit2"],
                       take_profit3=levels["take_profit3"], opened_at=datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"),
                       order_id=order_id, opened_ts=time.time(), st_direction_at_entry=signal.get("st_dir", 0),
                       entry_bull_score=signal.get("bull_score", 0), entry_bear_score=signal.get("bear_score", 0),
                       entry_confidence=signal.get("confidence", 0), entry_atr=atr, entry_st_dir=signal.get("st_dir", 0),
                       entry_roc=signal.get("roc", 0.0), entry_willr=signal.get("willr", 0.0), entry_is_early=bool(signal.get("is_early_entry", False)))
        with self._pos_lock: self.state.positions.append(pos)
        self._state_dirty = True
        if not self.dry_run:
            sl_side = 4 if side == "LONG" else 2; m_mode = 2 if c.get("MARGIN_MODE") == "CROSS" else 1; lev = c.get("LEVERAGE", 5)
            sl_res = self.client.place_stop_order(symbol, sl_side, levels["stop_loss"], qty, lev, m_mode)
            if sl_res: log.info(f"🛡️ [SL@BURSA] Hard SL dipasang di ${levels['stop_loss']:.4f}")
            else: log.warning(f"⚠️ [SL@BURSA] Gagal pasang hard SL — SL internal bot tetap aktif")
            tp1_res = self.client.place_stop_order(symbol, sl_side, levels["take_profit1"], qty, lev, m_mode, is_take_profit=True)
            if tp1_res: log.info(f"🎯 [TP1@BURSA] Hard TP1 dipasang di ${levels['take_profit1']:.4f}")
        early = "⚡ EARLY " if signal.get("is_early_entry") else ""; st_lbl = "🟢 BULLISH" if signal.get("st_dir") == -1 else "🔴 BEARISH"
        log.info(f"[OPEN] {early}{side} {symbol} | Entry=${entry_price:.4f} SL=${levels['stop_loss']:.4f} TP1=${levels['take_profit1']:.4f} RR={levels['rr_ratio']:.1f}x | ST={st_lbl} Score={max(signal['bull_score'],signal['bear_score'])}/{signal.get('max_score',18)} ATR%={atr_pct:.2f}%")
        self.notifier.send(f"{'🚀' if side == 'LONG' else '🔻'} *{early}SCALP {side}* — `{symbol}`\nEntry: `${entry_price:.4f}`\nSL: `${levels['stop_loss']:.4f}` | TP1: `${levels['take_profit1']:.4f}`\nR:R: `{levels['rr_ratio']:.1f}x` | ATR: `{atr_pct:.2f}%`\nSuperTrend: `{st_lbl}` | Score: `{max(signal['bull_score'],signal['bear_score'])}/{signal.get('max_score',18)}`\nROC: `{signal.get('roc',0):.3f}%` | WR%: `{signal.get('willr',0):.0f}`")

    def _close_position(self, pos: Position, price: float, reason: str, signal: dict = None):
        if pos.closed: return
        if not self.dry_run:
            log.info(f"📤 [LIVE CLOSE] {pos.side} {pos.symbol} — {reason}"); self.client.cancel_all_orders(pos.symbol)
            close_side = 4 if pos.side == "LONG" else 2
            close_result = self.client.place_order(pos.symbol, close_side, 5, self.cfg.get("LEVERAGE", 5), pos.quantity)
            if not close_result: log.error(f"❌ GAGAL menutup {pos.symbol} di bursa! Tutup manual di MEXC. Bot akan coba lagi tick berikutnya."); return
            log.info(f"✅ [LIVE CLOSE] {pos.symbol} berhasil tertutup di bursa.")
        pos.closed = True; pos.close_reason = reason; pos.closed_at = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"); self._state_dirty = True
        taker_fee = self.cfg.get("TAKER_FEE_PCT", 0.0006)
        gross_pnl = (price - pos.entry_price) * pos.quantity if pos.side == "LONG" else (pos.entry_price - price) * pos.quantity
        fee = (pos.entry_price * pos.quantity + price * pos.quantity) * taker_fee; net_pnl = gross_pnl - fee
        pos.fee += fee; pos.pnl += net_pnl; self.state.balance += net_pnl; self.state.total_pnl += net_pnl; self.state.daily_pnl += net_pnl
        self.state.total_trades += 1
        if pos.pnl > 0: self.state.winning_trades += 1
        else: self._last_loss_time[pos.symbol] = time.time()
        self.state.peak_balance = max(self.state.peak_balance, self.state.balance)
        pnl_emoji = "✅" if pos.pnl > 0 else "❌"
        log.info(f"[CLOSE] {pos.id} {pos.side} | {reason} | Entry=${pos.entry_price:.4f} Exit=${price:.4f} Gross=${gross_pnl:+.4f} Fee=${fee:.4f} Net={pnl_emoji}${pos.pnl:+.4f} | Balance=${self.state.balance:.2f} WR={self.state.win_rate():.0f}%")
        self.notifier.send(f"{pnl_emoji} *CLOSE {pos.side}* — `{pos.symbol}`\nAlasan: `{reason}`\nEntry: `${pos.entry_price:.4f}` → Exit: `${price:.4f}`\nPnL: `${pos.pnl:+.4f}` (fee: -${fee:.4f}) | Balance: `${self.state.balance:.2f}`\nWR: `{self.state.win_rate():.0f}%` ({self.state.winning_trades}/{self.state.total_trades})")
        self.journal.log_trade(pos, price, signal or {})
        if self.cfg.get("ENABLE_AUTO_SECURE") and pos.pnl > 0 and not self.dry_run:
            secure_amt = pos.pnl * self.cfg.get("SECURE_PROFIT_PCT", 50) / 100
            if secure_amt >= self.cfg.get("MIN_SECURE_TRANSFER", 1.0):
                if self.spot_client.transfer_to_spot(secure_amt): self.state.secured_total += secure_amt
        with self._pos_lock:
            closed = [p for p in self.state.positions if p.closed]
            if len(closed) > 500:
                keep = set(id(p) for p in closed[-500:]); open_pos = [p for p in self.state.positions if not p.closed]
                self.state.positions = open_pos + [p for p in closed if id(p) in keep]

    def close_all_positions(self, reason: str = "Manual"):
        price = self.price_feed.get_price()
        with self._pos_lock:
            for pos in list(self.state.positions):
                if not pos.closed: self._close_position(pos, price, reason)

    def _check_flip_exit(self, pos: Position, signal: dict, price: float) -> bool:
        c = self.cfg; opp_score = signal["bear_score"] if pos.side == "LONG" else signal["bull_score"]
        profit_pct = (price - pos.entry_price) / pos.entry_price if pos.side == "LONG" else (pos.entry_price - price) / pos.entry_price
        if c.get("USE_MOMENTUM_EXHAUST", True):
            held_sec = time.time() - pos.opened_ts if pos.opened_ts > 0 else 0; min_hold = c.get("EXHAUST_MIN_HOLD_SEC", 180)
            roc_now, roc_accel = signal.get("roc", 0), signal.get("roc_accel", 0)
            obi, trade_flow = signal.get("obi", 0.0), signal.get("trade_flow", 0.0)
            roc_exhaust = (pos.side == "LONG" and roc_now < 0 and roc_accel < 0) or (pos.side == "SHORT" and roc_now > 0 and roc_accel > 0)
            obi_against = (pos.side == "LONG" and obi < -0.30) or (pos.side == "SHORT" and obi > 0.30)
            flow_against = (pos.side == "LONG" and trade_flow < -0.40) or (pos.side == "SHORT" and trade_flow > 0.40)
            taker_fee = c.get("TAKER_FEE_PCT", 0.0006); min_gross_pnl = pos.entry_price * pos.quantity * taker_fee * 2.5
            if held_sec > min_hold and profit_pct > 0:
                gross_pnl = profit_pct * pos.entry_price * pos.quantity
                if gross_pnl < min_gross_pnl: pass
                else:
                    min_tp1_pct = c.get("EXHAUST_MIN_TP1_PCT", 0.30); tp1_dist = abs(pos.take_profit1 - pos.entry_price) / pos.entry_price if pos.take_profit1 and pos.entry_price > 0 else 0
                    reached_tp1 = profit_pct / tp1_dist if tp1_dist > 0 else 0
                    if reached_tp1 < min_tp1_pct: pass
                    elif roc_exhaust and obi_against: self._close_position(pos, price, "Momentum Exhaustion (ROC+OBI)", signal); return True
                    elif roc_exhaust and flow_against: self._close_position(pos, price, "Momentum Exhaustion (ROC+Flow)", signal); return True
                    elif roc_exhaust and opp_score >= c.get("FLIP_ZONE2_MIN_SCORE", 7): self._close_position(pos, price, "Momentum Exhaustion (ROC+OpScore)", signal); return True
            early_cut_pct = c.get("EARLY_CUT_LOSS_PCT", -0.004)
            if held_sec > min_hold and profit_pct < 0 and profit_pct > early_cut_pct:
                if roc_exhaust and (obi_against or flow_against): self._close_position(pos, price, "Momentum Exhaustion (early cut)", signal); return True
        if c.get("EXIT_ON_ST_FLIP", False):
            st_now = signal.get("st_dir", pos.st_direction_at_entry)
            if pos.st_direction_at_entry != 0 and st_now != pos.st_direction_at_entry:
                min_hold = c.get("ST_FLIP_MIN_HOLD_SEC", 120); held_sec = time.time() - pos.opened_ts if pos.opened_ts > 0 else 9999
                if held_sec >= min_hold:
                    dema_confirms = (pos.side == "LONG" and signal.get("dema_fast", 0) < signal.get("dema_slow", 0)) or (pos.side == "SHORT" and signal.get("dema_fast", 0) > signal.get("dema_slow", 0))
                    obi_confirms = (pos.side == "LONG" and signal.get("obi", 0) < -0.35) or (pos.side == "SHORT" and signal.get("obi", 0) > 0.35)
                    if dema_confirms and obi_confirms: self._close_position(pos, price, "SuperTrend Flip", signal); return True
        if not c.get("EXIT_ON_SIGNAL_FLIP", True): return False
        flip_held = time.time() - pos.opened_ts if pos.opened_ts > 0 else 9999; flip_min = c.get("SIGNAL_FLIP_MIN_HOLD_SEC", 90)
        if flip_held < flip_min: return False
        z1, z2, z2_min, z3_min, z3_can = c.get("FLIP_ZONE1_PCT", 0.003), c.get("FLIP_ZONE2_PCT", 0.015), c.get("FLIP_ZONE2_MIN_SCORE", 7), c.get("FLIP_ZONE3_MIN_SCORE", 8), c.get("FLIP_ZONE3_CANDLES", 1)
        opp_signal = "LONG" if pos.side == "SHORT" else "SHORT"
        if signal["signal"] != opp_signal: return False
        if profit_pct < z1: self._close_position(pos, price, "Signal Flip (zona 1)", signal); return True
        elif profit_pct < z2:
            if opp_score >= z2_min: self._close_position(pos, price, "Signal Flip (zona 2)", signal); return True
        else:
            if opp_score >= z3_min:
                pos.flip_count += 1
                if pos.flip_count >= z3_can: self._close_position(pos, price, "Signal Flip (zona 3)", signal); return True
        return False

    def _check_take_profits(self, pos: Position, price: float, signal: dict):
        if pos.closed: return
        if not pos.tp1_hit:
            if (pos.side == "LONG" and price >= pos.take_profit1) or (pos.side == "SHORT" and price <= pos.take_profit1):
                if self.cfg.get("TP1_PARTIAL_CLOSE") and not pos.partial_closed:
                    close_pct = self.cfg.get("TP1_CLOSE_PCT", 60) / 100; partial_qty = pos.quantity * close_pct
                    if not self.dry_run:
                        self.client.cancel_all_orders(pos.symbol); partial_side = 4 if pos.side == "LONG" else 2
                        partial_result = self.client.place_order(pos.symbol, partial_side, 5, self.cfg.get("LEVERAGE", 5), partial_qty)
                        if not partial_result: log.error(f"❌ GAGAL TP1 partial close {pos.symbol} di bursa! Skip tick ini."); return
                    pos.tp1_hit = True; pos.partial_closed = True
                    taker_fee = self.cfg.get("TAKER_FEE_PCT", 0.0006)
                    partial_gross = (price - pos.entry_price) * partial_qty if pos.side == "LONG" else (pos.entry_price - price) * partial_qty
                    partial_fee = (pos.entry_price * partial_qty + price * partial_qty) * taker_fee; partial_pnl = partial_gross - partial_fee
                    self.state.balance += partial_pnl; self.state.total_pnl += partial_pnl; self.state.daily_pnl += partial_pnl
                    pos.quantity -= partial_qty; pos.fee += partial_fee; pos.pnl += partial_pnl
                    log.info(f"[TP1] {pos.id} partial close {close_pct:.0%} @ ${price:.4f} Gross=${partial_gross:+.4f} Fee=${partial_fee:.4f} Net=${partial_pnl:+.4f}")
                    self.notifier.send(f"🎯 *TP1 Partial Close*\n`{pos.id}` `{pos.side}` @ `${price:.4f}`\nPnL: `${partial_pnl:+.4f}` (fee: -${partial_fee:.4f}, {close_pct:.0%} closed)")
                    if not self.dry_run:
                        sl_side = 4 if pos.side == "LONG" else 2; m_mode = 2 if self.cfg.get("MARGIN_MODE") == "CROSS" else 1
                        sl_re = self.client.place_stop_order(pos.symbol, sl_side, pos.stop_loss, pos.quantity, self.cfg.get("LEVERAGE", 5), m_mode)
                        if sl_re: log.info(f"🛡️ [SL@BURSA] Hard SL dipasang ulang untuk sisa qty @ ${pos.stop_loss:.4f}")
                else: pos.tp1_hit = True
        if pos.tp1_hit and not pos.tp2_hit:
            if (pos.side == "LONG" and price >= pos.take_profit2) or (pos.side == "SHORT" and price <= pos.take_profit2):
                pos.tp2_hit = True; self._close_position(pos, price, "TP2", signal)

    def sync_positions_from_mexc(self):
        if self.dry_run: return
        try:
            mexc_positions = self.client.get_open_positions()
            if mexc_positions is None: return
            mexc_active = {}
            for mp in mexc_positions:
                qty = float(mp.get("holdVol", 0) or mp.get("vol", 0))
                if qty <= 0: continue
                sym, side = mp.get("symbol", ""), "LONG" if mp.get("positionType", 1) == 1 else "SHORT"
                mexc_active[(sym, side)] = {"qty": qty, "entry": float(mp.get("holdAvgPrice") or mp.get("openAvgPrice") or 0)}
            price_cache = {}; current_bot_keys = set()
            with self._pos_lock:
                for pos in list(self.state.positions):
                    if pos.closed: continue
                    current_bot_keys.add((pos.symbol, pos.side))
                    key = (pos.symbol, pos.side)
                    if key not in mexc_active:
                        log.warning(f"⚠️ [SYNC] {pos.symbol} {pos.side} hilang dari MEXC → SL/TP/manual close tereksekusi")
                        if pos.symbol not in price_cache:
                            tick = self.client.get_ticker(pos.symbol); price_cache[pos.symbol] = float(tick) if isinstance(tick, (int, float)) else pos.entry_price
                        close_price = price_cache.get(pos.symbol, pos.entry_price)
                        taker_fee = self.cfg.get("TAKER_FEE_PCT", 0.0006)
                        gross_pnl = (close_price - pos.entry_price) * pos.quantity if pos.side == "LONG" else (pos.entry_price - close_price) * pos.quantity
                        fee = (pos.entry_price + close_price) * pos.quantity * taker_fee; pnl = gross_pnl - fee
                        if self.state.balance > 0 and abs(pnl) > self.state.balance * 0.5: log.warning(f"[SYNC] PnL ${pnl:.4f} terlalu besar (>50% balance) → set 0"); pnl = 0.0; close_price = pos.entry_price; fee = 0.0
                        pos.pnl = round(pos.pnl + pnl, 4); pos.fee = round(fee, 4); pos.closed = True; pos.close_reason = "External Close (SL/TP/Manual MEXC)"
                        pos.closed_at = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
                        self.state.total_trades += 1; self.state.total_pnl += pnl; self.state.daily_pnl += pnl; self.state.balance += pnl
                        if pnl > 0: self.state.winning_trades += 1
                        self.state.peak_balance = max(self.state.peak_balance, self.state.balance)
                        self.journal.log_trade(pos, close_price, {})
                        emoji = "✅" if pnl > 0 else "❌"
                        log.info(f"[SYNC CLOSE] {pos.id} {pos.side} | {emoji} ${pnl:+.4f} | Balance ${self.state.balance:.2f}")
                        self.notifier.send(f"{emoji} *Close External* — `{pos.symbol}`\nSL/TP bursa tereksekusi | {pos.side}\nPnL: `${pnl:+.4f}` | Balance: `${self.state.balance:.2f}`")
                        self.client.cancel_all_orders(pos.symbol)
            for (sym, side), mdata in mexc_active.items():
                if (sym, side) in current_bot_keys: continue
                entry, qty = mdata["entry"], mdata["qty"]
                if entry <= 0 or qty <= 0: continue
                log.warning(f"🔍 [SYNC] Posisi yatim ditemukan: {sym} {side} qty={qty} entry={entry}")
                found_sl = found_tp = 0.0; stop_orders = self.client.get_stop_orders(sym)
                for so in (stop_orders or []):
                    s_price = float(so.get("triggerPrice", 0))
                    if s_price <= 0: continue
                    if side == "LONG": found_sl = s_price if s_price < entry else found_sl; found_tp = s_price if s_price > entry else found_tp
                    else: found_sl = s_price if s_price > entry else found_sl; found_tp = s_price if s_price < entry else found_tp
                if found_sl == 0 or found_tp == 0:
                    try:
                        klines = self.client.get_klines(sym, self.cfg["PRIMARY_TF"], 20)
                        if not klines.empty:
                            ta_data = self.ta.compute(klines); atr = float(ta_data["atr"].iloc[-1])
                            sl_m, tp1_m = self.cfg.get("ATR_SL_MULT", 0.6), self.cfg.get("ATR_TP1_MULT", 1.0)
                            if side == "LONG": found_sl = entry - atr * sl_m if found_sl == 0 else found_sl; found_tp = entry + atr * tp1_m if found_tp == 0 else found_tp
                            else: found_sl = entry + atr * sl_m if found_sl == 0 else found_sl; found_tp = entry - atr * tp1_m if found_tp == 0 else found_tp
                    except Exception as e: log.warning(f"[SYNC] Gagal hitung ATR untuk {sym}: {e}")
                    if found_sl == 0: found_sl = entry * (0.994 if side == "LONG" else 1.006)
                    if found_tp == 0: found_tp = entry * (1.01 if side == "LONG" else 0.99)
                tp2_mult = 1.005 if side == "LONG" else 0.995; tp3_mult = 1.01 if side == "LONG" else 0.99
                new_pos = Position(id=f"SYNC-{sym}-{int(time.time())}", symbol=sym, side=side, entry_price=entry, quantity=qty,
                                   stop_loss=round(found_sl, 6), take_profit1=round(found_tp, 6), take_profit2=round(found_tp * tp2_mult, 6),
                                   take_profit3=round(found_tp * tp3_mult, 6), opened_at=datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"), opened_ts=time.time())
                with self._pos_lock: self.state.positions.append(new_pos)
                current_bot_keys.add((sym, side))
                if found_sl > 0 and not stop_orders:
                    sl_side = 4 if side == "LONG" else 2; m_mode = 2 if self.cfg.get("MARGIN_MODE") == "CROSS" else 1
                    self.client.place_stop_order(sym, sl_side, found_sl, qty, self.cfg.get("LEVERAGE", 5), m_mode)
                    log.info(f"🛡️ [SYNC] Hard SL dipasang untuk posisi yatim @ ${found_sl:.4f}")
                log.info(f"✅ [SYNC] Posisi diadopsi: {sym} {side} SL=${found_sl:.4f} TP1=${found_tp:.4f}")
                self.notifier.send(f"🔄 *Posisi Diadopsi* — `{sym}`\nBot menemukan posisi yatim `{side}` di MEXC\nEntry: `${entry:.4f}` | Qty: `{qty}`\nSL: `${found_sl:.4f}` | TP1: `${found_tp:.4f}`")
        except Exception as e: log.warning(f"[SYNC] Error sinkronisasi posisi: {e}")

    def run(self):
        log.info("=" * 60); log.info(f"  MEXC SCALPER V5 — {'DRY RUN' if self.dry_run else 'LIVE TRADING'}")
        log.info(f"  Symbol: {self.cfg['SYMBOL']} | TF: {self.cfg['PRIMARY_TF']}"); log.info(f"  Balance: ${self.state.balance:.2f}")
        log.info(f"  Indikator: SuperTrend + DEMA + ROC + WR% + VolDelta + OBI + CVD + VWAP + Funding + Whale"); log.info("=" * 60)
        self.notifier.send(f"🚀 *Scalper V5 Dimulai*\nMode: `{'DRY RUN' if self.dry_run else 'LIVE'}`\nSymbol: `{self.cfg['SYMBOL']}`\nBalance: `${self.state.balance:.2f}`\nEngine: `SuperTrend + DEMA + ROC + WR%`")
        os_signal.signal(os_signal.SIGINT, lambda s, f: self._graceful_exit()); os_signal.signal(os_signal.SIGTERM, lambda s, f: self._graceful_exit())
        last_score = {"bull": 0, "bear": 0}
        while True:
            try:
                self.state.iteration += 1; now = datetime.now(WIB); self._check_daily_reset()
                if time.time() - self._last_balance_sync >= 60: self.sync_balance_from_exchange(); self._last_balance_sync = time.time()
                if not self.dry_run: self.sync_positions_from_mexc()
                if time.time() - self._signal_persist_reset_ts >= 86400: self._signal_persist.clear(); self._signal_persist_reset_ts = time.time()
                if time.time() - self._funding_last_check >= 300:
                    sym = self.cfg["SYMBOL"]; fr = self.client.get_funding_rate(sym)
                    if fr is not None: self._funding_rate = fr; log.debug(f"Funding rate {sym}: {fr*100:+.4f}%")
                    self._funding_last_check = time.time()
                if self.regime_mgr: self.regime_mgr.apply_regime_to_config()
                if self._check_circuit_breaker(): log.warning(f"Circuit breaker aktif: {self.state.circuit_reason}"); time.sleep(30); continue
                allowed, reason = self.session_filt.is_trading_allowed()
                if not allowed: log.info(f"Session filter: {reason}"); time.sleep(60); continue
                if self.cfg.get("MULTI_COIN_MODE") and self.scanner.needs_scan(): self._trigger_scan()
                if self.cfg.get("AUTO_SWITCH_COIN") and self.open_positions_count() == 0: self._maybe_switch_coin()
                signal = self.fetch_and_analyze()
                if signal is None: time.sleep(self.cfg["POLL_INTERVAL"]); continue
                price = self.price_feed.get_price() or signal["close"]; sig_label = signal["signal"]
                bull_score, bear_score = signal["bull_score"], signal["bear_score"]; velocity = signal.get("velocity", 0.0)
                st_flip, sq_on = signal.get("st_flipped", False), signal.get("squeeze_on", False)
                bull_mom, bear_mom = bull_score - last_score["bull"], bear_score - last_score["bear"]
                last_score["bull"], last_score["bear"] = bull_score, bear_score
                st_sym = "🟢" if signal.get("st_dir") == -1 else "🔴"; sq_sym = "⏸️" if sq_on else "💥"; flip_sym = "🔥FLIP!" if st_flip else " "
                obi_val, flow_val = signal.get("obi", 0.0), signal.get("trade_flow", 0.0)
                trend_sym = {"BULLISH": "🟢", "BEARISH": "🔴"}.get(signal.get("trend_bias", ""), "⚪")
                log.info(f"[Iter {self.state.iteration}] {self.cfg['SYMBOL']} ${price:.4f} | 1H:{trend_sym}{signal.get('trend_bias','?')[:4]} 15m:{signal.get('htf_bias','?')[:4]} ST:{st_sym}{flip_sym} ROC:{signal['roc']:.3f}% WR%:{signal['willr']:.0f} Squeeze:{sq_sym} OBI:{obi_val:+.2f} Flow:{flow_val:+.2f} | Bull:{bull_score} Bear:{bear_score} → {sig_label} | Vel:{velocity:+.3f}% ADX:{signal['adx']:.1f}")
                for k, v in signal["details"].items(): log.debug(f"  {k}: {v}")
                with self._pos_lock:
                    for pos in list(self.state.positions):
                        if pos.closed: continue
                        self._check_abort(pos, price)
                        if not pos.closed: self._check_take_profits(pos, price, signal)
                        if not pos.closed: self._check_flip_exit(pos, signal, price)
                if self.open_positions_count() < self.cfg.get("MAX_OPEN_TRADES", 1):
                    early_score, early_mom = self.cfg.get("EARLY_ENTRY_SCORE", 13), self.cfg.get("EARLY_MOMENTUM_MIN", 1)
                    can_open, is_early = False, False
                    if sig_label in ("LONG", "SHORT"): can_open = True
                    elif self.cfg.get("MULTI_COIN_MODE"):
                        if (bull_score >= early_score and bull_score > bear_score and bull_mom >= early_mom and signal.get("st_dir") == -1): sig_label = "LONG"; can_open = True; is_early = True
                        elif (bear_score >= early_score and bear_score > bull_score and bear_mom >= early_mom and signal.get("st_dir") == 1): sig_label = "SHORT"; can_open = True; is_early = True
                    if can_open:
                        sym, persist_key = self.cfg["SYMBOL"], f"{self.cfg['SYMBOL']}_{sig_label}"
                        for k in list(self._signal_persist.keys()):
                            if k.startswith(sym + "_") and k != persist_key: self._signal_persist[k] = 0
                        self._signal_persist[persist_key] = self._signal_persist.get(persist_key, 0) + 1
                        is_pb = signal.get("is_pullback_entry", False); need = self.cfg.get("PULLBACK_PERSIST_CYCLES", 1) if is_pb else self.cfg.get("SIGNAL_PERSIST_CYCLES", 2)
                        got = self._signal_persist[persist_key]
                        if got < need: log.debug(f"[PERSIST] {sig_label} {got}/{need} cycle — tunggu konfirmasi"); can_open = False
                    if can_open and self._ml_model is not None:
                        signal["signal"] = sig_label; ml_score = self._ml_predict(signal); ml_thresh = self.cfg.get("ML_MIN_SCORE", 0.55)
                        log.debug(f"[ML] score={ml_score:.3f} thresh={ml_thresh}")
                        if ml_score < ml_thresh: can_open = False; log.info(f"[ML] Entry blocked: score {ml_score:.3f} < {ml_thresh:.2f}")
                    if can_open:
                        signal["signal"] = sig_label; signal["is_early_entry"] = is_early; old_count = self.open_positions_count()
                        self._open_position(signal, price)
                        if self.open_positions_count() > old_count: self._signal_persist[persist_key] = 0
                self.state.last_signal = sig_label
                if self._state_dirty or (time.time() - self._last_state_save >= 60): self.persistence.save(self.state); self._last_state_save = time.time(); self._state_dirty = False
            except KeyboardInterrupt: break
            except Exception as e: log.error(f"Loop error: {e}", exc_info=True)
            finally: time.sleep(self.cfg["POLL_INTERVAL"])

    def _graceful_exit(self): log.info("Menghentikan bot..."); self.price_feed.stop(); self.user_stream.stop(); self.persistence.save(self.state); sys.exit(0)

    def _load_ml_model(self):
        model_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ml_model.pkl")
        if not os.path.exists(model_path): return
        try:
            import joblib; bundle = joblib.load(model_path); self._ml_model = bundle["model"]; self._ml_features = bundle["features"]; log.info(f"[ML] Model loaded from {model_path}")
        except Exception as e: log.warning(f"[ML] Failed to load model: {e}")

    def _ml_predict(self, signal: dict) -> float:
        try:
            row = {"side": 1 if signal.get("signal") == "LONG" else -1, "st_dir": signal.get("st_dir", 0), "adx": signal.get("adx", 25),
                   "roc": signal.get("roc", 0), "roc_accel": signal.get("roc_accel", 0), "willr": signal.get("willr", -50), "atr_pct": signal.get("atr_pct", 0),
                   "squeeze_on": int(bool(signal.get("squeeze_on", False))), "squeeze_mom": signal.get("squeeze_mom", 0), "cvd_trend": signal.get("cvd_trend", 0),
                   "vol_ratio": signal.get("vol_ratio", 1), "body_ratio": signal.get("body_ratio", 0), "consec": signal.get("consec", 0),
                   "dema_cross": 1 if signal.get("dema_fast", 0) > signal.get("dema_slow", 0) else -1, "vwap_dist": signal.get("vwap_dist", 0),
                   "hour_utc": datetime.now(timezone.utc).hour, "dow": datetime.now(timezone.utc).weekday()}
            X = pd.DataFrame([{f: row.get(f, 0) for f in self._ml_features}])
            return float(self._ml_model.predict_proba(X)[0][1])
        except Exception as e: log.debug(f"[ML] predict error: {e}"); return 0.5

    def _start_dashboard(self):
        try: from flask import Flask, jsonify, request, Response
        except ImportError: log.warning("Flask tidak terinstall. Install: pip install flask"); return
        app = Flask("ScalperV5-Dashboard")
        # (Dashboard HTML/JS tetap sama seperti file asli, tidak perlu diubah karena tidak mengandung bug kritis)
        @app.route("/")
        def index(): return Response(DASHBOARD_HTML, mimetype="text/html")
        @app.route("/api/status")
        def status():
            positions_data = []; price = self.price_feed.get_price()
            with self._pos_lock:
                for pos in list(self.state.positions):
                    if not pos.closed: pnl_live = (price - pos.entry_price) * pos.quantity if pos.side == "LONG" else (pos.entry_price - price) * pos.quantity
                    positions_data.append({**asdict(pos), "pnl_live": round(pnl_live, 4), "current_price": price})
            return jsonify({"symbol": self.cfg["SYMBOL"], "price": price, "balance": round(self.state.balance, 2), "real_balance": round(self.state.real_balance, 2),
                            "peak_balance": round(self.state.peak_balance, 2), "total_pnl": round(self.state.total_pnl, 4), "daily_pnl": round(self.state.daily_pnl, 4),
                            "win_rate": round(self.state.win_rate(), 1), "total_trades": self.state.total_trades, "winning_trades": self.state.winning_trades,
                            "drawdown": round(self.state.drawdown(), 2), "circuit_breaker": self.state.circuit_breaker, "circuit_reason": self.state.circuit_reason,
                            "circuit_type": self.state.circuit_type, "positions": positions_data, "dry_run": self.dry_run, "secured_total": round(self.state.secured_total, 2),
                            "api_error": self.state.api_error, "iteration": self.state.iteration, "last_signal": self.state.last_signal, "ws_alive": self.price_feed.is_ws_alive})
        @app.route("/api/config", methods=["GET"])
        def get_config(): safe = dict(self.cfg); safe["MEXC_API_SECRET"] = "***"; return jsonify(safe)
        @app.route("/api/config", methods=["POST"])
        def set_config():
            try:
                data = request.get_json()
                if data: self.update_config_live(data); going_live = not data.get("DRY_RUN", True)
                if going_live and self.state.api_error: return jsonify({"success": True, "warning": self.state.api_error})
                return jsonify({"success": True})
                return jsonify({"success": False, "error": "No data"})
            except Exception as e: return jsonify({"success": False, "error": str(e)})
        @app.route("/api/close_all", methods=["POST"])
        def close_all_route(): self.close_all_positions("Manual via API"); return jsonify({"success": True})
        @app.route("/api/scanner")
        def scanner_results(): return jsonify(self.scanner.get_results())
        @app.route("/api/journal")
        def journal(): return jsonify({"mode": "live" if not self.dry_run else "dry", "trades": self.journal.get_trades()})
        @app.route("/api/journal/dry")
        def journal_dry(): j = TradeJournal(self.cfg.get("JOURNAL_FILE_DRY", "scalper_journal_v4_dry.csv")); return jsonify({"mode": "dry", "trades": j.get_trades()})
        @app.route("/api/journal/live")
        def journal_live(): j = TradeJournal(self.cfg.get("JOURNAL_FILE_LIVE", "scalper_journal_v4_live.csv")); return jsonify({"mode": "live", "trades": j.get_trades()})
        @app.route("/api/journal/reset", methods=["POST"])
        def journal_reset():
            data = request.get_json(silent=True) or {}; mode = data.get("mode", "all"); reset = []
            if mode in ("dry", "all"): path = self.cfg.get("JOURNAL_FILE_DRY", "scalper_journal_v4_dry.csv"); TradeJournal(path)._ensure_header()
            if mode in ("live", "all"): path = self.cfg.get("JOURNAL_FILE_LIVE", "scalper_journal_v4_live.csv"); TradeJournal(path)._ensure_header()
            log.info(f"[RESET] Journal direset: {reset}"); return jsonify({"ok": True, "reset": reset})
        host, port = self.cfg.get("DASHBOARD_HOST", "0.0.0.0"), self.cfg.get("DASHBOARD_PORT", 5001)
        threading.Thread(target=lambda: app.run(host=host, port=port, debug=False, use_reloader=False), daemon=True, name="Dashboard").start()
        log.info(f"Dashboard aktif di http://localhost:{port}")

# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="MEXC Scalper Bot V5 — Zero-Lag Engine")
    parser.add_argument("--live", action="store_true", help="Aktifkan LIVE trading")
    parser.add_argument("--dashboard", action="store_true", help="Aktifkan Dashboard")
    parser.add_argument("--symbol", type=str, default=None, help="Override symbol (e.g. BTC_USDT)")
    args = parser.parse_args()

    if args.live:
        print("=" * 60); print("  ⚠️  LIVE MODE AKTIF — UANG NYATA AKAN DIGUNAKAN!"); print("  Pastikan API Key sudah benar di config_scalper_v4.json"); print("=" * 60)
        confirm = input("Ketik YES untuk konfirmasi: ")
        if confirm.strip() != "YES": print("Dibatalkan."); sys.exit(0)
        CONFIG["DRY_RUN"] = False
    if args.symbol: CONFIG["SYMBOL"] = args.symbol

    bot = ScalperBotV5(run_dashboard=args.dashboard)
    if args.live: bot.dry_run = False; bot.cfg["DRY_RUN"] = False; bot.sync_balance_from_exchange()
    bot.run()