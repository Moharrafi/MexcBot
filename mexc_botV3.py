"""
MEXC PRO TRADING BOT V3
================================================================
UPGRADE dari V2:
  ✅ Multi-Coin Scanner — scan top 30 koin otomatis
  ✅ Profit-Zone Aware Exit — smart signal flip handler
  ✅ Early Signal Detection — masuk sebelum sinyal penuh
  ✅ Auto Coin Switch — pindah ke koin terkuat otomatis
  ✅ Composite Scoring — skor gabungan: signal + momentum + ATR + volume
================================================================
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
import argparse
import ssl
import signal as os_signal
from datetime import datetime, timezone, timedelta
from urllib.parse import urlencode
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Tuple
from collections import deque

import requests
import pandas as pd
import pandas_ta_remake as ta
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════
#  KONFIGURASI LENGKAP
# ══════════════════════════════════════════════════════════════

CONFIG = {
    # ─── Pair & Timeframe ───────────────────────────────────
    "SYMBOL":               "PIPPIN_USDT",      # Koin default (akan diganti auto jika MULTI_COIN_MODE aktif)
    "PRIMARY_TF":           "1m",
    "CONFIRM_TF":           "3m",
    "CANDLE_LIMIT":         300,

    # ─── Mode ───────────────────────────────────────────────
    "DRY_RUN":              True,
    "VIRTUAL_BALANCE":      100.0,

    # ─── ✨ MULTI-COIN SCANNER (NEW V3) ─────────────────────
    "MULTI_COIN_MODE":      True,           # Aktifkan scanner multi-koin
    "SCAN_TOP_N":           25,             # Scan top N koin by volume
    "SCAN_INTERVAL":        120,            # Scan ulang setiap N detik
    "SCAN_MIN_VOLUME":      500_000,        # Min 24h turnover USD agar likuid
    "SCAN_MIN_ADX":         22,             # Koin harus trending, bukan sideways
    "SCAN_MIN_ATR_PCT":     0.3,            # Min volatilitas 0.3% per candle
    "AUTO_SWITCH_COIN":     True,           # Otomatis pindah ke koin terkuat
    "SWITCH_MIN_ADVANTAGE": 3.0,            # Composite score koin baru harus unggul min ini
    "BLACKLIST_COINS":      [],             # Koin yang tidak mau di-trade, contoh: ["BTC_USDT"]

    # ─── ✨ EARLY SIGNAL DETECTION (NEW V3) ─────────────────
    "EARLY_ENTRY_MODE":     True,           # Masuk lebih awal sebelum sinyal penuh
    "EARLY_ENTRY_SCORE":    6,              # Entry jika skor >= ini DAN momentum naik
    "EARLY_MOMENTUM_MIN":   2,              # Skor harus naik minimal N poin dari iterasi lalu

    # ─── 🛡️ ANTI-RAPID-FIRE & SANITY (FIX V3.1) ──────────
    "LOSS_COOLDOWN_SEC":    120,            # Cooldown N detik setelah loss sebelum entry lagi
    "MAX_ATR_PCT_ENTRY":    5.0,            # Jangan entry koin dengan ATR% > ini (terlalu volatile)
    "GRACE_PERIOD_SEC":     30,             # Grace period: trailing/BE tidak aktif N detik pertama
    "SWITCH_IDLE_MAX_SEC":  300,            # Jika stuck NEUTRAL > N detik, paksa switch ke top koin

    # ─── 🔥 TREND POWER SYSTEM (V3.2 — SNIPER MODE) ──────
    "USE_TREND_POWER":      True,           # Aktifkan Trend Power gate
    "MIN_TREND_POWER":      55,             # Entry normal hanya jika trend power >= ini (0-100)
    "MIN_TREND_POWER_EARLY": 40,            # Early entry boleh masuk saat trend baru terbentuk (lebih rendah)
    "MAX_EXTENSION_ATR":    2.0,            # Jangan entry jika harga > N*ATR dari EMA trend
    "ADX_RISING_REQUIRED":  True,           # ADX harus naik (trend menguat, bukan melemah)
    "DYNAMIC_SL_TP":        True,           # SL/TP otomatis menyesuaikan kekuatan trend

    # ─── ✨ PROFIT-ZONE AWARE FLIP (NEW V3) ─────────────────
    "EXIT_ON_SIGNAL_FLIP":  True,
    "FLIP_ZONE1_PCT":       0.005,          # < 0.5% profit → exit langsung saat flip
    "FLIP_ZONE2_PCT":       0.02,           # 0.5-2% profit → hanya exit jika skor lawan kuat
    "FLIP_ZONE2_MIN_SCORE": 11,             # Skor lawan minimum untuk exit di zona 2
    "FLIP_ZONE3_MIN_SCORE": 12,             # Skor lawan minimum untuk exit di zona 3
    "FLIP_ZONE3_CANDLES":   2,              # Butuh N candle konfirmasi di zona 3

    # ─── Manajemen Risiko ───────────────────────────────────
    "RISK_PER_TRADE":       0.10,
    "LEVERAGE":             25,
    "MAX_MARGIN_PCT":       0.20,
    "USE_KELLY":            False,
    "ATR_SL_MULT":          1.0,
    "ATR_TP1_MULT":         1.2,
    "ATR_TP2_MULT":         2.5,
    "ATR_TP3_MULT":         5.0,
    "MAX_OPEN_TRADES":      1,
    "MIN_RR_RATIO":         1.5,
    "MAX_DAILY_LOSS_PCT":   0.20,
    "MAX_DRAWDOWN_PCT":     0.40,
    "AUTO_REVERSE_ON_FLIP": False,

    # ─── Break-Even & Partial Close ──────────────────────────
    "USE_BE_FILTER":        True,
    "BE_ACTIVATION_PCT":    0.005,
    "BE_FEE_BUFFER_PCT":    0.0015,
    "TP1_PARTIAL_CLOSE":    True,
    "TP1_CLOSE_PCT":        50,

    # ─── Filter Probabilitas TP ──────────────────────────────
    "USE_PROBABILITY_FILTER": True,
    "MIN_VOL_RATIO":          1.2,
    "MAX_ATR_DISTANCE_MULT":  2.5,
    "MIN_TP_DISTANCE_PCT":    0.15,

    # ─── Trailing Stop ───────────────────────────────────────
    "USE_TRAILING_STOP":    True,
    "TRAIL_ACTIVATION_PCT": 0.01,
    "TRAIL_DISTANCE_PCT":   0.005,

    # ─── Indikator ───────────────────────────────────────────
    "RSI_PERIOD":           14,
    "RSI_OVERSOLD":         38,
    "RSI_OVERBOUGHT":       62,
    "EMA_FAST":             9,
    "EMA_SLOW":             21,
    "EMA_TREND":            50,
    "EMA_LONG":             200,
    "MACD_FAST":            12,
    "MACD_SLOW":            26,
    "MACD_SIGNAL":          9,
    "BB_PERIOD":            20,
    "BB_STD":               2.0,
    "ATR_PERIOD":           14,
    "STOCH_K":              14,
    "STOCH_D":              3,
    "STOCH_SMOOTH":         3,
    "STOCH_OVERSOLD":       25,
    "STOCH_OVERBOUGHT":     75,

    # ─── Profit Securing ─────────────────────────────────────
    "ENABLE_AUTO_SECURE":   True,
    "SECURE_PROFIT_PCT":    50,
    "MIN_SECURE_TRANSFER":  1.0,
    "REQUIRE_MTF_CONFIRM":  True,

    # ─── Sinyal Threshold ────────────────────────────────────
    "MIN_BULL_SCORE":       8,
    "MIN_BEAR_SCORE":       8,

    # ─── Anti-Sideways (ADX) ─────────────────────────────────
    "USE_ADX_FILTER":       True,
    "ADX_MIN_THRESHOLD":    20,
    "ADX_PERIOD":           14,

    # ─── Session Filter ──────────────────────────────────────
    "USE_SESSION_FILTER":   False,
    "ALLOWED_HOURS_UTC":    list(range(0, 24)),
    "BLOCK_FRIDAY_CLOSE":   False,
    "BLOCK_SUNDAY_OPEN":    False,
    "NEWS_BLACKOUT":        [],

    # ─── Loop ────────────────────────────────────────────────
    "POLL_INTERVAL":        5,
    "PRICE_UPDATE_INTERVAL": 3,

    # ─── Logging & Persistence ───────────────────────────────
    "LOG_FILE":             "bot_v3.log",
    "LOG_LEVEL":            "INFO",
    "STATE_FILE":           "bot_state_v3.json",
    "JOURNAL_FILE":         "trade_journal_v3.csv",

    # ─── Dashboard ───────────────────────────────────────────
    "DASHBOARD_HOST":       "0.0.0.0",
    "DASHBOARD_PORT":       5000,
    "PRESET_MODE":          "CUSTOM",
    "SCAN_TOP_N":           25,
    "SCAN_INTERVAL":        120,
    "SCAN_MIN_VOLUME":      500000,
    "SCAN_MIN_ADX":         10,
    "SCAN_MIN_ATR_PCT":     0.05,
    "AUTO_SWITCH_COIN":     True,
    
    # ─── API Keys ────────────────────────────────────────────
    "MEXC_API_KEY":         "",
    "MEXC_API_SECRET":      "",
}

# ══════════════════════════════════════════════════════════════
#  ENV & LOGGING
# ══════════════════════════════════════════════════════════════

MEXC_API_KEY    = os.getenv("MEXC_API_KEY", "")
MEXC_API_SECRET = os.getenv("MEXC_API_SECRET", "")
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT   = os.getenv("TELEGRAM_CHAT_ID", "")
MEXC_BASE_URL   = "https://contract.mexc.com"

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
log = logging.getLogger("BotV3")

# ══════════════════════════════════════════════════════════════
#  DATA STRUCTURES
# ══════════════════════════════════════════════════════════════

@dataclass
class Position:
    id: str
    symbol: str                          # ✨ V3: track per-coin
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
    closed: bool = False
    close_reason: str = ""
    closed_at: str = ""
    flip_count: int = 0                  # ✨ V3: untuk zona 3 konfirmasi
    opened_ts: float = 0.0               # 🛡️ V3.1: timestamp epoch untuk grace period

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
    active_symbol: str = ""              # ✨ V3: koin yang sedang aktif
    api_error: str = ""

    def win_rate(self) -> float:
        return (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0.0

    def drawdown(self) -> float:
        if self.peak_balance <= 0:
            return 0.0
        return (self.peak_balance - self.balance) / self.peak_balance * 100

# ══════════════════════════════════════════════════════════════
#  MEXC REST API CLIENT
# ══════════════════════════════════════════════════════════════

class MEXCFuturesClient:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.session    = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
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
        log.info(f"[CONTRACT] {symbol} contractSize={cs}")
        return cs

    def _sign(self, timestamp: str, payload: str = "") -> str:
        message = f"{self.api_key}{timestamp}{payload}"
        return hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha256).hexdigest()

    def _request(self, method: str, ep: str, params: dict = None,
                 _retry: int = 0) -> Optional[dict]:
        timestamp = str(int(time.time() * 1000))
        url = MEXC_BASE_URL + ep
        payload = json.dumps(params) if method == "POST" and params else ""
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
                log.warning(f"[API] Empty response HTTP {r.status_code} {ep}")
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
            log.error(f"Request error {ep} HTTP={getattr(r,'status_code','?')} body={getattr(r,'text','')[:200]!r}: {e}")
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
                change   = float(x.get("riseFallRate", 0))
                results.append({
                    "symbol":     sym,
                    "turnover":   turnover,
                    "last_price": float(x.get("lastPrice", 0)),
                    "change":     change,
                })
            except (ValueError, TypeError):
                continue
        results.sort(key=lambda x: x["turnover"], reverse=True)
        return results[:limit]

    def get_balance(self, asset: str = "USDT") -> Optional[float]:
        d = self._request("GET", "/api/v1/private/account/assets")
        log.info(f"Fetch Balance -> {d}")
        if d is None: return None
        if not d: return 0.0
        for b in d:
            if b.get("currency") == asset:
                # 🚀 V3.2: Perluas pencarian kunci saldo (USDT Futures MEXC)
                # Gunakan equity (total termasuk unrealized PnL) bukan availableBalance
                val = b.get("equity") or b.get("availableBalance") or \
                      b.get("availableMargin") or b.get("availableMarginAmount") or \
                      b.get("cashBalance") or b.get("available") or 0.0
                log.info(f"💰 Found {asset} Balance: {val} (Keys: {list(b.keys())})")
                return float(val)
        log.warning(f"⚠️ Aset {asset} tidak ditemukan di wallet Futures Anda!")
        return 0.0

    def get_klines(self, symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
        # MEXC Futures TF mapping
        tf_map = {
            "1m": ("Min1", 60), "3m": ("Min3", 180), "5m": ("Min5", 300),
            "15m": ("Min15", 900), "30m": ("Min30", 1800),
            "1h": ("Min60", 3600), "4h": ("Hour4", 14400),
            "8h": ("Hour8", 28800), "1d": ("Day1", 86400)
        }
        tf_info = tf_map.get(interval, ("Min5", 300))
        tf_name = tf_info[0]
        tf_seconds = tf_info[1]

        # MEXC Futures API uses start/end timestamps (seconds), NOT limit
        end_ts = int(time.time())
        start_ts = end_ts - (limit * tf_seconds)

        params = {"interval": tf_name, "start": start_ts, "end": end_ts}
        d = self._request("GET", f"/api/v1/contract/kline/{symbol}", params)

        if d is None:
            return pd.DataFrame()
        if not d:  # empty list
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
        log.info(f"[ORDER] {symbol} side={side} qty={quantity} cs={contract_size} vol={vol}")
        # Fresh session per order — hindari Cloudflare WAF memblokir koneksi lama
        payload = json.dumps(params, separators=(',', ':'))
        timestamp = str(int(time.time() * 1000))
        signature = self._sign(timestamp, payload)
        for attempt in range(2):
            try:
                with requests.Session() as s:
                    s.headers.update({
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "application/json, text/plain, */*",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Origin": "https://www.mexc.com",
                        "Referer": "https://www.mexc.com/futures",
                    })
                    r = s.post(
                        MEXC_BASE_URL + "/api/v1/private/order/create",
                        headers={"ApiKey": self.api_key, "Request-Time": timestamp,
                                 "Signature": signature, "Content-Type": "application/json"},
                        data=payload, timeout=15
                    )
                    log.info(f"[ORDER] HTTP {r.status_code} body={r.content[:120]!r}")
                    body = r.content.strip()
                    if not body:
                        log.warning(f"[ORDER] Empty response attempt {attempt+1}")
                    elif body.startswith(b'<'):
                        log.warning(f"[ORDER] HTML response (WAF block) attempt {attempt+1}")
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

    def get_open_positions(self, symbol: str = None) -> list:
        params = {"symbol": symbol} if symbol else {}
        for ep in ["/api/v1/private/position/open_positions", "/api/v1/private/position/open_details"]:
            data = self._request("GET", ep, params)
            if data is not None:
                return data if isinstance(data, list) else []
        return []


class MEXCSpotClient:
    def __init__(self, api_key: str, api_secret: str):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.base_url   = "https://api.mexc.com"
        self.session    = requests.Session()
        self.session.verify = False

        class TLSAdapter(requests.adapters.HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                ctx = ssl.create_default_context()
                ctx.set_ciphers('DEFAULT@SECLEVEL=1')
                ctx.check_hostname = False
                kwargs['ssl_context'] = ctx
                return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

        self.session.mount("https://", TLSAdapter())

    def _sign(self, query_string: str) -> str:
        return hmac.new(self.api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()

    def transfer_to_spot(self, amount: float) -> bool:
        endpoint  = "/api/v3/capital/transfer"
        timestamp = int(time.time() * 1000)
        params    = {
            "fromAccountType": "FUTURES", "toAccountType": "SPOT",
            "asset": "USDT", "amount": str(round(amount, 4)), "timestamp": timestamp
        }
        qs  = "&".join([f"{k}={v}" for k, v in params.items()])
        sig = self._sign(qs)
        url = f"{self.base_url}{endpoint}?{qs}&signature={sig}"
        headers = {"X-MEXC-APIKEY": self.api_key}
        try:
            r   = self.session.post(url, headers=headers, timeout=15)
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
#  PRICE FEED
# ══════════════════════════════════════════════════════════════
#  🚀 V3.3: WebSocket Price Feed — Latensi milidetik
# ══════════════════════════════════════════════════════════════

MEXC_WS_URL = "wss://contract.mexc.com/edge"

class PriceFeed:
    """
    🔥 V3.3 WebSocket PriceFeed — Real-time price dengan auto-reconnect.
    Fallback ke REST polling jika WebSocket gagal total.
    """
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.price  = 0.0
        self.client = MEXCFuturesClient(MEXC_API_KEY, MEXC_API_SECRET)
        self._callbacks = []
        self._running   = False
        self._lock      = threading.Lock()
        self._ws        = None
        self._ws_connected = False
        self._ws_last_msg  = 0.0
        self._reconnect_count = 0
        self._max_reconnect   = 50

    def add_callback(self, cb):
        self._callbacks.append(cb)

    def start(self):
        self._running = True
        # Thread utama: WebSocket
        threading.Thread(target=self._ws_loop, daemon=True, name="WS-PriceFeed").start()
        # Thread backup: REST fallback (jaga kalau WS mati)
        threading.Thread(target=self._rest_fallback, daemon=True, name="REST-Fallback").start()

    def stop(self):
        self._running = False
        if self._ws:
            try: self._ws.close()
            except: pass

    def get_price(self) -> float:
        with self._lock:
            return self.price

    @property
    def is_ws_alive(self) -> bool:
        return self._ws_connected and (time.time() - self._ws_last_msg < 15)

    def _fire_callbacks(self, price: float):
        with self._lock:
            self.price = price
        for cb in self._callbacks:
            try: cb(price)
            except: pass

    # ── WebSocket Engine ──────────────────────────────────────
    def _ws_loop(self):
        try:
            import websocket
        except ImportError:
            log.warning("⚠️ websocket-client not installed. Falling back to REST only.")
            return

        while self._running and self._reconnect_count < self._max_reconnect:
            try:
                log.info(f"🔌 WebSocket menghubungkan ke MEXC ({self.symbol})...")
                ws = websocket.WebSocketApp(
                    MEXC_WS_URL,
                    on_open=self._on_ws_open,
                    on_message=self._on_ws_message,
                    on_error=self._on_ws_error,
                    on_close=self._on_ws_close,
                )
                self._ws = ws
                ws.run_forever(
                    ping_interval=20,
                    ping_timeout=10,
                    sslopt={"cert_reqs": ssl.CERT_NONE},
                )
            except Exception as e:
                log.warning(f"🔌 WebSocket crash: {e}")

            self._ws_connected = False
            self._reconnect_count += 1
            if self._running:
                wait = min(5 * self._reconnect_count, 60)
                log.info(f"🔄 Reconnect #{self._reconnect_count} dalam {wait}s...")
                time.sleep(wait)

        if self._reconnect_count >= self._max_reconnect:
            log.error("❌ WebSocket gagal total setelah 50x reconnect. Hanya REST fallback aktif.")

    def _on_ws_open(self, ws):
        self._ws_connected = True
        self._reconnect_count = 0
        log.info(f"✅ WebSocket terhubung! Subscribe: {self.symbol}")
        # Subscribe to ticker
        sub_msg = json.dumps({
            "method": "sub.ticker",
            "param": {"symbol": self.symbol}
        })
        ws.send(sub_msg)

    def _on_ws_message(self, ws, message):
        try:
            data = json.loads(message)
            # MEXC Futures WebSocket ticker format
            if data.get("channel") == "push.ticker":
                ticker_data = data.get("data", {})
                last_price = ticker_data.get("lastPrice")
                if last_price:
                    price = float(last_price)
                    self._ws_last_msg = time.time()
                    self._fire_callbacks(price)
            elif data.get("data") and isinstance(data["data"], dict):
                # Alternative response format
                last_price = data["data"].get("lastPrice")
                if last_price:
                    price = float(last_price)
                    self._ws_last_msg = time.time()
                    self._fire_callbacks(price)
        except Exception:
            pass

    def _on_ws_error(self, ws, error):
        log.debug(f"🔌 WS error: {error}")

    def _on_ws_close(self, ws, close_code, close_msg):
        self._ws_connected = False
        log.info(f"🔌 WebSocket terputus (code={close_code})")

    # ── Ganti Subscription (saat Auto-Switch koin) ────────────
    def resubscribe(self, new_symbol: str):
        """Ganti langganan ticker ke koin baru tanpa restart WebSocket."""
        old_symbol = self.symbol
        self.symbol = new_symbol
        if self._ws and self._ws_connected:
            try:
                # Unsub koin lama
                self._ws.send(json.dumps({
                    "method": "unsub.ticker",
                    "param": {"symbol": old_symbol}
                }))
                # Sub koin baru
                self._ws.send(json.dumps({
                    "method": "sub.ticker",
                    "param": {"symbol": new_symbol}
                }))
                log.info(f"🔄 WS resubscribe: {old_symbol} → {new_symbol}")
            except Exception as e:
                log.warning(f"⚠️ WS resubscribe error: {e}")

    # ── REST Fallback (jika WebSocket mati) ───────────────────
    def _rest_fallback(self):
        while self._running:
            try:
                if not self.is_ws_alive:
                    p = self.client.get_ticker(self.symbol)
                    if p:
                        self._fire_callbacks(p)
                        if not self._ws_connected:
                            log.debug(f"📡 REST fallback price: ${p:.4f}")
                time.sleep(3)
            except Exception as e:
                log.debug(f"REST fallback error: {e}")
                time.sleep(10)

# ══════════════════════════════════════════════════════════════
#  TECHNICAL ANALYSIS
# ══════════════════════════════════════════════════════════════

class TechnicalAnalysis:
    def __init__(self, cfg: dict):
        self.cfg = cfg

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.cfg
        df["rsi"] = ta.rsi(df["close"], length=c["RSI_PERIOD"])
        macd = ta.macd(df["close"], fast=c["MACD_FAST"], slow=c["MACD_SLOW"], signal=c["MACD_SIGNAL"])
        # Dynamic search for MACD columns
        for col in macd.columns:
            if col.startswith("MACD_") and "_" in col: df["macd"] = macd[col]
            if col.startswith("MACDs_"): df["macd_signal"] = macd[col]
            if col.startswith("MACDh_"): df["macd_hist"] = macd[col]
        
        if "macd" not in df.columns:
            df["macd"] = df["macd_signal"] = df["macd_hist"] = 0.0
        bb = ta.bbands(df["close"], length=c["BB_PERIOD"], std=c["BB_STD"])
        # Dynamic search for BB columns to avoid KeyError
        for col in bb.columns:
            if col.startswith("BBU_"): df["bb_upper"] = bb[col]
            if col.startswith("BBM_"): df["bb_mid"]   = bb[col]
            if col.startswith("BBL_"): df["bb_lower"] = bb[col]
        
        if "bb_upper" in df.columns and "bb_lower" in df.columns:
            df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"] * 100
            df["bb_pct"]   = (df["close"] - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])
        else:
            df["bb_upper"] = df["bb_mid"] = df["bb_lower"] = df["close"]
            df["bb_width"] = df["bb_pct"] = 0.0
        df["ema_fast"]  = ta.ema(df["close"], length=c["EMA_FAST"])
        df["ema_slow"]  = ta.ema(df["close"], length=c["EMA_SLOW"])
        df["ema_trend"] = ta.ema(df["close"], length=c["EMA_TREND"])
        df["ema_long"]  = ta.ema(df["close"], length=c["EMA_LONG"])
        df["atr"]     = ta.atr(df["high"], df["low"], df["close"], length=c["ATR_PERIOD"])
        df["atr_pct"] = df["atr"] / df["close"] * 100
        try:
            adx_result = ta.adx(df["high"], df["low"], df["close"], length=c.get("ADX_PERIOD", 14))
            if adx_result is not None and not adx_result.empty:
                adx_col = f"ADX_{c.get('ADX_PERIOD', 14)}"
                if adx_col in adx_result.columns:
                    df["adx"] = adx_result[adx_col]
                else:
                    for col in adx_result.columns:
                        if col.startswith("ADX_"):
                            df["adx"] = adx_result[col]
                            break
            if "adx" not in df.columns:
                df["adx"] = 25.0
        except Exception:
            df["adx"] = 25.0
        stoch = ta.stoch(df["high"], df["low"], df["close"],
                         k=c["STOCH_K"], d=c["STOCH_D"], smooth_k=c["STOCH_SMOOTH"])
        k_col = f"STOCHk_{c['STOCH_K']}_{c['STOCH_D']}_{c['STOCH_SMOOTH']}"
        d_col = f"STOCHd_{c['STOCH_K']}_{c['STOCH_D']}_{c['STOCH_SMOOTH']}"
        if k_col in stoch.columns:
            df["stoch_k"] = stoch[k_col]
            df["stoch_d"] = stoch[d_col]
        else:
            for col in stoch.columns:
                if "STOCHk" in col: df["stoch_k"] = stoch[col]
                if "STOCHd" in col: df["stoch_d"] = stoch[col]
        df["obv"]     = ta.obv(df["close"], df["volume"])
        df["obv_ema"] = ta.ema(df["obv"], length=21)
        df["vwap"]    = ta.vwap(df["high"], df["low"], df["close"], df["volume"])
        df["vol_ma"]    = df["volume"].rolling(20).mean()
        df["vol_ratio"] = (df["volume"] / df["vol_ma"]).round(2)
        df["body_size"]        = abs(df["close"] - df["open"])
        df["upper_wick"]       = df["high"] - df[["close","open"]].max(axis=1)
        df["lower_wick"]       = df[["close","open"]].min(axis=1) - df["low"]
        df["is_bullish_candle"] = df["close"] > df["open"]

        # 🔥 V3.2: Consecutive candle count untuk trend power
        df["consec_bull"] = 0
        df["consec_bear"] = 0
        bull_count = 0
        bear_count = 0
        for i in range(len(df)):
            if df["is_bullish_candle"].iloc[i]:
                bull_count += 1
                bear_count = 0
            else:
                bear_count += 1
                bull_count = 0
            df.iloc[i, df.columns.get_loc("consec_bull")] = bull_count
            df.iloc[i, df.columns.get_loc("consec_bear")] = bear_count

        return df

    # ─── 🔥 TREND POWER SYSTEM (V3.2) ────────────────────────

    def calc_trend_power(self, df: pd.DataFrame, direction: str) -> dict:
        """
        Hitung kekuatan trend saat ini (0-100).
        Semakin tinggi → trend semakin kuat & reliable.
        
        Komponen:
          - ADX Strength (25 poin max)   → seberapa kuat trending
          - ADX Slope (10 poin max)      → apakah trend menguat
          - EMA Alignment (20 poin max)  → apakah semua EMA terurut
          - MACD Momentum (15 poin max)  → momentum histogram
          - Volume Confirmation (15 poin max) → volume di atas rata-rata
          - Consecutive Candles (15 poin max) → candle berturut searah
        """
        if len(df) < 5:
            return {"trend_power": 0, "tp_details": {}, "adx_rising": False, "overextended": False}

        row  = df.iloc[-1]
        prev = df.iloc[-2]
        prev2 = df.iloc[-3]

        adx  = float(row.get("adx", 0)) if not pd.isna(row.get("adx", 0)) else 0
        adx_prev = float(prev.get("adx", 0)) if not pd.isna(prev.get("adx", 0)) else 0
        adx_prev2 = float(prev2.get("adx", 0)) if not pd.isna(prev2.get("adx", 0)) else 0
        atr  = float(row.get("atr", 0))
        close = float(row["close"])
        ema_trend_val = float(row.get("ema_trend", close))

        tp_details = {}
        score = 0

        # ── 1. ADX Strength (max 25) ──
        if adx >= 50:
            s = 25; lbl = "🔥 Sangat Kuat"
        elif adx >= 40:
            s = 20; lbl = "💪 Kuat"
        elif adx >= 30:
            s = 15; lbl = "📈 Sedang"
        elif adx >= 25:
            s = 10; lbl = "🔸 Lumayan"
        elif adx >= 20:
            s = 5; lbl = "⚪ Lemah"
        else:
            s = 0; lbl = "❌ Sideways"
        score += s
        tp_details["ADX Power"] = f"{adx:.1f} → {lbl} (+{s})"

        # ── 2. ADX Slope (max 10) ──
        adx_rising = adx > adx_prev and adx_prev > adx_prev2
        adx_up     = adx > adx_prev
        if adx_rising:
            s = 10; lbl = "📈 Naik Kuat"
        elif adx_up:
            s = 5; lbl = "↗️ Naik"
        else:
            s = 0; lbl = "↘️ Turun"
        score += s
        tp_details["ADX Slope"] = f"{lbl} (+{s})"

        # ── 3. EMA Alignment (max 20) ──
        ef = float(row.get("ema_fast", 0))
        es = float(row.get("ema_slow", 0))
        et = float(row.get("ema_trend", 0))
        el = float(row.get("ema_long", 0))

        if direction == "LONG":
            checks = [close > ef, ef > es, es > et, et > el]
            aligned = sum(checks)
            # 🚀 V3.2: Bonus jika baru breakout (EMA 20 > 50 sudah cukup kuat)
            if aligned < 4 and ef > es and es > et and close > ef:
                aligned = 3 # Naikkan grade jika 3 EMA utama sudah oke
        else:
            checks = [close < ef, ef < es, es < et, et < el]
            aligned = sum(checks)
            if aligned < 4 and ef < es and es < et and close < ef:
                aligned = 3

        s = [0, 5, 12, 18, 20][aligned]
        score += s
        lbl = ["❌ Chaos", "🔸 1/4", "🟡 2/4", "📈 3/4", "🔥 Perfect"][aligned]
        tp_details["EMA Align"] = f"{lbl} (+{s})"

        # ── 4. MACD Momentum (max 15) ──
        macd_line = float(row.get("macd", 0))
        macd_sig  = float(row.get("macd_signal", 0))
        macd_hist = float(row.get("macd_hist", 0))
        prev_hist = float(prev.get("macd_hist", 0))

        s = 0
        if direction == "LONG":
            if macd_line > macd_sig and macd_hist > prev_hist:
                s = 15; lbl = "🔥 Golden + Ascending"
            elif macd_line > macd_sig:
                s = 8; lbl = "📈 Above signal"
            elif macd_hist > prev_hist:
                s = 4; lbl = "↗️ Hist naik"
            else:
                lbl = "❌ Bearish"
        else:
            if macd_line < macd_sig and macd_hist < prev_hist:
                s = 15; lbl = "🔥 Death + Descending"
            elif macd_line < macd_sig:
                s = 8; lbl = "📉 Below signal"
            elif macd_hist < prev_hist:
                s = 4; lbl = "↘️ Hist turun"
            else:
                lbl = "❌ Bullish"
        score += s
        tp_details["MACD Mom"] = f"{lbl} (+{s})"

        # ── 5. Volume Confirmation (max 15) ──
        vol_ratio = float(row.get("vol_ratio", 1))
        obv_up = row.get("obv", 0) > row.get("obv_ema", 0)
        vol_confirms = (direction == "LONG" and obv_up) or (direction == "SHORT" and not obv_up)

        if vol_ratio >= 2.0 and vol_confirms:
            s = 15; lbl = f"🔥 Spike {vol_ratio:.1f}x + OBV"
        elif vol_ratio >= 1.5 and vol_confirms:
            s = 12; lbl = f"💪 {vol_ratio:.1f}x + OBV"
        elif vol_ratio >= 1.2:
            s = 7; lbl = f"📈 {vol_ratio:.1f}x"
        elif vol_ratio >= 0.8:
            s = 3; lbl = f"⚪ Normal {vol_ratio:.1f}x"
        else:
            s = 0; lbl = f"❌ Kering {vol_ratio:.1f}x"
        score += s
        tp_details["Volume"] = f"{lbl} (+{s})"

        # ── 6. Consecutive Candles (max 15) ──
        if direction == "LONG":
            consec = int(row.get("consec_bull", 0))
        else:
            consec = int(row.get("consec_bear", 0))

        if consec >= 4:
            s = 15; lbl = f"🔥 {consec} candle"
        elif consec >= 3:
            s = 10; lbl = f"💪 {consec} candle"
        elif consec >= 2:
            s = 5; lbl = f"📈 {consec} candle"
        else:
            s = 0; lbl = "❌ Tidak konsisten"
        score += s
        tp_details["Candles"] = f"{lbl} (+{s})"

        # ── Over-Extension Check ──
        # 🚀 V3.4: ext_limit naik seiring Trend Power — tren kuat = boleh lebih jauh dari EMA
        # Ini mencegah circular problem: TP tinggi butuh, tapi TP tinggi = harga jauh = blocked
        is_breakout = (vol_ratio >= 1.8) or (consec >= 3)
        base_limit  = self.cfg.get("MAX_EXTENSION_ATR", 2.0)
        if score >= 80:
            ext_limit = 12.0   # sangat kuat — jangan blok sama sekali
        elif score >= 75:
            ext_limit = 8.0    # kuat sekali
        elif score >= 65:
            ext_limit = 6.0    # kuat — toleransi cukup (fix circular problem)
        elif score >= 50:
            ext_limit = 4.0    # sedang — sedikit toleransi
        elif is_breakout:
            ext_limit = 4.0    # breakout meski belum kuat
        else:
            ext_limit = base_limit  # lemah — pakai config default

        distance_from_ema = abs(close - ema_trend_val)
        ext_ratio = distance_from_ema / atr if atr > 0 else 0
        overextended = ext_ratio > ext_limit

        if overextended:
            tp_details["Extension"] = f"⛔ {ext_ratio:.1f}x ATR (max {ext_limit:.0f}x, TP:{score})"
        else:
            tp_details["Extension"] = f"✅ Ext: {ext_ratio:.1f}x ATR"

        return {
            "trend_power":   min(score, 100),
            "tp_details":    tp_details,
            "adx_rising":    adx_rising or adx_up,
            "overextended":  overextended,
            "ext_ratio":     round(ext_ratio, 2),
            "consec_candles": consec,
        }

    def get_signal(self, df: pd.DataFrame) -> dict:
        c = self.cfg
        if len(df) < 5:
            return {
                "signal": "NEUTRAL", "bull_score": 0, "bear_score": 0,
                "max_score": 14, "confidence": 0, "htf_bias": "—",
                "rsi": 50.0, "macd": 0.0, "atr": 0.0, "atr_pct": 0.0,
                "stoch_k": 50.0, "vol_ratio": 1.0, "ema_fast": 0.0, "ema_slow": 0.0,
                "close": df["close"].iloc[-1] if len(df) > 0 else 0,
                "bb_upper": 0, "bb_lower": 0, "bb_width": 0, "ema_trend": 0,
                "vwap": 0, "stoch_d": 0, "macd_signal": 0, "adx": 0.0,
                "trend_power": 0, "tp_details": {},
                "details": {"Status": "Data tidak cukup"}
            }

        row  = df.iloc[-1]
        prev = df.iloc[-2]

        adx_val = float(row.get("adx", 25.0)) if not pd.isna(row.get("adx", 25.0)) else 25.0
        bull, bear, details = 0, 0, {}
        details["ADX"] = f"✅ ADX {adx_val:.1f} — Trending"

        # [INTERNAL CHECK] ADX Filter (Sideways Protection)
        is_sideways = False
        if c.get("USE_ADX_FILTER", True) and adx_val < c.get("ADX_MIN_THRESHOLD", 20):
            is_sideways = True
            details["ADX"] = f"❌ Sideways ({adx_val:.1f})"

        # RSI
        rsi = row["rsi"]
        prev_rsi = prev["rsi"]
        if rsi < c["RSI_OVERSOLD"] and rsi > prev_rsi:
            bull += 2; details["RSI"] = f"{rsi:.1f} 🟢 Recovery Oversold"
        elif rsi < c["RSI_OVERSOLD"]:
            bull += 1; details["RSI"] = f"{rsi:.1f} 🟡 Oversold Falling"
        elif rsi > c["RSI_OVERBOUGHT"] and rsi < prev_rsi:
            bear += 2; details["RSI"] = f"{rsi:.1f} 🔴 Pullback Overbought"
        elif rsi > c["RSI_OVERBOUGHT"]:
            bear += 1; details["RSI"] = f"{rsi:.1f} 🟠 Overbought Rising"
        elif rsi > 50:
            bull += 1; details["RSI"] = f"{rsi:.1f} 🟡 Bullish"
        else:
            bear += 1; details["RSI"] = f"{rsi:.1f} 🟠 Bearish"

        # MACD
        macd_cross_up   = row["macd"] > row["macd_signal"] and prev["macd"] <= prev["macd_signal"]
        macd_cross_down = row["macd"] < row["macd_signal"] and prev["macd"] >= prev["macd_signal"]
        macd_hist_up    = row["macd_hist"] > prev["macd_hist"]
        if macd_cross_up:
            bull += 2; details["MACD"] = "🟢 Golden cross"
        elif macd_cross_down:
            bear += 2; details["MACD"] = "🔴 Death cross"
        elif row["macd"] > row["macd_signal"] and macd_hist_up:
            bull += 1; details["MACD"] = "🟡 Bullish momentum"
        elif row["macd"] < row["macd_signal"] and not macd_hist_up:
            bear += 1; details["MACD"] = "🟠 Bearish momentum"
        else:
            details["MACD"] = "⚪ Netral"

        # Bollinger Bands
        close  = row["close"]
        bb_pct = row["bb_pct"]
        if close > row["bb_lower"] and prev["close"] <= prev["bb_lower"]:
            bull += 2; details["BB"] = "🟢 Bounce lower band"
        elif close < row["bb_upper"] and prev["close"] >= prev["bb_upper"]:
            bear += 2; details["BB"] = "🔴 Rejection upper band"
        elif bb_pct < 0.2 and row["close"] > prev["close"]:
            bull += 1; details["BB"] = "🟡 Bottoming"
        elif bb_pct > 0.8 and row["close"] < prev["close"]:
            bear += 1; details["BB"] = "🟠 Topping"
        else:
            details["BB"] = f"⚪ Normal ({bb_pct:.0%})"

        # EMA
        ema_bull       = row["ema_fast"] > row["ema_slow"]
        above_trend    = close > row["ema_trend"]
        above_long     = close > row["ema_long"]
        ema_cross_up   = row["ema_fast"] > row["ema_slow"] and prev["ema_fast"] <= prev["ema_slow"]
        ema_cross_down = row["ema_fast"] < row["ema_slow"] and prev["ema_fast"] >= prev["ema_slow"]
        if ema_cross_up:
            bull += 2; details["EMA"] = f"🟢 Golden cross EMA{c['EMA_FAST']}/{c['EMA_SLOW']}"
        elif ema_cross_down:
            bear += 2; details["EMA"] = f"🔴 Death cross EMA{c['EMA_FAST']}/{c['EMA_SLOW']}"
        elif ema_bull and above_trend and above_long:
            bull += 2; details["EMA"] = "🟢 Full bullish"
        elif not ema_bull and not above_trend and not above_long:
            bear += 2; details["EMA"] = "🔴 Full bearish"
        elif ema_bull:
            bull += 1; details["EMA"] = "🟡 Short-term bullish"
        else:
            bear += 1; details["EMA"] = "🟠 Short-term bearish"

        # Stochastic
        sk, sd = row["stoch_k"], row["stoch_d"]
        stoch_cross_up   = sk > sd and prev["stoch_k"] <= prev["stoch_d"]
        stoch_cross_down = sk < sd and prev["stoch_k"] >= prev["stoch_d"]
        if sk < c["STOCH_OVERSOLD"] and stoch_cross_up:
            bull += 2; details["STOCH"] = f"🟢 Oversold crossover K={sk:.0f}"
        elif sk > c["STOCH_OVERBOUGHT"] and stoch_cross_down:
            bear += 2; details["STOCH"] = f"🔴 Overbought crossover K={sk:.0f}"
        elif sk < c["STOCH_OVERSOLD"]:
            bull += 1; details["STOCH"] = f"🟡 Oversold K={sk:.0f}"
        elif sk > c["STOCH_OVERBOUGHT"]:
            bear += 1; details["STOCH"] = f"🟠 Overbought K={sk:.0f}"
        else:
            details["STOCH"] = f"⚪ Netral K={sk:.0f}"

        # OBV
        obv_trend_up = row["obv"] > row["obv_ema"]
        if obv_trend_up and ema_bull:
            bull += 1; details["OBV"] = "🟢 Volume konfirmasi uptrend"
        elif not obv_trend_up and not ema_bull:
            bear += 1; details["OBV"] = "🔴 Volume konfirmasi downtrend"
        else:
            details["OBV"] = "⚪ OBV divergen"

        # VWAP
        if not pd.isna(row["vwap"]):
            if close > row["vwap"]:
                bull += 1; details["VWAP"] = f"🟢 Above VWAP"
            else:
                bear += 1; details["VWAP"] = f"🔴 Below VWAP"

        # Volume Spike
        vol_ratio = row["vol_ratio"]
        if vol_ratio > 1.8:
            if ema_bull:
                bull += 1; details["VOL"] = f"🟢 Volume spike {vol_ratio:.1f}x + uptrend"
            else:
                bear += 1; details["VOL"] = f"🔴 Volume spike {vol_ratio:.1f}x + downtrend"

        # Candle Body
        body_ratio = row["body_size"] / (row["high"] - row["low"] + 1e-9)
        if body_ratio > 0.6:
            if row["is_bullish_candle"]:
                bull += 1; details["CANDLE"] = f"🟢 Strong bullish ({body_ratio:.0%})"
            else:
                bear += 1; details["CANDLE"] = f"🔴 Strong bearish ({body_ratio:.0%})"

        dominant = "LONG" if bull >= bear else "SHORT"
        tp_data = self.calc_trend_power(df, dominant)

        # 🔥 V3.3: Smart Override — Kurangi syarat skor sesuai kekuatan trend
        req_bull = c["MIN_BULL_SCORE"]
        req_bear = c["MIN_BEAR_SCORE"]
        tp_score = tp_data.get("trend_power", 0)
        if tp_score >= 70 and tp_data.get("adx_rising", False):
            # Trend sangat kuat: cukup 5 skor
            req_bull = min(req_bull, 5)
            req_bear = min(req_bear, 5)
        elif tp_score >= 55:
            # Trend moderat-kuat: turunkan ke EARLY_ENTRY_SCORE
            early_s  = c.get("EARLY_ENTRY_SCORE", 6)
            req_bull = min(req_bull, early_s)
            req_bear = min(req_bear, early_s)

        if is_sideways:
            signal = "NEUTRAL"
        elif bull >= req_bull and bull > bear:
            signal = "LONG"
        elif bear >= req_bear and bear > bull:
            signal = "SHORT"
        else:
            signal = "NEUTRAL"
            
        return {
            "signal":      signal,
            "is_sideways": is_sideways,
            "bull_score":  bull,
            "bear_score":  bear,
            "max_score":   14,
            "confidence":  round(max(bull, bear) / 14 * 100),
            "rsi":         rsi,
            "atr":         float(row.get("atr", 0)),
            "atr_pct":     float(row.get("atr_pct", 0)),
            "close":       close,
            "bb_upper":    float(row.get("bb_upper", 0)),
            "bb_lower":    float(row.get("bb_lower", 0)),
            "bb_width":    float(row.get("bb_width", 0)),
            "ema_fast":    float(row.get("ema_fast", 0)),
            "ema_slow":    float(row.get("ema_slow", 0)),
            "ema_trend":   float(row.get("ema_trend", 0)),
            "vwap":        float(row.get("vwap", 0)) if not pd.isna(row.get("vwap", 0)) else 0,
            "stoch_k":     sk,
            "stoch_d":     sd,
            "vol_ratio":   vol_ratio,
            "macd":        float(row.get("macd", 0)),
            "macd_signal": float(row.get("macd_signal", 0)),
            "adx":         adx_val,
            "trend_power":  tp_data["trend_power"],
            "tp_details":   tp_data["tp_details"],
            "adx_rising":   tp_data["adx_rising"],
            "overextended": tp_data["overextended"],
            "ext_ratio":    tp_data.get("ext_ratio", 0),
            "details":     details,
        }

    def get_htf_bias(self, df_htf: pd.DataFrame) -> str:
        try:
            df_htf = self.compute(df_htf.copy())
            df_htf.dropna(inplace=True)
            row      = df_htf.iloc[-1]
            ema_bull = row["ema_fast"] > row["ema_slow"]
            above_t  = row["close"] > row["ema_trend"]
            rsi_bull = row["rsi"] < 60
            macd_b   = row["macd"] > row["macd_signal"]
            score    = sum([ema_bull, above_t, rsi_bull, macd_b])
            if score >= 3: return "BULLISH"
            elif score <= 1: return "BEARISH"
            return "NEUTRAL"
        except Exception:
            return "NEUTRAL"

# ══════════════════════════════════════════════════════════════
#  ✨ COIN SCANNER (NEW V3)
# ══════════════════════════════════════════════════════════════

class CoinScanner:
    """
    Scan top N koin MEXC Futures, ranking berdasarkan:
    - Signal strength (bull/bear score)
    - Signal momentum (apakah skor sedang naik?)
    - Volatilitas (ATR%)
    - Volume konfirmasi
    """
    def __init__(self, client: MEXCFuturesClient, ta_engine: TechnicalAnalysis, cfg: dict):
        self.client    = client
        self.ta        = ta_engine
        self.cfg       = cfg
        self._history: Dict[str, deque] = {}   # symbol → deque skor terakhir
        self._results:  List[dict] = []
        self._lock      = threading.Lock()
        self._last_scan = 0.0
        self._scanning  = False

    def _composite_score(self, signal: dict, sym: str) -> float:
        """Hitung skor gabungan untuk ranking koin."""
        raw_score   = max(signal["bull_score"], signal["bear_score"])
        atr_pct     = signal.get("atr_pct", 0)
        vol_ratio   = signal.get("vol_ratio", 1)
        adx         = signal.get("adx", 20)
        tp          = signal.get("trend_power", 0)

        # Momentum: apakah skor sedang naik?
        hist = list(self._history.get(sym, deque()))
        if len(hist) >= 2:
            momentum = hist[-1] - hist[0]
        else:
            momentum = 0

        # 🔥 V3.2: ATR adalah bonus jka trend kuat, tapi jadi bahaya jika trend melemah
        atr_weight = 40 if signal.get("adx_rising") else 5

        composite = (
            raw_score  * 1.0 +          # kekuatan sinyal saat ini
            momentum   * 2.5 +          # koin yang sedang menanjak lebih diutamakan
            tp         * 2.0 +          # 🔥 V3.2: Trend Power sangat diutamakan
            atr_pct    * atr_weight +   # Volatilitas hanya dihargai jika trend sehat
            (vol_ratio - 1) * 1.5 +     # volume di atas rata-rata = ada momentum
            (adx - 20) * 0.1 +          # trending = lebih aman
            (20 if signal.get("adx_rising") else -20) # Pokok utama: MENGUAT vs MELEMAH
        )
        return round(composite, 3)

    def scan_once(self) -> List[dict]:
        """Jalankan satu putaran scan. Dipanggil dari thread terpisah."""
        if self._scanning:
            return self._results
        self._scanning = True

        c          = self.cfg
        blacklist  = set(c.get("BLACKLIST_COINS", []))
        min_vol    = c.get("SCAN_MIN_VOLUME", 500_000)
        min_adx    = c.get("SCAN_MIN_ADX", 22)
        min_atr    = c.get("SCAN_MIN_ATR_PCT", 0.3)
        top_coins  = self.client.get_top_volume_coins(c.get("SCAN_TOP_N", 30))

        results = []
        scanned = 0

        for coin in top_coins:
            sym = coin["symbol"]
            if sym in blacklist:
                continue
            if coin.get("turnover", 0) < min_vol:
                continue

            try:
                # Must fetch enough for EMA_LONG (200) warmup + buffer
                df = self.client.get_klines(sym, c["PRIMARY_TF"], c.get("CANDLE_LIMIT", 300))
                if df is None or len(df) < 50:
                    continue
                df = self.ta.compute(df)
                df.dropna(inplace=True)
                if len(df) < 5:
                    continue

                signal = self.ta.get_signal(df)
                adx    = signal.get("adx", 0)
                atr_p  = signal.get("atr_pct", 0)

                # Filter koin sideways / terlalu sepi (relaxed for user visibility)
                if sym != self.cfg.get("SYMBOL"): # Selalu izinkan koin aktif masuk scanner
                    if adx < min_adx or atr_p < min_atr:
                        continue

                raw = max(signal["bull_score"], signal["bear_score"])

                # Update history
                if sym not in self._history:
                    self._history[sym] = deque(maxlen=6)
                self._history[sym].append(raw)

                composite = self._composite_score(signal, sym)
                hist      = list(self._history[sym])
                momentum  = (hist[-1] - hist[0]) if len(hist) >= 2 else 0

                results.append({
                    "symbol":     sym,
                    "signal":     signal["signal"],
                    "bull_score": signal["bull_score"],
                    "bear_score": signal["bear_score"],
                    "raw_score":  raw,
                    "momentum":   round(momentum, 2),
                    "composite":  composite,
                    "atr_pct":    round(atr_p, 3),
                    "vol_ratio":  round(signal.get("vol_ratio", 1), 2),
                    "adx":        round(adx, 1),
                    "price":      round(signal.get("close", 0), 6),
                    "turnover":   coin.get("turnover", 0),
                    "trend_power": signal.get("trend_power", 0),
                })
                scanned += 1

            except Exception as e:
                log.warning(f"[Scanner] Error scan {sym}: {e}")
                continue

        # Urutkan: composite tertinggi dulu
        results.sort(key=lambda x: x["composite"], reverse=True)

        with self._lock:
            self._results  = results
            self._last_scan = time.time()

        if scanned > 0:
            msg = f"[Scanner] Berhasil: {len(results)}/{scanned} koin lolos filter."
            if results: msg += f" Top: {results[0]['symbol']} ({results[0]['composite']:.1f})"
            log.info(msg)
        else:
            log.info(f"[Scanner] Scan selesai, 0 koin dianalisa (cek koneksi API).")

        self._scanning = False
        return results

    def best_coin(self) -> Optional[dict]:
        """Koin terbaik dengan sinyal actionable saat ini."""
        with self._lock:
            results = list(self._results)
        actionable = [r for r in results if r["signal"] in ("LONG", "SHORT")]
        return actionable[0] if actionable else None

    def early_entry_candidate(self) -> Optional[dict]:
        """
        Deteksi sinyal yang sedang TUMBUH sebelum mencapai threshold penuh.
        Kriteria: skor >= EARLY_ENTRY_SCORE DAN momentum naik >= EARLY_MOMENTUM_MIN.
        """
        with self._lock:
            results = list(self._results)
        c        = self.cfg
        min_s    = c.get("EARLY_ENTRY_SCORE", 6)
        min_mom  = c.get("EARLY_MOMENTUM_MIN", 2)

        for r in results:
            score    = r["raw_score"]
            momentum = r["momentum"]
            # Tentukan arah dari mana skornya berasal
            bull_dominant = r["bull_score"] >= r["bear_score"]
            direction     = "LONG" if bull_dominant and r["bull_score"] >= min_s else \
                            "SHORT" if not bull_dominant and r["bear_score"] >= min_s else None
            if direction and momentum >= min_mom:
                return {**r, "signal": direction, "is_early_entry": True}
        return None

    def get_results(self) -> List[dict]:
        with self._lock:
            return list(self._results)

    def needs_scan(self) -> bool:
        return (time.time() - self._last_scan) >= self.cfg.get("SCAN_INTERVAL", 120)

# ══════════════════════════════════════════════════════════════
#  RISK MANAGER
# ══════════════════════════════════════════════════════════════

class RiskManager:
    def __init__(self, cfg: dict):
        self.cfg = cfg

    def calculate_levels(self, side: str, entry: float, atr: float, trend_power: int = 50) -> dict:
        """
        🔥 V3.2: Dynamic SL/TP berdasarkan kekuatan trend.
        
        Logika:
        - Trend kuat (power 80-100) → SL ketat (trend reliable), TP lebar (trend lanjut)
        - Trend sedang (power 55-80) → SL normal, TP normal
        - Trend lemah (<55)          → SL lebar (proteksi), TP tipis
        
        Gambaran:
          TP = [base] × (1 + power_bonus)
          SL = [base] × (1 - power_discount)
        """
        c = self.cfg

        if c.get("DYNAMIC_SL_TP", True) and trend_power > 0:
            # Normalize 0-1 range (clamp 40-100 → 0-1)
            power_norm = max(0, min((trend_power - 40) / 60.0, 1.0))

            # SL: semakin kuat trend → semakin ketat (0.7x - 1.0x base)
            sl_factor = 1.0 - (power_norm * 0.3)   # power 100 → 0.7x, power 40 → 1.0x
            # TP: semakin kuat trend → semakin lebar (1.0x - 2.0x base)
            tp_factor = 1.0 + (power_norm * 1.0)   # power 100 → 2.0x, power 40 → 1.0x

            sl  = atr * c["ATR_SL_MULT"] * sl_factor
            tp1 = atr * c["ATR_TP1_MULT"] * tp_factor
            tp2 = atr * c["ATR_TP2_MULT"] * tp_factor
            tp3 = atr * c["ATR_TP3_MULT"] * tp_factor

            log.info(f"[DYNAMIC SL/TP] Power={trend_power} → SL×{sl_factor:.2f} TP×{tp_factor:.2f}")
        else:
            sl  = atr * c["ATR_SL_MULT"]
            tp1 = atr * c["ATR_TP1_MULT"]
            tp2 = atr * c["ATR_TP2_MULT"]
            tp3 = atr * c["ATR_TP3_MULT"]

        if side == "LONG":
            return {
                "stop_loss": round(entry - sl, 6), "take_profit1": round(entry + tp1, 6),
                "take_profit2": round(entry + tp2, 6), "take_profit3": round(entry + tp3, 6),
                "sl_distance": round(sl, 6), "rr_ratio": round(tp1 / sl, 2), "entry": entry
            }
        else:
            return {
                "stop_loss": round(entry + sl, 6), "take_profit1": round(entry - tp1, 6),
                "take_profit2": round(entry - tp2, 6), "take_profit3": round(entry - tp3, 6),
                "sl_distance": round(sl, 6), "rr_ratio": round(tp1 / sl, 2), "entry": entry
            }

    def position_size(self, balance: float, sl_distance: float, entry_price: float,
                      win_rate: float = 0.5) -> float:
        c = self.cfg
        if c["USE_KELLY"] and win_rate > 0:
            rr    = c["ATR_TP1_MULT"] / c["ATR_SL_MULT"]
            kelly = win_rate - (1 - win_rate) / rr
            risk_pct = min(max(kelly * 0.5, 0.005), 0.04)
        else:
            risk_pct = c["RISK_PER_TRADE"]
        risk_amount = balance * risk_pct
        risk_qty    = risk_amount / sl_distance if sl_distance > 0 else 0.0
        max_margin  = balance * c["MAX_MARGIN_PCT"]
        max_qty     = (max_margin * c["LEVERAGE"]) / entry_price if entry_price > 0 else 0.0
        return round(min(risk_qty, max_qty), 6)

    def check_rr(self, levels: dict) -> bool:
        return levels["rr_ratio"] >= self.cfg["MIN_RR_RATIO"]

    def check_tp_probability(self, levels: dict, signal: dict) -> Tuple[bool, str]:
        c = self.cfg
        if not c.get("USE_PROBABILITY_FILTER"):
            return True, ""
        entry = levels["entry"]
        tp1   = levels["take_profit1"]
        atr   = signal["atr"]
        vol   = signal["vol_ratio"]
        if vol < c["MIN_VOL_RATIO"]:
            return False, f"Volume rendah ({vol:.1f}x)"
        dist_to_tp = abs(tp1 - entry)
        atr_mult   = dist_to_tp / atr if atr > 0 else 99
        if atr_mult > c["MAX_ATR_DISTANCE_MULT"]:
            return False, f"TP terlalu jauh ({atr_mult:.1f}x ATR)"
        dist_pct = (dist_to_tp / entry) * 100
        if dist_pct < c["MIN_TP_DISTANCE_PCT"]:
            return False, f"TP terlalu tipis ({dist_pct:.2f}%)"
        return True, "OK"

    def update_trailing_stop(self, pos: Position, current_price: float) -> Optional[float]:
        c = self.cfg
        if not c["USE_TRAILING_STOP"]:
            return None
        if pos.side == "LONG":
            if not pos.trailing_active:
                profit_pct = (current_price - pos.entry_price) / pos.entry_price
                if profit_pct >= c["TRAIL_ACTIVATION_PCT"]:
                    pos.trailing_active = True
                    pos.highest_price   = current_price
                    trail_sl = current_price * (1 - c["TRAIL_DISTANCE_PCT"])
                    pos.trailing_stop = max(trail_sl, pos.stop_loss)
                    return pos.trailing_stop
            if pos.trailing_active and current_price > pos.highest_price:
                pos.highest_price = current_price
                new_sl = current_price * (1 - c["TRAIL_DISTANCE_PCT"])
                if new_sl > pos.trailing_stop:
                    pos.trailing_stop = new_sl
                    return new_sl
        else:
            if not pos.trailing_active:
                profit_pct = (pos.entry_price - current_price) / pos.entry_price
                if profit_pct >= c["TRAIL_ACTIVATION_PCT"]:
                    pos.trailing_active = True
                    pos.lowest_price    = current_price
                    trail_sl = current_price * (1 + c["TRAIL_DISTANCE_PCT"])
                    pos.trailing_stop = min(trail_sl, pos.stop_loss)
                    return pos.trailing_stop
            if pos.trailing_active and current_price < pos.lowest_price:
                pos.lowest_price = current_price
                new_sl = current_price * (1 + c["TRAIL_DISTANCE_PCT"])
                if new_sl < pos.trailing_stop:
                    pos.trailing_stop = new_sl
                    return new_sl
        return None

# ══════════════════════════════════════════════════════════════
#  SESSION & NEWS FILTER
# ══════════════════════════════════════════════════════════════

class SessionFilter:
    def __init__(self, cfg: dict):
        self.cfg = cfg

    def is_trading_allowed(self) -> Tuple[bool, str]:
        now     = datetime.now(WIB)
        c       = self.cfg
        if not c["USE_SESSION_FILTER"]:
            return True, "Filter dinonaktifkan"
        hour    = now.hour
        weekday = now.weekday()
        if weekday == 5:
            return False, "Weekend — Sabtu"
        if weekday == 6 and c["BLOCK_SUNDAY_OPEN"] and hour < 7:
            return False, "Weekend — Minggu pagi"
        if weekday == 4 and c["BLOCK_FRIDAY_CLOSE"] and hour >= 20:
            return False, "Jumat malam"
        if hour not in c["ALLOWED_HOURS_UTC"]:
            return False, f"Di luar jam trading ({hour:02d}:xx)"
        for news_str in c["NEWS_BLACKOUT"]:
            try:
                news_dt = datetime.strptime(f"{now.year}-{news_str}", "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
                if abs((now - news_dt).total_seconds()) <= 1800:
                    return False, f"News blackout: {news_str}"
            except Exception:
                pass
        if 13 <= hour < 16: session = "London/NY Overlap"
        elif 7 <= hour < 13: session = "London session"
        elif 16 <= hour < 22: session = "New York session"
        else: session = "Asia session"
        return True, session

# ══════════════════════════════════════════════════════════════
#  TRADE JOURNAL
# ══════════════════════════════════════════════════════════════

class TradeJournal:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self._ensure_header()

    def _ensure_header(self):
        if not os.path.exists(self.filepath):
            with open(self.filepath, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow([
                    "trade_id","symbol","side","entry_price","exit_price",
                    "quantity","pnl","sl","tp1","tp2",
                    "opened_at","closed_at","close_reason",
                    "bull_score","bear_score","confidence",
                    "trailing_activated","rsi_at_entry","atr_at_entry",
                    "is_early_entry","flip_zone",
                ])

    def log_trade(self, pos: Position, exit_price: float, signal: dict):
        with open(self.filepath, "a", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                pos.id, pos.symbol, pos.side,
                pos.entry_price, exit_price, pos.quantity,
                round(pos.pnl, 4),
                pos.stop_loss, pos.take_profit1, pos.take_profit2,
                pos.opened_at, pos.closed_at, pos.close_reason,
                signal.get("bull_score",""), signal.get("bear_score",""),
                signal.get("confidence",""),
                pos.trailing_active,
                round(signal.get("rsi", 0), 2),
                round(signal.get("atr", 0), 6),
                signal.get("is_early_entry", False),
                signal.get("flip_zone", ""),
            ])

    def get_trades(self, date_filter: str = None) -> list:
        if not os.path.exists(self.filepath):
            return []
        trades = []
        try:
            with open(self.filepath, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if date_filter:
                        if row["closed_at"].startswith(date_filter):
                            trades.append(row)
                    else:
                        trades.append(row)
            return trades[::-1]
        except Exception as e:
            log.error(f"Error baca journal: {e}")
            return []

# ══════════════════════════════════════════════════════════════
#  STATE PERSISTENCE
# ══════════════════════════════════════════════════════════════

class StatePersistence:
    def __init__(self, filepath: str):
        self.filepath = filepath

    def save(self, state: BotState):
        try:
            data = {
                "balance": state.balance, "peak_balance": state.peak_balance,
                "total_trades": state.total_trades, "winning_trades": state.winning_trades,
                "total_pnl": state.total_pnl, "daily_pnl": state.daily_pnl,
                "daily_reset_date": state.daily_reset_date,
                "circuit_breaker": state.circuit_breaker, "circuit_reason": state.circuit_reason,
                "circuit_type": state.circuit_type, "started_at": state.started_at,
                "iteration": state.iteration, "secured_total": state.secured_total,
                "active_symbol": state.active_symbol,
                "positions": [asdict(p) for p in state.positions if not p.closed],
            }
            with open(self.filepath, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            log.error(f"Gagal save state: {e}")

    def load(self) -> Optional[dict]:
        if not os.path.exists(self.filepath):
            return None
        try:
            with open(self.filepath) as f:
                return json.load(f)
        except Exception as e:
            log.error(f"Gagal load state: {e}")
            return None

# ══════════════════════════════════════════════════════════════
#  TELEGRAM NOTIFIER
# ══════════════════════════════════════════════════════════════

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        self._queue  = deque()
        if self.enabled:
            threading.Thread(target=self._worker, daemon=True, name="Telegram").start()

    def send(self, message: str):
        if self.enabled:
            self._queue.append(message)

    def _worker(self):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        while True:
            if self._queue:
                msg = self._queue.popleft()
                try:
                    requests.post(url, json={"chat_id": self.chat_id, "text": msg, "parse_mode": "Markdown"}, timeout=8)
                    time.sleep(0.5)
                except Exception as e:
                    log.warning(f"Telegram error: {e}")
            else:
                time.sleep(1)

# ══════════════════════════════════════════════════════════════
#  BOT UTAMA V3
# ══════════════════════════════════════════════════════════════

class TradingBotV3:

    # ── Config Management ─────────────────────────────────────

    def load_config(self) -> dict:
        path = "config_v3.json"
        if os.path.exists(path):
            try:
                with open(path) as f:
                    file_cfg = json.load(f)
                full_cfg = CONFIG.copy()
                full_cfg.update(file_cfg)
                log.info("Config dimuat dari config_v3.json")
                return full_cfg
            except Exception as e:
                log.error(f"Gagal baca config: {e}")
        self.save_config(CONFIG)
        return CONFIG.copy()

    def save_config(self, cfg: dict):
        try:
            with open("config_v3.json", "w") as f:
                json.dump(cfg, f, indent=4)
        except Exception as e:
            log.error(f"Gagal save config: {e}")

    def update_config_live(self, new_cfg: dict):
        was_dry_run = self.cfg.get("DRY_RUN", True)
        old_symbol = self.cfg.get("SYMBOL")
        self.cfg.update(new_cfg)
        self.save_config(self.cfg)
        
        # Perbarui kredensial client jika berubah
        if "MEXC_API_KEY" in new_cfg or "MEXC_API_SECRET" in new_cfg:
            self.client.api_key    = self.cfg.get("MEXC_API_KEY", "")
            self.client.api_secret = self.cfg.get("MEXC_API_SECRET", "")
            self.spot_client.api_key    = self.cfg.get("MEXC_API_KEY", "")
            self.spot_client.api_secret = self.cfg.get("MEXC_API_SECRET", "")

        self.dry_run = self.cfg.get("DRY_RUN", True)
        
        # Sinkronisasi balance saat berada di atau beralih ke LIVE MODE
        if not self.dry_run:
            log.info("🚀 LIVE MODE Aktif: Menyamakan saldo dengan dompet MEXC...")
            self.sync_balance_from_exchange()

        if "SYMBOL" in new_cfg and new_cfg["SYMBOL"] != old_symbol:
            log.info(f"Ganti pair: {old_symbol} → {new_cfg['SYMBOL']}")
            self.price_feed.resubscribe(new_cfg["SYMBOL"])
            
            new_price = self.price_feed.client.get_ticker(new_cfg["SYMBOL"])
            if new_price:
                with self.price_feed._lock:
                    self.price_feed.price = new_price
                    
            self.state.active_symbol = new_cfg["SYMBOL"]
            self._last_signal.clear()
        if hasattr(self, "ta"):    self.ta.cfg    = self.cfg
        if hasattr(self, "risk"):  self.risk.cfg  = self.cfg
        if hasattr(self, "scanner"): self.scanner.cfg = self.cfg
        log.info("🔥 Hot-reload konfigurasi selesai")

    # ── Init ──────────────────────────────────────────────────

    def __init__(self, run_dashboard: bool = False):
        self.cfg          = self.load_config()
        self.client       = MEXCFuturesClient(MEXC_API_KEY, MEXC_API_SECRET)
        self.ta           = TechnicalAnalysis(self.cfg)
        self.risk         = RiskManager(self.cfg)
        self.session      = SessionFilter(self.cfg)
        self.notifier     = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT)
        self.journal      = TradeJournal(self.cfg["JOURNAL_FILE"])
        self.persistence  = StatePersistence(self.cfg["STATE_FILE"])
        self.price_feed   = PriceFeed(self.cfg["SYMBOL"])
        self.spot_client  = MEXCSpotClient(MEXC_API_KEY, MEXC_API_SECRET)
        self.state        = BotState()
        self.dry_run      = self.cfg["DRY_RUN"]
        self.run_dashboard = run_dashboard

        # 🔥 V3.3: Sinkron API Key dari config_v3.json ke client saat startup
        cfg_key    = self.cfg.get("MEXC_API_KEY", "")
        cfg_secret = self.cfg.get("MEXC_API_SECRET", "")
        if cfg_key and cfg_secret:
            self.client.api_key      = cfg_key
            self.client.api_secret   = cfg_secret
            self.spot_client.api_key    = cfg_key
            self.spot_client.api_secret = cfg_secret
            log.info("🔑 API Key dimuat dari config_v3.json")

        self._last_signal  = {}
        self._price_history = deque(maxlen=500)
        self._trade_id_counter = 0
        self._last_loss_time: Dict[str, float] = {}   # 🛡️ V3.1: per-coin cooldown tracker
        self._last_neutral_since: float = 0.0          # 🛡️ V3.1: kapan mulai stuck NEUTRAL
        self._waf_block_until: float = 0.0             # ⏳ cooldown setelah WAF block order

        # ✨ V3: CoinScanner
        self.scanner = CoinScanner(self.client, self.ta, self.cfg)
        self._scanner_thread: Optional[threading.Thread] = None

        self._init_state()
        self.price_feed.add_callback(self._on_price_update)
        self.price_feed.start()
        time.sleep(2)

    def _init_state(self):
        saved = self.persistence.load()
        if saved:
            log.info("Melanjutkan state sebelumnya...")
            self.state.balance          = saved.get("balance", 0)
            self.state.peak_balance     = saved.get("peak_balance", 0)
            self.state.total_trades     = saved.get("total_trades", 0)
            self.state.winning_trades   = saved.get("winning_trades", 0)
            self.state.total_pnl        = saved.get("total_pnl", 0)
            self.state.daily_pnl        = saved.get("daily_pnl", 0)
            self.state.daily_reset_date = saved.get("daily_reset_date", "")
            self.state.circuit_breaker  = saved.get("circuit_breaker", False)
            self.state.circuit_reason   = saved.get("circuit_reason", "")
            self.state.circuit_type     = saved.get("circuit_type", "")
            self.state.started_at       = saved.get("started_at", datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"))
            self.state.iteration        = saved.get("iteration", 0)
            self.state.secured_total    = saved.get("secured_total", 0)
            self.state.active_symbol    = saved.get("active_symbol", self.cfg["SYMBOL"])
            # Restore posisi
            for p_data in saved.get("positions", []):
                try:
                    if "symbol" not in p_data:
                        p_data["symbol"] = self.cfg["SYMBOL"]
                    if "flip_count" not in p_data:
                        p_data["flip_count"] = 0
                    if "opened_ts" not in p_data:
                        p_data["opened_ts"] = 0.0
                    pos = Position(**p_data)
                    if not pos.closed:
                        self.state.positions.append(pos)
                except Exception as e:
                    log.warning(f"Skip restore posisi: {e}")
            
            # 🔥 V3.2: Jika restart dalam MODE LIVE, paksa sinkron saldo dari bursa
            if not self.dry_run:
                log.info("🚀 Restart di MODE LIVE: Sinkronisasi saldo real-time...")
                self.sync_balance_from_exchange()
        else:
            self.state.balance       = (self.client.get_balance() or self.cfg["VIRTUAL_BALANCE"]) \
                                        if not self.cfg["DRY_RUN"] else self.cfg["VIRTUAL_BALANCE"]
            self.state.peak_balance  = self.state.balance
            self.state.started_at    = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
            self.state.active_symbol = self.cfg["SYMBOL"]
            log.info(f"State baru — Saldo: ${self.state.balance:,.2f}")

    def _gen_trade_id(self) -> str:
        self._trade_id_counter += 1
        return f"T{datetime.now(WIB).strftime('%Y%m%d%H%M%S')}-{self._trade_id_counter:04d}"

    def sync_balance_from_exchange(self):
        bal = self.client.get_balance()
        if bal is not None:
            self.state.balance = bal
            self.state.peak_balance = max(self.state.peak_balance, bal)
            log.info(f"💰 Saldo berhasil disinkronkan: ${bal:,.2f}")
            self.state.api_error = ""
        else:
            self.state.api_error = "API Error: Gagal konek/Unauthorized (Cek API Key & Secret)"
            log.warning("❌ Gagal mendapatkan saldo dari MEXC. Periksa API Keys Anda!")

    # ── Price Update Callback ──────────────────────────────────

    def _on_price_update(self, price: float):
        self._price_history.append({"price": price, "ts": time.time()})
        now = time.time()
        grace_sec = self.cfg.get("GRACE_PERIOD_SEC", 30)

        for pos in self.state.positions:
            if pos.closed:
                continue

            # 🛡️ V3.1: Grace period — tidak aktifkan trailing/BE dalam N detik pertama
            age_sec = now - pos.opened_ts if pos.opened_ts > 0 else 9999
            in_grace = age_sec < grace_sec

            if not in_grace:
                new_sl = self.risk.update_trailing_stop(pos, price)
                if new_sl:
                    log.debug(f"[TRAIL] {pos.id} SL → ${new_sl:.4f}")

                # Break-Even
                if self.cfg.get("USE_BE_FILTER") and not pos.be_hit and not pos.tp1_hit:
                    profit_pct = (price - pos.entry_price) / pos.entry_price if pos.side == "LONG" \
                                 else (pos.entry_price - price) / pos.entry_price
                    if profit_pct >= self.cfg.get("BE_ACTIVATION_PCT", 0.005):
                        pos.be_hit    = True
                        buffer = self.cfg.get("BE_FEE_BUFFER_PCT", 0.0015)
                        if pos.side == "LONG":
                            pos.stop_loss = pos.entry_price * (1 + buffer)
                        else:
                            pos.stop_loss = pos.entry_price * (1 - buffer)
                        log.info(f"[BE] {pos.id} SL pindah ke entry+fee ${pos.stop_loss:.4f}")
                        self.notifier.send(f"🛡️ *Break-Even Aktif*\n`{pos.id}` SL ke Entry + Fee.")

            # SL real-time (selalu aktif, ini proteksi utama)
            sl = pos.trailing_stop if pos.trailing_active else pos.stop_loss
            if pos.side == "LONG" and price <= sl:
                self._close_position(pos, price, "Trailing SL" if pos.trailing_active else "Stop Loss")
            elif pos.side == "SHORT" and price >= sl:
                self._close_position(pos, price, "Trailing SL" if pos.trailing_active else "Stop Loss")

    # ── Daily Reset & Circuit Breaker ─────────────────────────

    def _check_daily_reset(self):
        today = datetime.now(WIB).strftime("%Y-%m-%d")
        if self.state.daily_reset_date != today:
            self.state.daily_pnl           = 0.0
            self.state.daily_start_balance  = self.state.balance
            self.state.daily_reset_date     = today

    def _check_circuit_breaker(self) -> bool:
        if self.state.circuit_breaker:
            return True
        c     = self.cfg
        basis = self.state.daily_start_balance if self.state.daily_start_balance > 0 \
                else (self.state.balance + abs(self.state.daily_pnl))
        daily_loss_pct = abs(self.state.daily_pnl) / max(basis, 1)
        if self.state.daily_pnl < 0 and daily_loss_pct >= c["MAX_DAILY_LOSS_PCT"]:
            self._trigger_circuit("AUTO", f"Daily loss {c['MAX_DAILY_LOSS_PCT']*100:.0f}% tercapai")
            return True
        dd = self.state.drawdown()
        if dd >= c["MAX_DRAWDOWN_PCT"] * 100:
            self._trigger_circuit("AUTO", f"Max drawdown {dd:.1f}% tercapai")
            return True
        return False

    def _trigger_circuit(self, ctype: str, reason: str):
        self.state.circuit_breaker       = True
        self.state.circuit_triggered_at  = time.time()
        self.state.circuit_type          = ctype
        self.state.circuit_reason        = reason
        log.warning(f"⛔ CIRCUIT BREAKER: {reason}")
        self.close_all_positions(reason=f"Circuit Breaker ({reason})")
        self.notifier.send(f"⛔ *Circuit Breaker*\n{reason}")

    # ── Multi-Coin Scanner Logic ───────────────────────────────

    def _trigger_scan(self):
        """Jalankan scanner di background thread."""
        if self._scanner_thread and self._scanner_thread.is_alive():
            return
        self._scanner_thread = threading.Thread(
            target=self.scanner.scan_once, daemon=True, name="CoinScanner"
        )
        self._scanner_thread.start()

    def _maybe_switch_coin(self) -> bool:
        """
        Evaluasi apakah perlu switch ke koin lain.
        Return True jika switch dilakukan.
        🛡️ V3.1: Improved — jika stuck NEUTRAL terlalu lama, paksa switch ke top koin.
        """
        if not self.cfg.get("AUTO_SWITCH_COIN", True):
            return False
        if self.open_positions_count() > 0:
            return False  # Jangan switch kalau masih ada posisi terbuka

        current   = self.cfg.get("SYMBOL")
        advantage = self.cfg.get("SWITCH_MIN_ADVANTAGE", 3.0)

        # Cari skor koin saat ini di hasil scanner
        current_results = {r["symbol"]: r for r in self.scanner.get_results()}
        current_data    = current_results.get(current, {})
        current_score   = current_data.get("composite", 0)
        current_signal  = current_data.get("signal", "NEUTRAL")

        # Jika koin yang sedang aktif bersinyal NEUTRAL atau ADX melemah,
        # anggap skornya sangat rendah (0 atau negatif) agar lebih mudah disingkirkan.
        if current_signal == "NEUTRAL":
            current_score = 0
        
        # 🔥 V3.2: Jika ADX koin aktif melemah, beri penalti agar cepat switch
        if not current_data.get("adx_rising", True):
            current_score -= 15
            log.info(f"[SWITCH] Koin aktif {current} melemah (ADX Falling), mempermudah switch...")

        # 🛡️ V3.1: Track berapa lama stuck di NEUTRAL
        if current_signal != "NEUTRAL":
            self._last_neutral_since = 0.0
        elif self._last_neutral_since == 0.0:
            self._last_neutral_since = time.time()

        # 1) Coba switch ke koin dengan sinyal actionable (LONG/SHORT)
        best = self.scanner.best_coin()
        if best:
            candidate = best["symbol"]
            candidate_score = best["composite"]

            if candidate != current and (candidate_score - current_score) >= advantage:
                return self._do_switch(current, candidate, current_score, candidate_score, best)

        # 2) 🔥 V3.3: Langsung pindah ke koin top composite jika jauh lebih unggul
        #    (tidak perlu menunggu 300s idle — Sniper harus selalu di posisi terbaik)
        all_results = self.scanner.get_results()
        if all_results:
            top = all_results[0]
            if top["symbol"] != current and top["composite"] > current_score + advantage:
                log.info(f"[SWITCH] Koin {current} (score {current_score:.1f}) kalah jauh "
                         f"dari {top['symbol']} (score {top['composite']:.1f})")
                return self._do_switch(current, top["symbol"], current_score,
                                       top["composite"], top)

        # 3) 🛡️ V3.1: Jika stuck NEUTRAL terlalu lama, paksa switch ke koin top
        idle_max = self.cfg.get("SWITCH_IDLE_MAX_SEC", 300)
        if (current_signal == "NEUTRAL" and self._last_neutral_since > 0
                and (time.time() - self._last_neutral_since) >= idle_max):
            if all_results:
                top = all_results[0]
                if top["symbol"] != current:
                    log.info(f"[SWITCH] Paksa switch — stuck NEUTRAL {idle_max}s")
                    self._last_neutral_since = 0.0
                    return self._do_switch(current, top["symbol"], current_score,
                                           top["composite"], top)
        return False


    def _do_switch(self, old: str, new: str, old_score: float, new_score: float, data: dict) -> bool:
        """Eksekusi switch koin."""
        log.info(f"[SWITCH] Pindah dari {old} → {new} "
                 f"(score {old_score:.1f} → {new_score:.1f})")
        self.notifier.send(
            f"🔄 *Auto Switch Koin*\n"
            f"Dari: `{old}` (score {old_score:.1f})\n"
            f"Ke:   `{new}` (score {new_score:.1f})\n"
            f"Sinyal: `{data.get('signal','?')}` | Momentum: `{data.get('momentum',0):+.1f}`"
        )
        self.cfg["SYMBOL"] = new
        self.state.active_symbol = new
        self.price_feed.resubscribe(new)
        self._last_neutral_since = 0.0

        # CEGAH BUG PNL: Segera fetch harga koin baru agar tidak pakai cache harga koin lama
        new_price = self.price_feed.client.get_ticker(new)
        if new_price:
            with self.price_feed._lock:
                self.price_feed.price = new_price

        self.save_config(self.cfg)
        return True

    # ── Fetch & Analyze ───────────────────────────────────────

    def _is_candle_closed(self, df: pd.DataFrame, tolerance_sec: int = 3) -> bool:
        tf_map = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400}
        tf_sec = tf_map.get(self.cfg["PRIMARY_TF"], 60)
        try:
            last_ts  = df.index[-1].timestamp()
            expected = last_ts + tf_sec
            return time.time() >= (expected - tolerance_sec)
        except Exception:
            return True

    def _fetch_df(self, symbol: str, tf: str) -> Optional[pd.DataFrame]:
        df = self.client.get_klines(symbol, tf, self.cfg["CANDLE_LIMIT"])
        if df is None or len(df) < 5:
            return None
        df = self.ta.compute(df)
        df.dropna(inplace=True)
        return df

    def fetch_and_analyze(self) -> Optional[dict]:
        symbol     = self.cfg["SYMBOL"]
        df_primary = self._fetch_df(symbol, self.cfg["PRIMARY_TF"])
        if df_primary is None:
            log.warning("⚠️ Gagal ambil candle primary")
            return None

        signal = self.ta.get_signal(df_primary)

        # Candle closed filter
        if not self._is_candle_closed(df_primary):
            signal["signal"]      = "NEUTRAL"
            signal["hold_reason"] = "⏳ Candle belum tutup"

        # MTF confirmation
        htf_bias = "NEUTRAL"
        if self.cfg["REQUIRE_MTF_CONFIRM"]:
            df_htf = self._fetch_df(symbol, self.cfg["CONFIRM_TF"])
            if df_htf is not None:
                htf_bias = self.ta.get_htf_bias(df_htf)
        signal["htf_bias"] = htf_bias

        if self.cfg["REQUIRE_MTF_CONFIRM"]:
            if signal["signal"] == "LONG"  and htf_bias == "BEARISH":
                signal["signal"] = "NEUTRAL"
                signal["blocked_reason"] = "MTF conflict LONG vs HTF BEARISH"
            elif signal["signal"] == "SHORT" and htf_bias == "BULLISH":
                signal["signal"] = "NEUTRAL"
                signal["blocked_reason"] = "MTF conflict SHORT vs HTF BULLISH"

        # ✨ V3: Early entry detection
        if signal["signal"] == "NEUTRAL" and self.cfg.get("EARLY_ENTRY_MODE"):
            early = self.scanner.early_entry_candidate()
            if early and early["symbol"] == symbol:
                signal["signal"]        = early["signal"]
                signal["is_early_entry"] = True
                signal["hold_reason"]   = ""
                log.info(f"[EARLY] Entry awal terdeteksi: {symbol} {early['signal']} "
                         f"(score {early['raw_score']}, momentum +{early['momentum']:.1f})")

        ws_price = self.price_feed.get_price()
        signal["live_price"] = ws_price if ws_price > 0 else signal["close"]
        signal["symbol"]     = symbol
        self._last_signal    = signal
        return signal

    # ── ✨ Profit-Zone Aware Exit (NEW V3) ────────────────────

    def check_exits_candle(self, signal: dict):
        price       = signal["live_price"]
        current_sig = signal["signal"]
        c           = self.cfg

        for pos in self.state.positions:
            if pos.closed:
                continue

            # ── TP Checks ──────────────────────────────────────
            if pos.side == "LONG":
                if not pos.tp1_hit and price >= pos.take_profit1:
                    pos.tp1_hit   = True
                    buffer = self.cfg.get("BE_FEE_BUFFER_PCT", 0.0015)
                    pos.stop_loss = pos.entry_price * (1 + buffer)
                    if c.get("TP1_PARTIAL_CLOSE"):
                        self._partial_close_position(pos, price, "TP1 Partial ✅")
                    else:
                        self.notifier.send(f"🎯 *TP1*\n`{pos.id}` LONG @ ${price:.4f}")
                if pos.tp1_hit and not pos.tp2_hit and price >= pos.take_profit2:
                    pos.tp2_hit = True
                    self._close_position(pos, price, "TP2 ✅✅")
                elif pos.tp1_hit and price >= pos.take_profit3:
                    self._close_position(pos, price, "TP3 ✅✅✅")
            else:
                if not pos.tp1_hit and price <= pos.take_profit1:
                    pos.tp1_hit   = True
                    buffer = self.cfg.get("BE_FEE_BUFFER_PCT", 0.0015)
                    pos.stop_loss = pos.entry_price * (1 - buffer)
                    if c.get("TP1_PARTIAL_CLOSE"):
                        self._partial_close_position(pos, price, "TP1 Partial ✅")
                    else:
                        self.notifier.send(f"🎯 *TP1*\n`{pos.id}` SHORT @ ${price:.4f}")
                if pos.tp1_hit and not pos.tp2_hit and price <= pos.take_profit2:
                    pos.tp2_hit = True
                    self._close_position(pos, price, "TP2 ✅✅")
                elif pos.tp1_hit and price <= pos.take_profit3:
                    self._close_position(pos, price, "TP3 ✅✅✅")

            if pos.closed:
                continue

            # ── ✨ Signal Flip — Profit Zone Aware ─────────────
            if c.get("EXIT_ON_SIGNAL_FLIP"):
                is_flip = (pos.side == "LONG" and current_sig == "SHORT") or \
                          (pos.side == "SHORT" and current_sig == "LONG")

                if is_flip:
                    # Hitung profit % dari entry
                    if pos.side == "LONG":
                        profit_pct     = (price - pos.entry_price) / pos.entry_price
                        opposite_score = signal["bear_score"]
                    else:
                        profit_pct     = (pos.entry_price - price) / pos.entry_price
                        opposite_score = signal["bull_score"]

                    z1 = c.get("FLIP_ZONE1_PCT", 0.005)
                    z2 = c.get("FLIP_ZONE2_PCT", 0.02)

                    # ZONA 1: Belum / hampir belum profit → keluar langsung
                    if profit_pct < z1:
                        signal["flip_zone"] = "ZONA1"
                        self._close_position(pos, price,
                            f"Signal Flip Z1 — Skor lawan {opposite_score} (profit {profit_pct:.1%})")
                        signal["flip_occurred"] = True

                    # ZONA 2: Profit kecil → keluar hanya jika sinyal lawan kuat
                    elif profit_pct < z2:
                        min_score_z2 = c.get("FLIP_ZONE2_MIN_SCORE", 11)
                        if opposite_score >= min_score_z2:
                            signal["flip_zone"] = "ZONA2"
                            self._close_position(pos, price,
                                f"Signal Flip Z2 — Skor lawan {opposite_score}/{min_score_z2} (profit {profit_pct:.1%})")
                            signal["flip_occurred"] = True
                        else:
                            # Perketat trailing stop 60% lebih ketat
                            trail_dist = c.get("TRAIL_DISTANCE_PCT", 0.005) * 0.4
                            if pos.side == "LONG":
                                new_sl = price * (1 - trail_dist)
                                if new_sl > pos.stop_loss:
                                    pos.stop_loss = new_sl
                                    log.info(f"[FLIP Z2] {pos.id} SL diperketat → ${new_sl:.4f}")
                            else:
                                new_sl = price * (1 + trail_dist)
                                if new_sl < pos.stop_loss:
                                    pos.stop_loss = new_sl
                                    log.info(f"[FLIP Z2] {pos.id} SL diperketat → ${new_sl:.4f}")
                            pos.flip_count = 0  # reset counter

                    # ZONA 3: Profit dalam → butuh konfirmasi beberapa candle
                    else:
                        min_score_z3   = c.get("FLIP_ZONE3_MIN_SCORE", 12)
                        candles_needed = c.get("FLIP_ZONE3_CANDLES", 2)
                        if opposite_score >= min_score_z3:
                            pos.flip_count += 1
                            log.info(f"[FLIP Z3] {pos.id} profit {profit_pct:.1%} | "
                                     f"skor lawan {opposite_score} | konfirmasi {pos.flip_count}/{candles_needed}")
                            if pos.flip_count >= candles_needed:
                                signal["flip_zone"] = "ZONA3"
                                self._close_position(pos, price,
                                    f"Signal Flip Z3 — {candles_needed} candle konfirmasi (profit {profit_pct:.1%})")
                                signal["flip_occurred"] = True
                        else:
                            # Sinyal lawan tidak cukup kuat, reset counter
                            if pos.flip_count > 0:
                                log.info(f"[FLIP Z3] {pos.id} flip counter reset — skor lawan {opposite_score} < {min_score_z3}")
                            pos.flip_count = 0

    # ── Position Management ───────────────────────────────────

    def open_positions_count(self) -> int:
        return sum(1 for p in self.state.positions if not p.closed)

    def _reconcile_positions(self):
        """Deteksi posisi yang ditutup manual di MEXC dan sinkron ke state bot."""
        if self.dry_run:
            return
        active = [p for p in self.state.positions if not p.closed]
        if not active:
            return
        try:
            mexc_pos = self.client.get_open_positions()
            mexc_open = {
                mp["symbol"]
                for mp in (mexc_pos or [])
                if float(mp.get("holdVol", 0)) > 0
            }
            for pos in active:
                if pos.symbol not in mexc_open:
                    # Posisi tidak ada di MEXC — ditutup dari luar (manual/liquidasi)
                    ws_price = self.price_feed.get_price()
                    exit_price = ws_price if ws_price > 0 else pos.entry_price
                    log.warning(
                        f"[RECONCILE] {pos.symbol} tidak ada di MEXC — "
                        f"ditutup eksternal @ ~${exit_price:.4f}"
                    )
                    pos.closed       = True
                    pos.close_reason = "Closed Externally"
                    pos.closed_at    = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
                    self.state.total_trades += 1
                    self.journal.log_trade(pos, exit_price, self._last_signal)
                    self.notifier.send(
                        f"⚠️ *Posisi Ditutup Eksternal*\n"
                        f"Koin: `{pos.symbol}` {pos.side}\n"
                        f"Entry: `${pos.entry_price:.4f}`\n"
                        f"Ditutup manual/likuidasi di MEXC."
                    )
                    # Sync balance dari MEXC
                    real_bal = self.client.get_balance("USDT")
                    if real_bal and real_bal > 0:
                        log.info(f"[RECONCILE] Balance synced: ${self.state.balance:.2f} → ${real_bal:.2f}")
                        self.state.balance = real_bal
                        if real_bal > self.state.peak_balance:
                            self.state.peak_balance = real_bal
                    self.persistence.save(self.state)
        except Exception as e:
            log.warning(f"[RECONCILE] Error: {e}")

    def close_all_positions(self, reason: str = "Manual Stop"):
        active = [p for p in self.state.positions if not p.closed]
        if not active:
            return
        price = self.price_feed.get_price()
        for pos in active:
            self._close_position(pos, price if price > 0 else pos.entry_price, reason)

    def _partial_close_position(self, pos: Position, price: float, reason: str):
        if pos.closed or pos.partial_closed:
            return
        close_ratio  = self.cfg.get("TP1_CLOSE_PCT", 50) / 100.0
        qty_to_close = pos.quantity * close_ratio
        pnl = (price - pos.entry_price) * qty_to_close if pos.side == "LONG" \
              else (pos.entry_price - price) * qty_to_close
        self.state.total_pnl  += pnl
        self.state.daily_pnl  += pnl
        self.state.balance    += pnl
        pos.pnl               += pnl
        pos.quantity          -= qty_to_close
        pos.partial_closed     = True
        log.info(f"[PARTIAL {pos.side}] {pos.id} | {close_ratio*100:.0f}% closed | PnL: ${pnl:.4f}")
        self.notifier.send(
            f"💰 *Partial TP*\n`{pos.id}` {pos.side} `{pos.symbol}`\n"
            f"Profit Aman: `${pnl:.4f}`\nSisa qty berjalan dengan BE SL."
        )
        if pnl > 0 and self.cfg.get("ENABLE_AUTO_SECURE"):
            secure_amt = pnl * (self.cfg.get("SECURE_PROFIT_PCT", 50) / 100.0)
            if secure_amt >= self.cfg.get("MIN_SECURE_TRANSFER", 1.0):
                if self.dry_run:
                    self.state.balance    -= secure_amt
                    self.state.secured_total += secure_amt
                else:
                    if self.spot_client.transfer_to_spot(secure_amt):
                        self.state.balance    -= secure_amt
                        self.state.secured_total += secure_amt
        self.persistence.save(self.state)

    def _close_position(self, pos: Position, price: float, reason: str):
        if pos.closed:
            return
        pnl = (price - pos.entry_price) * pos.quantity if pos.side == "LONG" \
              else (pos.entry_price - price) * pos.quantity
        pos.pnl          = round(pos.pnl + pnl, 4)
        pos.closed       = True
        pos.close_reason = reason
        pos.closed_at    = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
        self.state.total_trades  += 1
        self.state.total_pnl     += pnl
        self.state.daily_pnl     += pnl
        self.state.balance       += pnl
        if pnl > 0:
            self.state.winning_trades += 1
        else:
            # 🛡️ V3.1: Catat waktu loss untuk cooldown per-koin
            self._last_loss_time[pos.symbol] = time.time()
        if self.state.balance > self.state.peak_balance:
            self.state.peak_balance = self.state.balance
        emoji = "✅" if pnl > 0 else "❌"
        log.info(
            f"[CLOSE {pos.side}] {reason} | {pos.id} | {pos.symbol} | "
            f"${pos.entry_price:.4f} → ${price:.4f} | PnL: {emoji} ${pnl:+.4f} | "
            f"Bal: ${self.state.balance:,.2f}"
        )
        self.journal.log_trade(pos, price, self._last_signal)
        # Sync balance dari MEXC setelah close agar tidak drift
        if not self.dry_run:
            try:
                time.sleep(1.0)
                real_bal = self.client.get_balance("USDT")
                if real_bal and real_bal > 0:
                    log.info(f"[SYNC] Balance synced: ${self.state.balance:.2f} → ${real_bal:.2f} (MEXC)")
                    self.state.balance = real_bal
                    if real_bal > self.state.peak_balance:
                        self.state.peak_balance = real_bal
            except Exception as e:
                log.warning(f"[SYNC] Balance sync failed: {e}")
        self.persistence.save(self.state)
        if pnl > 0 and self.cfg.get("ENABLE_AUTO_SECURE"):
            secure_amt = pnl * (self.cfg.get("SECURE_PROFIT_PCT", 50) / 100.0)
            if secure_amt >= self.cfg.get("MIN_SECURE_TRANSFER", 1.0):
                if self.dry_run:
                    self.state.balance    -= secure_amt
                    self.state.secured_total += secure_amt
                    self.persistence.save(self.state)
                else:
                    if self.spot_client.transfer_to_spot(secure_amt):
                        self.state.balance    -= secure_amt
                        self.state.secured_total += secure_amt
                        self.persistence.save(self.state)
        self.notifier.send(
            f"{emoji} *Posisi Ditutup*\nID: `{pos.id}`\n"
            f"Koin: `{pos.symbol}` {pos.side}\n"
            f"${pos.entry_price:.4f} → ${price:.4f}\n"
            f"PnL: `${pnl:+.4f} USDT`\nAlasan: {reason}\n"
            f"Balance: `${self.state.balance:,.2f}`"
        )
        if not self.dry_run:
            side_map = {"LONG": 4, "SHORT": 2}
            self.client.place_order(pos.symbol, side_map[pos.side], 5, self.cfg["LEVERAGE"], pos.quantity)

    def open_position(self, side: str, signal: dict):
        if self.open_positions_count() >= self.cfg["MAX_OPEN_TRADES"]:
            signal["hold_reason"] = "Max posisi tercapai"
            return

        # ⏳ WAF cooldown — jangan retry order jika masih dalam cooldown
        if time.time() < self._waf_block_until:
            remaining = int(self._waf_block_until - time.time())
            signal["hold_reason"] = f"⏳ WAF cooldown {remaining}s"
            return

        symbol = signal.get("symbol", self.cfg["SYMBOL"])

        # 🛡️ V3.1: Cooldown per-koin setelah loss
        cooldown_sec = self.cfg.get("LOSS_COOLDOWN_SEC", 120)
        last_loss = self._last_loss_time.get(symbol, 0)
        elapsed = time.time() - last_loss
        if elapsed < cooldown_sec:
            remaining = int(cooldown_sec - elapsed)
            signal["hold_reason"] = f"⏳ Cooldown {remaining}s (loss terakhir di {symbol})"
            return

        # 🛡️ V3.1: ATR% sanity check — jangan entry koin terlalu volatile
        atr_pct = signal.get("atr_pct", 0)
        max_atr_pct = self.cfg.get("MAX_ATR_PCT_ENTRY", 5.0)
        if atr_pct > max_atr_pct:
            signal["hold_reason"] = f"⚠️ ATR% {atr_pct:.1f}% > max {max_atr_pct}% (terlalu volatile)"
            return

        # 🔥 V3.2: Trend Power Gate — pisah threshold early vs normal entry
        trend_power = signal.get("trend_power", 0)
        if self.cfg.get("USE_TREND_POWER", True):
            is_early = signal.get("is_early_entry", False)
            if is_early:
                min_tp = self.cfg.get("MIN_TREND_POWER_EARLY", 40)
            else:
                min_tp = self.cfg.get("MIN_TREND_POWER", 55)
            if trend_power < min_tp:
                label = "early" if is_early else "normal"
                signal["hold_reason"] = f"⚡ Trend Power {trend_power}/100 < min {min_tp} [{label}] (trend lemah)"
                return
            if signal.get("overextended", False):
                ext = signal.get("ext_ratio", 0)
                signal["hold_reason"] = f"🚫 Over-extended {ext:.1f}x ATR dari EMA (masuk terlambat)"
                return
            if self.cfg.get("ADX_RISING_REQUIRED", True) and not signal.get("adx_rising", False):
                signal["hold_reason"] = f"📉 ADX turun (trend melemah, bukan menguat)"
                return

        try:
            entry  = signal["live_price"]
            atr    = signal["atr"]
            levels = self.risk.calculate_levels(side, entry, atr, trend_power)
            if not self.risk.check_rr(levels):
                signal["hold_reason"] = f"RR terlalu rendah ({levels['rr_ratio']:.2f})"
                return
            prob_ok, prob_msg = self.risk.check_tp_probability(levels, signal)
            if not prob_ok:
                signal["hold_reason"] = prob_msg
                return
        except Exception as e:
            signal["hold_reason"] = f"Error kalkulasi level: {e}"
            return

        qty = self.risk.position_size(
            self.state.balance, levels["sl_distance"], entry,
            win_rate=self.state.win_rate() / 100
        )
        if qty <= 0:
            signal["hold_reason"] = "Position size 0 (saldo terlalu kecil)"
            return
        qty_total = round(qty, 1)
        if qty_total < 0.1:
            qty_total = 0.1
        cost = (qty_total * entry) / self.cfg["LEVERAGE"]
        if cost > self.state.balance * 0.95:
            signal["hold_reason"] = f"Cost ${cost:.2f} melebihi saldo"
            return
        if (qty_total * entry) < 5.0:
            signal["hold_reason"] = f"Order terlalu kecil (${qty_total * entry:.2f} < $5)"
            return

        trade_id = self._gen_trade_id()
        is_early = signal.get("is_early_entry", False)
        pos = Position(
            id=trade_id, symbol=symbol, side=side,
            entry_price=entry, quantity=qty_total,
            stop_loss=levels["stop_loss"],
            take_profit1=levels["take_profit1"],
            take_profit2=levels["take_profit2"],
            take_profit3=levels["take_profit3"],
            opened_at=datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"),
            highest_price=entry, lowest_price=entry,
            opened_ts=time.time(),  # 🛡️ V3.1: timestamp untuk grace period
        )

        if not self.dry_run:
            side_map = {"LONG": 1, "SHORT": 3}
            result = self.client.place_order(symbol, side_map[side], 5, self.cfg["LEVERAGE"], qty_total)
            if result:
                pos.order_id = str(result)
                # Sync actual fill price from MEXC
                time.sleep(1.0)
                mexc_pos = self.client.get_open_positions(symbol)
                for mp in (mexc_pos or []):
                    if mp.get("symbol") == symbol and float(mp.get("holdVol", 0)) > 0:
                        actual_entry = float(mp.get("holdAvgPrice") or mp.get("openAvgPrice") or 0)
                        if actual_entry > 0 and abs(actual_entry - entry) / entry < 0.02:
                            log.info(f"[SYNC] Entry synced: ${entry:.4f} → ${actual_entry:.4f}")
                            pos.entry_price = actual_entry
                            pos.highest_price = actual_entry
                            pos.lowest_price = actual_entry
                            lvl2 = self.risk.calculate_levels(side, actual_entry, atr, trend_power)
                            pos.stop_loss    = lvl2["stop_loss"]
                            pos.take_profit1 = lvl2["take_profit1"]
                            pos.take_profit2 = lvl2["take_profit2"]
                            pos.take_profit3 = lvl2["take_profit3"]
                        break
            else:
                self._waf_block_until = time.time() + 300  # cooldown 5 menit setelah gagal
                log.warning(f"[ORDER] Gagal — WAF cooldown 300s aktif")
                signal["hold_reason"] = "Gagal place order (API Error)"
                return

        self.state.positions.append(pos)
        self.persistence.save(self.state)

        early_label = " 🔮 [EARLY]" if is_early else ""
        prefix      = "🔵 [DRY RUN] " if self.dry_run else ""
        log.info(
            f"[OPEN {side}]{early_label} {trade_id} | {symbol} | "
            f"${entry:.4f} | Qty: {qty_total} | SL: ${levels['stop_loss']:.4f} | "
            f"TP1: ${levels['take_profit1']:.4f} | RR: 1:{levels['rr_ratio']:.2f}"
        )
        self.notifier.send(
            f"{prefix}🚀 *Posisi Dibuka{early_label}*\n"
            f"ID: `{trade_id}`\nKoin: `{symbol}` *{side}*\n"
            f"Entry: `${entry:.4f}`\n"
            f"SL: `${levels['stop_loss']:.4f}` | TP1: `${levels['take_profit1']:.4f}`\n"
            f"RR: `1:{levels['rr_ratio']:.2f}` | Conf: `{signal['confidence']}%`\n"
            f"HTF: `{signal.get('htf_bias','?')}` | "
            f"Bull: `{signal['bull_score']}` Bear: `{signal['bear_score']}`"
        )

    # ── Dashboard State ───────────────────────────────────────

    def get_dashboard_state(self) -> dict:
        ws_price = self.price_feed.get_price()
        sig      = self._last_signal or {}
        active   = []
        float_pnl = 0.0

        # Fetch live PnL from MEXC (cached 10s to avoid rate limit)
        mexc_pnl_map = {}
        now = time.time()
        if not self.dry_run and any(not p.closed for p in self.state.positions):
            if now - getattr(self, "_mexc_pnl_cache_ts", 0) >= 10:
                try:
                    mexc_pos_list = self.client.get_open_positions()
                    cache = {}
                    for mp in (mexc_pos_list or []):
                        if float(mp.get("holdVol", 0)) > 0:
                            # Simpan actual entry price dari MEXC untuk hitung PnL akurat
                            cache[mp["symbol"]] = {
                                "actual_entry": float(mp.get("holdAvgPrice") or mp.get("openAvgPrice") or 0),
                                "pos_type": int(mp.get("positionType", 1)),  # 1=LONG, 2=SHORT
                            }
                    self._mexc_pnl_cache = cache
                    self._mexc_pnl_cache_ts = now
                except Exception:
                    pass
            mexc_pnl_map = getattr(self, "_mexc_pnl_cache", {})

        for p in self.state.positions:
            if p.closed: continue
            curr  = ws_price if ws_price > 0 else p.entry_price
            # Gunakan holdAvgPrice MEXC (harga fill aktual) untuk PnL akurat
            mexc_data = mexc_pnl_map.get(p.symbol)
            if mexc_data and mexc_data["actual_entry"] > 0 and curr > 0:
                actual_entry = mexc_data["actual_entry"]
                fpnl = (curr - actual_entry) * p.quantity if p.side == "LONG" \
                       else (actual_entry - curr) * p.quantity
            else:
                fpnl = (curr - p.entry_price) * p.quantity if p.side == "LONG" \
                       else (p.entry_price - curr) * p.quantity
            float_pnl += fpnl
            active.append({
                "id": p.id, "symbol": p.symbol, "side": p.side,
                "entry_price": p.entry_price, "quantity": p.quantity,
                "stop_loss": p.stop_loss,
                "take_profit1": p.take_profit1, "take_profit2": p.take_profit2,
                "take_profit3": p.take_profit3,
                "pnl": round(p.pnl, 4), "live_pnl": round(fpnl, 4),
                "trailing_active": p.trailing_active,
                "flip_count": p.flip_count,
            })
        return {
            "balance":      round(self.state.balance + float_pnl, 2),
            "peak_balance": round(max(self.state.peak_balance, self.state.balance + float_pnl), 2),
            "leverage":     self.cfg["LEVERAGE"],
            "total_pnl":    round(self.state.total_pnl + float_pnl, 4),
            "daily_pnl":    round(self.state.daily_pnl + float_pnl, 4),
            "win_rate":     round(self.state.win_rate(), 1),
            "total_trades": self.state.total_trades,
            "drawdown":     round(self.state.drawdown(), 2),
            "iteration":    self.state.iteration,
            "started_at":   self.state.started_at,
            "circuit_breaker": self.state.circuit_breaker,
            "circuit_reason":  self.state.circuit_reason,
            "circuit_type":    self.state.circuit_type,
            "circuit_triggered_at": self.state.circuit_triggered_at,
            "signal":     sig.get("signal", "NEUTRAL"),
            "bull_score": sig.get("bull_score", 0),
            "bear_score": sig.get("bear_score", 0),
            "confidence": sig.get("confidence", 0),
            "htf_bias":   sig.get("htf_bias", "—"),
            "rsi":        round(sig.get("rsi", 0), 2),
            "stoch_k":    round(sig.get("stoch_k", 0), 2),
            "atr":        round(sig.get("atr", 0), 6),
            "vol_ratio":  round(sig.get("vol_ratio", 0), 2),
            "adx":        round(sig.get("adx", 0), 1),
            "trend_power": sig.get("trend_power", 0),
            "adx_rising":  sig.get("adx_rising", False),
            "overextended": sig.get("overextended", False),
            "ext_ratio":   sig.get("ext_ratio", 0),
            "tp_details":  sig.get("tp_details", {}),
            "live_price": round(ws_price if ws_price > 0 else sig.get("close", 0), 6),
            "hold_reason": sig.get("hold_reason", ""),
            "session":    self.session.is_trading_allowed()[1],
            "secured_total": round(self.state.secured_total, 2),
            "positions":  active,
            "is_dry_run": self.dry_run,
            "symbol":     self.cfg["SYMBOL"],
            "active_symbol": self.state.active_symbol,
            "multi_coin_mode": self.cfg.get("MULTI_COIN_MODE", False),
            "scanner_results": self.scanner.get_results()[:8],   # top 8 untuk dashboard
            "scanner_last":    round(self.scanner._last_scan, 0),
            "ws_alive":       self.price_feed.is_ws_alive,
            "api_error":      self.state.api_error,
        }

    def print_status(self, signal: dict):
        try:
            ws_price   = self.price_feed.get_price()
            live_price = ws_price if ws_price > 0 else signal.get("close", 0)
            _, session = self.session.is_trading_allowed()
            tp = signal.get('trend_power', 0)
            ext = signal.get('ext_ratio', 0)
            adx_r = '↗️' if signal.get('adx_rising') else '↘️'
            log.info(
                f"[{self.cfg['SYMBOL']}] ${live_price:,.4f} | "
                f"Sig: {signal.get('signal','?')} | "
                f"Bull: {signal.get('bull_score',0)} Bear: {signal.get('bear_score',0)} | "
                f"🔥TP: {tp}/100 | ADX: {signal.get('adx',0):.1f}{adx_r} | "
                f"Ext: {ext:.1f}x | {signal.get('hold_reason','')}"
            )
        except Exception as e:
            log.warning(f"print_status error: {e}")

    # ── Main Loop ─────────────────────────────────────────────

    def run(self):
        log.info("=" * 64)
        log.info("  MEXC Pro Trading Bot V3 — Multi-Coin Edition")
        log.info("=" * 64)
        log.info(f"  Mode: {'DRY RUN' if self.dry_run else 'LIVE TRADING'}")
        log.info(f"  Multi-Coin: {'ON' if self.cfg.get('MULTI_COIN_MODE') else 'OFF'}")
        log.info(f"  Early Entry: {'ON' if self.cfg.get('EARLY_ENTRY_MODE') else 'OFF'}")
        log.info("=" * 64)

        def _shutdown(sig, frame):
            log.info("Shutdown diterima...")
            self.price_feed.stop()
            self.persistence.save(self.state)
            self.notifier.send("⛔ Bot V3 dihentikan. State disimpan.")
            sys.exit(0)
        os_signal.signal(os_signal.SIGINT, _shutdown)
        os_signal.signal(os_signal.SIGTERM, _shutdown)

        if self.run_dashboard:
            self._start_dashboard()

        self.notifier.send(
            f"🤖 *TRADE Bot V3 Dimulai*\n"
            f"Mode: `{'DRY RUN' if self.dry_run else 'LIVE'}`\n"
            f"Multi-Coin: `{'ON' if self.cfg.get('MULTI_COIN_MODE') else 'OFF'}`\n"
            f"Koin Awal: `{self.cfg['SYMBOL']}`\n"
            f"Balance: `${self.state.balance:,.2f} USDT`"
        )

        # Scan awal
        if self.cfg.get("MULTI_COIN_MODE"):
            log.info("Memulai scan koin pertama...")
            self._trigger_scan()

        while True:
            try:
                self.state.iteration += 1
                self._check_daily_reset()

                if self._check_circuit_breaker():
                    log.warning(f"Circuit breaker: {self.state.circuit_reason}")
                    time.sleep(10)
                    today = datetime.now().strftime("%Y-%m-%d")
                    if self.state.daily_reset_date != today:
                        self.state.circuit_breaker = False
                    continue

                # Sync balance dari MEXC setiap 2 menit (tangkap transfer manual)
                if not self.dry_run and self.open_positions_count() == 0:
                    now_ts = time.time()
                    if now_ts - getattr(self, "_last_balance_sync_ts", 0) >= 120:
                        real_bal = self.client.get_balance("USDT")
                        if real_bal and real_bal > 0 and abs(real_bal - self.state.balance) > 0.01:
                            log.info(f"[BALANCE SYNC] ${self.state.balance:.2f} → ${real_bal:.2f}")
                            self.state.balance = real_bal
                            if real_bal > self.state.peak_balance:
                                self.state.peak_balance = real_bal
                            self.persistence.save(self.state)
                        self._last_balance_sync_ts = now_ts

                ok, session = self.session.is_trading_allowed()
                if not ok:
                    log.info(f"Trading tidak diizinkan: {session}")
                    time.sleep(60)
                    continue

                # ✨ V3: Jalankan scanner jika sudah waktunya
                if self.cfg.get("MULTI_COIN_MODE") and self.scanner.needs_scan():
                    self._trigger_scan()

                # ✨ V3: Evaluasi switch koin (hanya jika tidak ada posisi terbuka)
                if self.cfg.get("MULTI_COIN_MODE") and self.cfg.get("AUTO_SWITCH_COIN"):
                    if self.open_positions_count() == 0:
                        self._maybe_switch_coin()

                # Analisis koin aktif
                signal = self.fetch_and_analyze()
                if signal is None:
                    time.sleep(30)
                    continue

                # Cek posisi yang ditutup manual di MEXC (setiap 30 detik)
                if self.open_positions_count() > 0:
                    now_ts = time.time()
                    if now_ts - getattr(self, "_last_reconcile_ts", 0) >= 30:
                        self._reconcile_positions()
                        self._last_reconcile_ts = now_ts

                # Cek exit
                self.check_exits_candle(signal)

                # Entry baru
                can_entry = True
                if signal.get("flip_occurred") and not self.cfg.get("AUTO_REVERSE_ON_FLIP", False):
                    can_entry = False
                    signal["blocked_reason"] = "Auto-reverse disabled"

                if can_entry:
                    if signal["signal"] == "LONG":
                        self.open_position("LONG", signal)
                    elif signal["signal"] == "SHORT":
                        self.open_position("SHORT", signal)

                self.print_status(signal)
                self.persistence.save(self.state)
                time.sleep(self.cfg["POLL_INTERVAL"])

            except KeyboardInterrupt:
                log.info("KeyboardInterrupt — shutdown...")
                self.price_feed.stop()
                self.persistence.save(self.state)
                self.notifier.send("⛔ Bot V3 dihentikan manual.")
                break
            except Exception as e:
                log.exception(f"Error loop utama: {e}")
                time.sleep(30)

    # ── Dashboard (Flask) ─────────────────────────────────────

    def _start_dashboard(self):
        try:
            from flask import Flask, jsonify, render_template_string, request
            app = Flask(__name__)

            @app.route("/")
            def index():
                return render_template_string(DASHBOARD_HTML)

            @app.route("/api/state")
            def api_state():
                import math
                from decimal import Decimal
                def sanitize(v):
                    if isinstance(v, float):
                        return 0.0 if (math.isnan(v) or math.isinf(v)) else v
                    if isinstance(v, Decimal): return float(v)
                    if isinstance(v, dict):  return {k: sanitize(val) for k, val in v.items()}
                    if isinstance(v, list):  return [sanitize(val) for val in v]
                    return v
                return jsonify(sanitize(self.get_dashboard_state()))

            @app.route("/api/scanner")
            def api_scanner():
                return jsonify({
                    "results":   self.scanner.get_results()[:self.cfg.get("SCAN_TOP_N", 30)],
                    "last_scan": self.scanner._last_scan,
                    "active":    self.cfg["SYMBOL"],
                })

            @app.route("/api/reset_circuit", methods=["POST"])
            def reset_circuit():
                if self.state.circuit_type == "AUTO":
                    elapsed = time.time() - self.state.circuit_triggered_at
                    if elapsed < 600:
                        return jsonify({"success": False, "error": f"Tunggu {int(600-elapsed)}s"}), 403
                self.state.circuit_breaker = False
                self.state.circuit_reason  = ""
                self.state.circuit_type    = ""
                self.state.peak_balance    = self.state.balance
                self.state.daily_pnl       = 0.0
                self.persistence.save(self.state)
                return jsonify({"success": True})

            @app.route("/api/manual_stop", methods=["POST"])
            def manual_stop():
                self.close_all_positions(reason="Stop Manual")
                self.state.circuit_breaker      = True
                self.state.circuit_type         = "MANUAL"
                self.state.circuit_reason       = "Berhenti manual"
                self.state.circuit_triggered_at = time.time()
                self.persistence.save(self.state)
                return jsonify({"success": True})

            @app.route("/api/history")
            def api_history():
                date_filter  = request.args.get("date")
                history      = self.journal.get_trades(date_filter)
                total_pnl = total_win = total_loss = 0.0
                for t in history:
                    pnl = float(t.get("pnl", 0))
                    total_pnl += pnl
                    if pnl > 0: total_win  += pnl
                    else:       total_loss += pnl
                return jsonify({
                    "history": history, "total_pnl": round(total_pnl, 4),
                    "total_win": round(total_win, 4), "total_loss": round(total_loss, 4)
                })

            @app.route("/api/config", methods=["GET", "POST"])
            def api_config():
                if request.method == "GET":
                    hide     = ["TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]
                    safe_cfg = {k: v for k, v in self.cfg.items() if k not in hide}
                    return jsonify(safe_cfg)
                try:
                    new_data = request.json
                    if not new_data:
                        return jsonify({"success": False, "error": "Data kosong"}), 400
                    if "SYMBOL" in new_data and new_data["SYMBOL"] != self.cfg.get("SYMBOL"):
                        active_pos = [p for p in self.state.positions if not p.closed]
                        if active_pos:
                            return jsonify({"success": False, "error": "Masih ada posisi terbuka"}), 400
                    self.update_config_live(new_data)
                    return jsonify({"success": True})
                except Exception as e:
                    return jsonify({"success": False, "error": str(e)}), 500

            @app.route("/api/force_scan", methods=["POST"])
            def force_scan():
                """Paksa scan ulang sekarang."""
                self.scanner._last_scan = 0  # reset timer agar scan segera
                self._trigger_scan()
                return jsonify({"success": True, "message": "Scan dimulai di background"})

            @app.route("/api/top_coins")
            def api_top_coins():
                now = time.time()
                if now - getattr(self, "_tc_last", 0) > 300 or not getattr(self, "_tc_cache", []):
                    self._tc_cache = self.client.get_top_volume_coins(100)
                    self._tc_last  = now
                return jsonify({"success": True, "data": getattr(self, "_tc_cache", [])})

            @app.route("/api/reset_all", methods=["POST"])
            def api_reset_all():
                self.state.balance = self.cfg.get("VIRTUAL_BALANCE", 100.0)
                self.state.peak_balance = self.state.balance
                self.state.total_pnl = self.state.daily_pnl = 0.0
                self.state.total_trades = self.state.winning_trades = 0
                self.state.iteration = 0
                self.state.circuit_breaker = False
                self.state.secured_total = 0.0
                if os.path.exists(self.journal.filepath):
                    os.remove(self.journal.filepath)
                    self.journal._ensure_header()
                self.persistence.save(self.state)
                return jsonify({"success": True})

            t = threading.Thread(
                target=lambda: app.run(
                    host=self.cfg["DASHBOARD_HOST"],
                    port=self.cfg["DASHBOARD_PORT"],
                    debug=False, use_reloader=False
                ),
                daemon=True, name="Dashboard"
            )
            t.start()
            log.info(f"Dashboard: http://localhost:{self.cfg['DASHBOARD_PORT']}")
        except ImportError:
            log.warning("Flask tidak tersedia — dashboard tidak aktif. Install: pip install flask")


# ══════════════════════════════════════════════════════════════
#  DASHBOARD HTML
# ══════════════════════════════════════════════════════════════

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>MEXC Bot V3</title>
<style>
  :root{--bg:#0d1117;--card:#161b22;--border:#30363d;--green:#3fb950;--red:#f85149;--gold:#d29922;--blue:#58a6ff;--gray:#8b949e;--text:#e6edf3}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',monospace;font-size:13px}
  .header{background:var(--card);border-bottom:1px solid var(--border);padding:12px 16px;display:flex;align-items:center;gap:10px;flex-wrap:wrap}
  .header h1{font-size:15px;font-weight:700;color:var(--blue)}
  .badge{font-size:10px;padding:2px 8px;border-radius:10px;font-weight:700}
  .badge-live{background:#f8514922;color:var(--red);border:1px solid var(--red)}
  .badge-dry{background:#58a6ff22;color:var(--blue);border:1px solid var(--blue)}
  .badge-lev{background:#d2992222;color:var(--gold);border:1px solid var(--gold)}
  .badge-mc{background:#3fb95022;color:var(--green);border:1px solid var(--green)}
  .tabs{display:flex;background:var(--card);border-bottom:1px solid var(--border)}
  .tab{padding:10px 18px;cursor:pointer;border-bottom:2px solid transparent;font-size:12px;font-weight:600;color:var(--gray);transition:.2s}
  .tab.active{color:var(--blue);border-color:var(--blue)}
  .page{display:none;padding:14px;max-width:900px;margin:auto}
  .page.active{display:block}
  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px}
  .grid4{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:12px}
  .card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:12px}
  .chart-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:20px}
  .card-label{font-size:10px;color:var(--gray);text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px}
  .card-val{font-size:18px;font-weight:700}
  .pos{color:var(--green)} .neg{color:var(--red)} .neu{color:var(--gray)}
  .sig-box{border-radius:8px;padding:14px;margin-bottom:12px;border:1px solid}
  .sig-long{background:#3fb95015;border-color:#3fb95044}
  .sig-short{background:#f8514915;border-color:#f8514944}
  .sig-neutral{background:#d2992215;border-color:#d2992244}
  .sig-dir{font-size:22px;font-weight:900}
  .sig-dir.long{color:var(--green)} .sig-dir.short{color:var(--red)} .sig-dir.neutral{color:var(--gold)}
  .pos-card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:12px;margin-bottom:8px}
  .pos-hdr{display:flex;align-items:center;gap:8px;margin-bottom:8px}
  .pos-grid{display:grid;grid-template-columns:auto 1fr;gap:3px 12px}
  .pos-grid dt{color:var(--gray);font-size:11px} .pos-grid dd{font-size:12px;font-weight:600}
  .badge-long{background:#3fb95022;color:var(--green);border:1px solid #3fb95044;border-radius:4px;padding:2px 8px;font-size:11px;font-weight:700}
  .badge-short{background:#f8514922;color:var(--red);border:1px solid #f8514944;border-radius:4px;padding:2px 8px;font-size:11px;font-weight:700}
  .ind-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:12px}
  .ind-card{background:var(--card);border:1px solid var(--border);border-radius:6px;padding:8px;text-align:center}
  .ind-label{font-size:9px;color:var(--gray);text-transform:uppercase}
  .ind-val{font-size:15px;font-weight:700;margin-top:2px}
  .cb-banner{background:#f8514918;border:1px solid #f8514944;border-radius:8px;padding:12px;margin-bottom:12px;color:var(--red)}
  .btn{padding:8px 14px;border:none;border-radius:6px;cursor:pointer;font-weight:700;font-size:12px;transition:.2s}
  .btn-red{background:#f8514922;color:var(--red);border:1px solid var(--red)}
  .btn-green{background:#3fb95022;color:var(--green);border:1px solid var(--green)}
  .btn-blue{background:#58a6ff22;color:var(--blue);border:1px solid var(--blue)}
  .btn-gold{background:#d2992222;color:var(--gold);border:1px solid var(--gold)}
  .btn:disabled{opacity:.4;cursor:not-allowed}
  table{width:100%;border-collapse:collapse;font-size:12px}
  th{text-align:left;padding:6px 8px;color:var(--gray);font-size:10px;text-transform:uppercase;border-bottom:1px solid var(--border)}
  td{padding:6px 8px;border-bottom:1px solid var(--border)22}
  .scanner-table td:first-child{font-weight:700;color:var(--blue)}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--green);display:inline-block;animation:blink 1.5s infinite}
  .dot.off{background:var(--red);animation:none}
   @keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}
   .api-error-bar { background: #fee2e2; color: #b91c1c; border-bottom: 1px solid #f87171; padding: 10px; text-align: center; font-size: 13px; font-weight: 600; }
   .switch-badge{font-size:9px;padding:1px 5px;border-radius:8px;background:#58a6ff22;color:var(--blue);border:1px solid var(--blue);margin-left:4px}
  .settings-grid{display:grid;grid-template-columns:repeat(auto-fill, minmax(280px, 1fr));gap:15px}
  .setting-group{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:15px;margin-bottom:15px}
  .setting-group h3{font-size:12px;color:var(--blue);margin-bottom:12px;border-bottom:1px solid var(--border);padding-bottom:5px;text-transform:uppercase;letter-spacing:1px}
  .setting-item{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;gap:10px}
  .setting-item > label:not(.toggle-switch){font-size:11px;color:var(--gray);flex:1}
  .setting-item input, .setting-item select{background:var(--bg);border:1px solid var(--border);color:var(--text);padding:6px 10px;border-radius:6px;font-size:11px;width:120px;outline:none;transition:all 0.2s}
  .setting-item input:focus, .setting-item select:focus{border-color:var(--blue);box-shadow:0 0 5px rgba(88,166,255,0.2)}
  .setting-item input[type="checkbox"]{width:auto;flex-shrink:0}
  .setting-save-bar{position:fixed;bottom:0;left:0;right:0;background:var(--card);border-top:1px solid var(--border);padding:15px;text-align:center;box-shadow:0 -5px 15px rgba(0,0,0,0.3);z-index:100}
  .save-status{font-size:11px;margin-bottom:8px;height:12px}
  .toggle-switch { position: relative; display: inline-block; width: 40px; height: 20px; flex: 0 0 40px; margin-left: auto; }
  .toggle-switch input { opacity: 0; width: 0; height: 0; }
  .slider { position: absolute; cursor: pointer; top: 0; left: 0; right: 0; bottom: 0; background-color: #333; transition: .4s; border-radius: 20px; }
  .slider:before { position: absolute; content: ""; height: 16px; width: 16px; left: 2px; bottom: 2px; background-color: white; transition: .4s; border-radius: 50%; }
  input:checked + .slider { background-color: var(--blue); }
  input:checked + .slider:before { transform: translateX(20px); }

  /* Toast Notification */
  #toast {
    position: fixed; top: 20px; right: 20px; z-index: 9999;
    padding: 12px 20px; border-radius: 8px; color: #fff;
    font-weight: 600; font-size: 13px; display: none;
    box-shadow: 0 4px 12px rgba(0,0,0,0.5);
    animation: slideIn 0.3s ease;
  }
  .toast-success { background: #238636; border: 1px solid #2ea043; }
  .toast-error { background: #da3633; border: 1px solid #f85149; }
  @keyframes slideIn { from { transform: translateX(100%); } to { transform: translateX(0); } }
  @media (max-width: 768px) {
    .chart-grid { grid-template-columns: 1fr; gap: 12px; margin-bottom: 15px; }
    .grid2 { grid-template-columns: 1fr 1fr; gap: 8px; }
    .grid4 { grid-template-columns: repeat(2, 1fr); gap: 10px; }
  }
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
</head>
<body>
<div id="toast"></div>
<div id="apiErrorBar" class="api-error-bar" style="display:none">
  ⚠️ <span id="apiErrorMsg"></span>
</div>
<div class="header">
  <h1>🤖 MEXC Bot <span style="color:var(--gold)">V3</span></h1>
  <span id="modeBadge" class="badge badge-dry">DRY RUN</span>
  <span id="levBadge" class="badge badge-lev">25X</span>
  <span id="mcBadge" class="badge badge-mc" style="display:none">MULTI-COIN</span>
  <span id="wsDot" class="dot"></span>
  <span id="currPrice" style="font-weight:700;margin-left:auto;font-size:14px">$0</span>
</div>
<div class="tabs">
  <div class="tab active" onclick="switchTab('home')">📊 Dashboard</div>
  <div class="tab" onclick="switchTab('scanner')">🔍 Scanner</div>
  <div class="tab" onclick="switchTab('history')">📋 History</div>
  <div class="tab" onclick="switchTab('settings')">⚙️ Settings</div>
</div>

<!-- HOME TAB -->
<div id="tab-home" class="page active">
  <div id="cbBanner" class="cb-banner" style="display:none">
    ⛔ Circuit Breaker: <span id="cbReason"></span>
    <div style="margin-top:8px;display:flex;gap:8px">
      <button id="btnReset" class="btn btn-green" onclick="resetCircuit()">Mulai Trade Lagi</button>
      <button class="btn btn-red" onclick="manualStop()">Stop Manual</button>
    </div>
  </div>

  <div class="grid4">
    <div class="card"><div class="card-label">Balance</div><div class="card-val" id="balance">$0</div><div style="font-size:10px;color:var(--gray)" id="pnlSub"></div></div>
    <div class="card"><div class="card-label">Total PnL</div><div class="card-val" id="totalPnl">$0</div><div style="font-size:10px;color:var(--gray)" id="dailyPnl"></div></div>
    <div class="card"><div class="card-label">Win Rate</div><div class="card-val" id="winRate">0%</div><div style="font-size:10px;color:var(--gray)" id="tradeCount"></div></div>
    <div class="card"><div class="card-label">Secured</div><div class="card-val" id="securedTot">$0</div><div style="font-size:10px;color:var(--gray)" id="drawdown"></div></div>
  </div>

  <div class="sig-box sig-neutral" id="signalBox">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
      <span class="sig-dir neutral" id="sigVal">NEUTRAL</span>
      <span style="font-size:11px;color:var(--gray)" id="symbolLabel">—</span>
    </div>
    <div style="font-size:12px;color:var(--gray)" id="sigDetails">Bull: 0 | Bear: 0 | Conf: 0%</div>
    <div style="font-size:11px;color:var(--gold);margin-top:4px" id="holdReason"></div>
  </div>

  <!-- 🔥 TREND POWER METER -->
  <div class="card" style="margin-bottom:12px;padding:10px 14px">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
      <span style="font-size:11px;font-weight:700;color:var(--text)">🔥 TREND POWER</span>
      <span style="font-size:18px;font-weight:900" id="tpValue">0</span>
    </div>
    <div style="background:#1a1a2e;border-radius:6px;height:10px;overflow:hidden">
      <div id="tpBar" style="height:100%;width:0%;border-radius:6px;transition:width 0.5s ease,background 0.5s ease;background:#555"></div>
    </div>
    <div style="display:flex;justify-content:space-between;margin-top:6px;font-size:9px;color:var(--gray)">
      <span>0 Lemah</span>
      <span id="tpLabel" style="color:var(--gold);font-weight:600">—</span>
      <span>100 🔥</span>
    </div>
    <div style="display:flex;gap:8px;margin-top:8px;font-size:10px">
      <span id="tpAdxBadge" style="padding:2px 8px;border-radius:4px;background:#333">ADX ↗️</span>
      <span id="tpExtBadge" style="padding:2px 8px;border-radius:4px;background:#333">Ext: —</span>
    </div>
  </div>

  <div class="ind-grid">
    <div class="ind-card"><div class="ind-label">RSI</div><div class="ind-val" id="iRsi">—</div></div>
    <div class="ind-card"><div class="ind-label">ADX</div><div class="ind-val" id="iAdx">—</div></div>
    <div class="ind-card"><div class="ind-label">Stoch K</div><div class="ind-val" id="iStoch">—</div></div>
    <div class="ind-card"><div class="ind-label">ATR</div><div class="ind-val" id="iAtr">—</div></div>
    <div class="ind-card"><div class="ind-label">Volume</div><div class="ind-val" id="iVol">—</div></div>
    <div class="ind-card"><div class="ind-label">Iterasi</div><div class="ind-val" id="iteration">—</div></div>
  </div>

  <div id="positions"><div style="color:var(--gray);text-align:center;padding:20px">Tidak ada posisi terbuka</div></div>

  <div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap">
    <button class="btn btn-red" onclick="manualStop()">⛔ Stop Semua</button>
    <button id="btnResetAll" class="btn btn-gold" onclick="resetAll()" style="display:none">🔄 Reset DRY RUN</button>
  </div>
</div>

<!-- SCANNER TAB -->
<div id="tab-scanner" class="page">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
    <div>
      <span style="font-weight:700;font-size:14px">🔍 Coin Scanner</span>
      <span style="color:var(--gray);font-size:11px;margin-left:8px" id="scannerTime">—</span>
    </div>
    <button class="btn btn-blue" onclick="forceScan()">⚡ Scan Sekarang</button>
  </div>
  <div class="card" style="margin-bottom:12px">
    <div style="font-size:11px;color:var(--gray);margin-bottom:8px">
      Ranking berdasarkan: <b>Skor Sinyal</b> + <b>Momentum</b> + <b>Volatilitas (ATR)</b> + <b>Volume</b>
    </div>
    <div id="scannerContent" style="overflow-x:auto; width:100%">
      <div style="color:var(--gray);text-align:center;padding:20px">Belum ada data scanner. Klik "Scan Sekarang" atau tunggu scan otomatis.</div>
    </div>
  </div>
</div>

<!-- HISTORY TAB -->
<div id="tab-history" class="page">
  <div style="display:flex;gap:8px;align-items:center;margin-bottom:12px;justify-content:space-between">
    <div style="display:flex;gap:8px;align-items:center">
      <input type="date" id="histDate" style="background:var(--card);border:1px solid var(--border);color:var(--text);padding:6px;border-radius:6px;font-size:12px">
      <button class="btn btn-blue" onclick="loadHistory()">Tampilkan</button>
    </div>
    <button class="btn btn-blue" style="background:var(--gold);color:#000" onclick="switchTab('analysis')">📊 Analysis</button>
  </div>
  <div class="grid2" style="margin-bottom:12px">
    <div class="card"><div class="card-label">Total Win</div><div class="card-val pos" id="histTotalWin">$0</div></div>
    <div class="card"><div class="card-label">Total Loss</div><div class="card-val neg" id="histTotalLoss">$0</div></div>
  </div>
  <div class="card"><table><thead><tr><th>Side</th><th>Symbol</th><th>Entry → Exit</th><th style="text-align:right">PnL</th><th>Alasan</th></tr></thead><tbody id="histBody"></tbody></table></div>
</div>

<!-- ANALYSIS TAB -->
<div id="tab-analysis" class="page">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:15px">
    <h2 style="font-size:18px">📊 Analisa Performa</h2>
    <div style="display:flex;gap:8px">
      <button class="btn btn-blue" onclick="switchTab('history')">◀ Kembali</button>
      <button class="btn btn-blue" style="background:var(--gold);color:#000" onclick="loadAnalysis()">🔄 Refresh</button>
    </div>
  </div>
  
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:15px" id="analysisStats">
    <!-- Diisi oleh JS -->
  </div>

  <div class="chart-grid">
    <div class="card" style="padding:16px">
      <div style="font-size:11px;color:var(--gray);margin-bottom:12px;text-align:center;font-weight:600">Rasio Keuntungan ($)</div>
      <div style="height:220px" id="wlContent"><canvas id="winLossChart"></canvas></div>
    </div>
    <div class="card" style="padding:16px">
      <div style="font-size:11px;color:var(--gray);margin-bottom:12px;font-weight:600">Grafik Pertumbuhan PnL Kumulatif ($)</div>
      <div style="height:220px" id="analysisContent"><canvas id="pnlChart"></canvas></div>
    </div>
  </div>

  <div class="card" style="margin-bottom:20px;padding:16px">
    <div style="font-size:11px;color:var(--gray);margin-bottom:12px;font-weight:600">Performa Bulanan ($)</div>
    <div style="height:240px" id="monthlyContent"><canvas id="monthlyChart"></canvas></div>
  </div>
</div>

<!-- SETTINGS TAB -->
<div id="tab-settings" class="page">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:15px">
    <h2 style="font-size:18px">⚙️ Pengaturan Bot</h2>
    <div id="saveStatus" class="save-status"></div>
  </div>
  
  <div class="settings-grid" id="configForm">
    <!-- Diisi oleh JS loadConfig() -->
  </div>

  <div style="height:100px"></div>
  <div class="setting-save-bar">
    <button class="btn btn-blue" style="width:200px;font-size:14px" onclick="saveConfig()">💾 SIMPAN PERUBAHAN</button>
  </div>
</div>
</div>

<script>
let currentTab = 'home';
let countdownInterval = null;
const $ = id => document.getElementById(id);
const fmt = v => (v >= 0 ? '+' : '') + '$' + parseFloat(v).toFixed(2);

function switchTab(tab) {
  currentTab = tab;
  // If analyzing, we don't have a bottom tab for it, keep history highlighted
  const navTab = tab === 'analysis' ? 'history' : tab;
  const tabs = ['home','scanner','history','settings'];
  document.querySelectorAll('.tab').forEach((t,i) => t.classList.toggle('active', tabs[i] === navTab));
  
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  $('tab-'+tab).classList.add('active');
  
  if(tab === 'scanner') loadScanner();
  if(tab === 'history') { $('histDate').value = new Date().toISOString().split('T')[0]; loadHistory(); }
  if(tab === 'settings') loadConfig();
  if(tab === 'analysis') loadAnalysis();
}

async function refresh() {
  if(currentTab !== 'home') return;
  try {
    const d = await(await fetch('/api/state')).json();
    const sym = d.symbol ? d.symbol.replace('_USDT','') : '...';
    $('currPrice').innerHTML = '$' + d.live_price.toFixed(4) + ' <small style="color:var(--gray)">' + sym + '</small>';
    $('modeBadge').textContent = d.is_dry_run ? 'DRY RUN' : 'LIVE';
    $('modeBadge').className   = 'badge ' + (d.is_dry_run ? 'badge-dry' : 'badge-live');
    $('levBadge').textContent  = 'LEV ' + d.leverage + 'X';
    if(d.multi_coin_mode) $('mcBadge').style.display = '';
    
    const errBar = $('apiErrorBar');
    if(d.api_error) {
      errBar.style.display = 'block';
      $('apiErrorMsg').textContent = d.api_error;
    } else {
      errBar.style.display = 'none';
    }

    $('wsDot').className = d.live_price > 0 ? 'dot' : 'dot off';
    $('wsDot').title = d.ws_alive ? 'WebSocket LIVE' : 'REST Fallback';
    $('wsDot').style.background = d.ws_alive ? 'var(--green)' : '#f59e0b';
    if(d.circuit_breaker) {
      $('cbBanner').style.display = 'block';
      $('cbReason').textContent   = d.circuit_reason;
      updateCircuitTimer(d);
    } else {
      $('cbBanner').style.display = 'none';
    }
    $('balance').textContent   = '$' + d.balance.toFixed(2);
    $('pnlSub').textContent    = 'Peak: $' + d.peak_balance.toFixed(2);
    const tp = $('totalPnl'); tp.textContent = fmt(d.total_pnl); tp.className = 'card-val '+(d.total_pnl>=0?'pos':'neg');
    $('dailyPnl').textContent  = 'Daily: ' + fmt(d.daily_pnl);
    $('drawdown').textContent  = 'DD: ' + d.drawdown + '%';
    $('securedTot').textContent = '$' + d.secured_total.toFixed(2);
    $('winRate').textContent   = d.win_rate + '%';
    $('tradeCount').textContent = d.total_trades + ' trades';
    $('iteration').textContent = '#' + d.iteration;

    const sig = d.signal || 'NEUTRAL';
    const box = $('signalBox');
    const sv  = $('sigVal');
    box.className = 'sig-box ' + (sig==='LONG'?'sig-long':sig==='SHORT'?'sig-short':'sig-neutral');
    sv.textContent = sig;
    sv.className   = 'sig-dir ' + sig.toLowerCase();
    $('holdReason').textContent = d.hold_reason || '';
    $('sigDetails').innerHTML = `Bull: <span class="pos">${d.bull_score}</span> Bear: <span class="neg">${d.bear_score}</span> Conf: <b>${d.confidence}%</b>`;
    const symLabel = d.symbol || '—';
    $('symbolLabel').innerHTML = symLabel + (d.multi_coin_mode ? ' <span class="switch-badge">AUTO</span>' : '');

    $('iRsi').textContent  = parseFloat(d.rsi).toFixed(1);
    $('iAtr').textContent  = '$' + d.atr.toFixed(6);
    $('iStoch').textContent = parseFloat(d.stoch_k).toFixed(1);
    $('iVol').textContent  = d.vol_ratio + 'x';
    const adxEl = $('iAdx');
    adxEl.textContent = parseFloat(d.adx).toFixed(1);
    adxEl.style.color  = d.adx >= 30 ? 'var(--green)' : d.adx >= 20 ? 'var(--gold)' : 'var(--red)';

    // 🔥 Trend Power Meter
    const tpVal = d.trend_power || 0;
    $('tpValue').textContent = tpVal;
    $('tpValue').style.color = tpVal >= 70 ? '#00e676' : tpVal >= 55 ? '#ffd740' : tpVal >= 40 ? '#ff9100' : '#ff3b69';
    $('tpBar').style.width = tpVal + '%';
    $('tpBar').style.background = tpVal >= 70 ? 'linear-gradient(90deg, #00c853, #00e676)' :
                                  tpVal >= 55 ? 'linear-gradient(90deg, #ff9100, #ffd740)' :
                                  tpVal >= 40 ? 'linear-gradient(90deg, #ff6d00, #ff9100)' :
                                                'linear-gradient(90deg, #c62828, #ff3b69)';
    const tpLbl = tpVal >= 80 ? '🔥 SANGAT KUAT — SNIPER READY' :
                  tpVal >= 65 ? '💪 KUAT — Siap Entry' :
                  tpVal >= 55 ? '📈 Cukup — Selektif' :
                  tpVal >= 40 ? '🔸 Lemah — Tunggu' : '❌ Sideways';
    $('tpLabel').textContent = tpLbl;
    $('tpLabel').style.color = tpVal >= 55 ? '#00e676' : '#ff9100';

    const adxBadge = $('tpAdxBadge');
    if(d.adx_rising) { adxBadge.textContent = 'ADX ↗️ Menguat'; adxBadge.style.background = '#00e67622'; adxBadge.style.color = '#00e676'; }
    else { adxBadge.textContent = 'ADX ↘️ Melemah'; adxBadge.style.background = '#ff3b6922'; adxBadge.style.color = '#ff3b69'; }

    const extBadge = $('tpExtBadge');
    const extR = (d.ext_ratio || 0).toFixed(1);
    if(d.overextended) { extBadge.textContent = '⛔ Over-ext ' + extR + 'x'; extBadge.style.background = '#ff3b6922'; extBadge.style.color = '#ff3b69'; }
    else { extBadge.textContent = '✅ Ext: ' + extR + 'x ATR'; extBadge.style.background = '#00e67622'; extBadge.style.color = '#00e676'; }

    const pDiv = $('positions');
    if(!d.positions || d.positions.length === 0) {
      pDiv.innerHTML = '<div style="color:var(--gray);text-align:center;padding:20px">Tidak ada posisi terbuka</div>';
    } else {
      pDiv.innerHTML = d.positions.map(p => `
        <div class="pos-card">
          <div class="pos-hdr">
            <span class="badge ${p.side==='LONG'?'badge-long':'badge-short'}">${p.side}</span>
            <span style="font-weight:700">${p.symbol||''}</span>
            <span style="color:var(--gray);font-size:11px">${p.id}</span>
            ${p.flip_count>0?`<span style="color:var(--gold);font-size:10px">FLIP ${p.flip_count}x</span>`:''}
          </div>
          <div class="pos-grid">
            <dt>Entry</dt><dd>$${p.entry_price.toFixed(6)}</dd>
            <dt>Live PnL</dt><dd class="${p.live_pnl>=0?'pos':'neg'}">${p.live_pnl>=0?'+':''}$${p.live_pnl.toFixed(4)}</dd>
            <dt>Stop Loss</dt><dd style="color:var(--red)">$${p.stop_loss.toFixed(6)}</dd>
            <dt>TP1</dt><dd style="color:var(--green)">$${p.take_profit1.toFixed(6)}</dd>
            <dt>TP2</dt><dd style="color:var(--green)">$${p.take_profit2.toFixed(6)}</dd>
            <dt>Trailing</dt><dd>${p.trailing_active?'✅ Aktif':'—'}</dd>
          </div>
        </div>
      `).join('');
    }
    const resetBtn = $('btnResetAll');
    if(resetBtn) resetBtn.style.display = d.is_dry_run ? 'block' : 'none';
  } catch(e) { console.error(e); }
}

function updateCircuitTimer(d) {
  const cooldown = 600;
  if(countdownInterval) clearInterval(countdownInterval);
  const tick = () => {
    let remaining = Math.max(0, cooldown - (Date.now()/1000 - d.circuit_triggered_at));
    if(d.circuit_type === 'MANUAL') remaining = 0;
    const btn = $('btnReset');
    if(remaining > 0) {
      btn.disabled = true;
      btn.innerHTML = `Tunggu ${Math.floor(remaining/60)}m ${Math.floor(remaining%60)}s`;
    } else {
      btn.disabled = false;
      btn.innerHTML = '🚀 Mulai Trade Lagi';
      clearInterval(countdownInterval);
    }
  };
  countdownInterval = setInterval(tick, 1000);
  tick();
}

async function loadScanner() {
  try {
    const d = await(await fetch('/api/scanner')).json();
    const ts = d.last_scan > 0 ? new Date(d.last_scan*1000).toLocaleTimeString('id') : 'Belum scan';
    $('scannerTime').textContent = 'Update: ' + ts;
    if(!d.results || d.results.length === 0) {
      $('scannerContent').innerHTML = '<div style="color:var(--gray);text-align:center;padding:20px">Belum ada koin yang memenuhi kriteria filter (ADX/ATR%).<br><small>Coba kurangi ambang batas di CONFIG.</small></div>';
      return;
    }
    const rows = d.results.map((r,i) => {
      const isActive = r.symbol === d.active;
      const sigColor = r.signal==='LONG'?'var(--green)':r.signal==='SHORT'?'var(--red)':'var(--gray)';
      const momColor = r.momentum>0?'var(--green)':r.momentum<0?'var(--red)':'var(--gray)';
      const adxColor = r.adx >= 25 ? 'var(--green)' : r.adx >= 20 ? 'var(--gold)' : 'var(--red)';
      return `<tr style="${isActive?'background:var(--blue)15;':''}" class="scanner-table">
        <td>#${i+1}</td>
        <td style="white-space:nowrap">${r.symbol.replace('_USDT','')} ${isActive?'<span class="switch-badge" style="margin-left:4px">AKTIF</span>':''}</td>
        <td style="color:${sigColor};font-weight:700">${r.signal}</td>
        <td style="text-align:center">${r.raw_score}/15</td>
        <td style="text-align:right;color:${momColor}">${r.momentum>0?'+':''}${r.momentum.toFixed(1)}</td>
        <td style="text-align:right">${r.composite.toFixed(1)}</td>
        <td style="text-align:right">${r.atr_pct.toFixed(2)}%</td>
        <td style="text-align:right;color:${adxColor};font-weight:700">${r.adx.toFixed(0)}</td>
      </tr>`;
    }).join('');
    $('scannerContent').innerHTML = `<table>
      <thead><tr>
        <th>#</th>
        <th>Koin</th>
        <th>Sinyal</th>
        <th style="text-align:center">Skor</th>
        <th style="text-align:right">Momentum</th>
        <th style="text-align:right">Composite</th>
        <th style="text-align:right">ATR%</th>
        <th style="text-align:right">ADX</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>`;
  } catch(e) { console.error(e); }
}

async function forceScan() {
  await fetch('/api/force_scan', {method:'POST'});
  setTimeout(loadScanner, 3000);
}

async function loadHistory() {
  const date = $('histDate').value;
  const r = await fetch('/api/history?date=' + date);
  const d = await r.json();
  $('histTotalWin').textContent  = '+$' + parseFloat(d.total_win||0).toFixed(2);
  $('histTotalLoss').textContent = '$' + parseFloat(d.total_loss||0).toFixed(2);
  const body = $('histBody');
  if(!d.history || d.history.length === 0) {
    body.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--gray);padding:20px">Tidak ada riwayat</td></tr>';
    return;
  }
  body.innerHTML = d.history.map(t => `
    <tr>
      <td><span class="${t.side==='LONG'?'badge-long':'badge-short'}">${t.side}</span></td>
      <td style="color:var(--blue);font-size:11px">${(t.symbol||'').replace('_USDT','')}</td>
      <td style="font-size:11px">$${parseFloat(t.entry_price).toFixed(4)} → $${parseFloat(t.exit_price).toFixed(4)}</td>
      <td style="text-align:right;font-weight:700" class="${t.pnl>=0?'pos':'neg'}">${parseFloat(t.pnl)>=0?'+':''}$${parseFloat(t.pnl).toFixed(4)}</td>
      <td style="font-size:10px;color:var(--gray)">${t.close_reason||''}</td>
    </tr>
  `).join('');
}

async function loadAnalysis() {
  try {
    const r = await fetch('/api/history');
    const d = await r.json();
    const trades = d.history || [];
    
    if(trades.length === 0) {
      const emptyStateHTML = '<div style="color:var(--gray);text-align:center;display:flex;align-items:center;justify-content:center;height:100%;font-size:12px">Belum ada riwayat.</div>';
      $('analysisContent').innerHTML = emptyStateHTML;
      $('wlContent').innerHTML = emptyStateHTML;
      $('monthlyContent').innerHTML = emptyStateHTML;
      $('analysisStats').innerHTML = `
        <div class="card"><div class="card-label">Win Rate</div><div class="card-val pos">0%</div></div>
        <div class="card"><div class="card-label">Total Trades</div><div class="card-val">0</div></div>
      `;
      return;
    }
    
    $('analysisContent').innerHTML = '<canvas id="pnlChart"></canvas>';
    $('wlContent').innerHTML       = '<canvas id="winLossChart"></canvas>';
    $('monthlyContent').innerHTML  = '<canvas id="monthlyChart"></canvas>';
    
    let curve = [0];
    let labels = ['Start'];
    let currentPnl = 0;
    
    let wins = 0;
    let losses = 0;
    let grossWin = 0;
    let grossLoss = 0;
    
    let maxDrawdown = 0;
    let peakPnl = 0;
    let maxWin = 0;
    let maxLoss = 0;
    
    let monthlyData = {};
    
    trades.slice().reverse().forEach((t, i) => {
      const pnl = parseFloat(t.pnl || 0);
      currentPnl += pnl;
      
      if (pnl > 0) { wins++; grossWin += pnl; }
      else if (pnl < 0) { losses++; grossLoss += Math.abs(pnl); }
      
      if (pnl > maxWin) maxWin = pnl;
      if (pnl < maxLoss) maxLoss = pnl;
      if (currentPnl > peakPnl) peakPnl = currentPnl;
      
      const dd = peakPnl - currentPnl;
      if (dd > maxDrawdown) maxDrawdown = dd;
      
      curve.push(currentPnl);
      labels.push('T' + (i+1));
      
      const ts = t.timestamp || (Date.now()/1000);
      const dObj = new Date(ts * 1000);
      const monStr = dObj.getFullYear() + "-" + String(dObj.getMonth() + 1).padStart(2, '0');
      if(!monthlyData[monStr]) monthlyData[monStr] = 0;
      monthlyData[monStr] += pnl;
    });
    
    const winRate = trades.length > 0 ? (wins / trades.length * 100).toFixed(1) : 0;
    const wrColor = winRate >= 50 ? 'pos' : 'neg';
    
    $('analysisStats').innerHTML = `
      <div class="card"><div class="card-label">Win Rate (${wins}W / ${losses}L)</div><div class="card-val ${wrColor}">${winRate}%</div></div>
      <div class="card"><div class="card-label">Net Profit</div><div class="card-val ${currentPnl>=0?'pos':'neg'}">${fmt(currentPnl)}</div></div>
      <div class="card"><div class="card-label">Best Trade</div><div class="card-val pos">${fmt(maxWin)}</div></div>
      <div class="card"><div class="card-label">Max Drawdown</div><div class="card-val neg">-$${maxDrawdown.toFixed(2)}</div></div>
    `;
    
    Chart.defaults.color = '#8b949e';
    
    // PNL CHART
    const ctx = document.getElementById('pnlChart').getContext('2d');
    if(window.pnlChartInst) window.pnlChartInst.destroy();
    window.pnlChartInst = new Chart(ctx, {
      type: 'line',
      data: {
        labels: labels,
        datasets: [{
          label: 'Total PnL ($)',
          data: curve,
          borderWidth: 2, fill: true,
          backgroundColor: function(context) {
            const chart = context.chart;
            const {ctx, chartArea} = chart;
            if (!chartArea) return null;
            let gradient = ctx.createLinearGradient(0, chartArea.bottom, 0, chartArea.top);
            gradient.addColorStop(0, 'rgba(0, 230, 118, 0.01)');
            gradient.addColorStop(1, 'rgba(0, 230, 118, 0.15)');
            return gradient;
          },
          segment: { borderColor: ctx => ctx.p0.parsed.y <= ctx.p1.parsed.y ? '#00e676' : '#ff3b69' },
          tension: 0.2, pointRadius: Math.max(1, 5 - Math.floor(trades.length/10)), pointBackgroundColor: '#161b22',
          pointBorderColor: ctx => {
            if(ctx.dataIndex === 0) return '#8b949e';
            return curve[ctx.dataIndex] >= curve[ctx.dataIndex-1] ? '#00e676' : '#ff3b69';
          }
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { mode: 'index', intersect: false } },
        scales: {
          x: { ticks: { font: {size: 10} }, grid: { display: false } },
          y: { ticks: { font: {size: 10} }, grid: { color: 'rgba(255,255,255,0.05)' } }
        }, interaction: { mode: 'nearest', axis: 'x', intersect: false }
      }
    });

    // WIN/LOSS DOUGHNUT
    const wlCtx = document.getElementById('winLossChart').getContext('2d');
    if(window.wlChartInst) window.wlChartInst.destroy();
    window.wlChartInst = new Chart(wlCtx, {
      type: 'doughnut',
      data: {
        labels: ['Win ($' + grossWin.toFixed(2) + ')', 'Loss ($' + grossLoss.toFixed(2) + ')'],
        datasets: [{
          data: [grossWin, grossLoss],
          backgroundColor: ['#00e676', '#ff3b69'],
          borderWidth: 0, hoverOffset: 4
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false, cutout: '65%',
        plugins: {
          legend: { position: 'bottom', labels: { boxWidth: 12, padding: 15, font: {size: 11} } }
        }
      }
    });

    // MONTHLY BAR CHART
    const moCtx = document.getElementById('monthlyChart').getContext('2d');
    if(window.moChartInst) window.moChartInst.destroy();
    const moLabels = Object.keys(monthlyData).sort();
    const moData = moLabels.map(m => monthlyData[m]);
    const moColors = moData.map(v => v >= 0 ? '#00e676' : '#ff3b69');
    
    window.moChartInst = new Chart(moCtx, {
      type: 'bar',
      data: {
        labels: moLabels,
        datasets: [{
          label: 'Net PnL ($)',
          data: moData,
          backgroundColor: moColors,
          borderRadius: 4
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { font: {size: 11} }, grid: { display: false } },
          y: { ticks: { font: {size: 11} }, grid: { color: 'rgba(255,255,255,0.05)' } }
        }
      }
    });

  } catch(e) { console.error(e); }
}

function showToast(msg, type='success') {
  const t = $('toast');
  t.textContent = msg;
  t.className = type === 'success' ? 'toast-success' : 'toast-error';
  t.style.display = 'block';
  setTimeout(() => { t.style.display = 'none'; }, 3000);
}

async function resetCircuit() { 
  await fetch('/api/reset_circuit',{method:'POST'}); 
  showToast('Sistem dinyalakan kembali!'); 
}
async function manualStop() { 
  if(!confirm('Yakin ingin menghentikan semua trade?')) return;
  await fetch('/api/manual_stop',{method:'POST'}); 
  showToast('Bot berhasil dihentikan!', 'error'); 
}
async function resetAll() { 
  if(!confirm('PERINGATAN: Ini akan mereset saldo ke $100 & menghapus jurnal. Lanjutkan?')) return;
  await fetch('/api/reset_all',{method:'POST'}); 
  showToast('Semua data berhasil di-reset!'); 
}

async function loadConfig() {
  try {
    const res = await fetch('/api/config');
    const cfg = await res.json();
    const form = $('configForm');
    
    const groups = {
      "Kredensial API": ["MEXC_API_KEY", "MEXC_API_SECRET"],
      "Umum": ["SYMBOL","PRIMARY_TF","CONFIRM_TF","LEVERAGE","DRY_RUN","VIRTUAL_BALANCE","POLL_INTERVAL"],
      "Strategi": ["MULTI_COIN_MODE","AUTO_SWITCH_COIN","SWITCH_MIN_ADVANTAGE","SWITCH_IDLE_MAX_SEC","EXIT_ON_SIGNAL_FLIP","REQUIRE_MTF_CONFIRM","MIN_VOL_RATIO","MIN_BULL_SCORE","MIN_BEAR_SCORE","PRESET_MODE"],
      "Keamanan Profit": ["ENABLE_AUTO_SECURE", "SECURE_PROFIT_PCT", "MIN_SECURE_TRANSFER"],
      "Trend Power": ["USE_TREND_POWER","MIN_TREND_POWER","MIN_TREND_POWER_EARLY","MAX_EXTENSION_ATR","ADX_RISING_REQUIRED","DYNAMIC_SL_TP"],
      "Risiko": ["RISK_PER_TRADE","MAX_MARGIN_PCT","MAX_OPEN_TRADES","MIN_RR_RATIO","MAX_DAILY_LOSS_PCT","MAX_DRAWDOWN_PCT","LOSS_COOLDOWN_SEC","MAX_ATR_PCT_ENTRY"],
      "TP & SL (ATR)": ["ATR_SL_MULT","ATR_TP1_MULT","ATR_TP2_MULT","ATR_TP3_MULT","TP1_PARTIAL_CLOSE","TP1_CLOSE_PCT","MAX_ATR_DISTANCE_MULT","MIN_TP_DISTANCE_PCT"],
      "Trailing & BE": ["USE_TRAILING_STOP","TRAIL_ACTIVATION_PCT","TRAIL_DISTANCE_PCT","USE_BE_FILTER","BE_ACTIVATION_PCT","BE_FEE_BUFFER_PCT","GRACE_PERIOD_SEC"],
      "Scanner": ["SCAN_TOP_N","SCAN_INTERVAL","SCAN_MIN_VOLUME","SCAN_MIN_ADX","SCAN_MIN_ATR_PCT","EARLY_ENTRY_MODE","EARLY_ENTRY_SCORE"],
      "Indikator": ["USE_ADX_FILTER","ADX_MIN_THRESHOLD","RSI_PERIOD","RSI_OVERSOLD","RSI_OVERBOUGHT","EMA_FAST","EMA_SLOW","EMA_TREND","EMA_LONG","MACD_FAST","MACD_SLOW","ATR_PERIOD","STOCH_K","STOCH_D"]
    };

    let html = '';
    const dropdowns = {
      "PRIMARY_TF": ["1m","5m","15m","30m","1h","4h","8h","1d"],
      "CONFIRM_TF": ["1m","5m","15m","30m","1h","4h","8h","1d"],
      "PRESET_MODE": ["SCALPER","TREND","AGGRESSIVE","SNIPER","CUSTOM"]
    };

    window.PRESETS = {

      // ─── SCALPER ─────────────────────────────────────────────────────────────
      // Banyak trade kecil di 1m, profit cepat, SL ketat, modal terjaga
      "SCALPER": {
        "PRIMARY_TF": "1m", "CONFIRM_TF": "5m",
        "EMA_FAST": 8, "EMA_SLOW": 21, "EMA_TREND": 50, "EMA_LONG": 200,
        "RSI_OVERSOLD": 35, "RSI_OVERBOUGHT": 65,
        "MIN_BULL_SCORE": 6, "MIN_BEAR_SCORE": 6,
        "SCAN_MIN_ADX": 15, "ADX_MIN_THRESHOLD": 15,
        "USE_TREND_POWER": true, "MIN_TREND_POWER": 45, "MIN_TREND_POWER_EARLY": 30,
        "MAX_EXTENSION_ATR": 3.0, "ADX_RISING_REQUIRED": false, "DYNAMIC_SL_TP": false,
        "ATR_SL_MULT": 0.8, "ATR_TP1_MULT": 1.0, "ATR_TP2_MULT": 1.8, "ATR_TP3_MULT": 3.0,
        "RISK_PER_TRADE": 0.15, "LEVERAGE": 20, "MAX_MARGIN_PCT": 0.4,
        "MAX_DAILY_LOSS_PCT": 0.15, "MAX_DRAWDOWN_PCT": 0.25,
        "USE_TRAILING_STOP": true, "TRAIL_ACTIVATION_PCT": 0.008, "TRAIL_DISTANCE_PCT": 0.004,
        "USE_BE_FILTER": true, "BE_ACTIVATION_PCT": 0.005, "GRACE_PERIOD_SEC": 10,
        "LOSS_COOLDOWN_SEC": 60, "SWITCH_MIN_ADVANTAGE": 3, "SWITCH_IDLE_MAX_SEC": 120,
        "EARLY_ENTRY_MODE": true, "EARLY_ENTRY_SCORE": 5, "EARLY_MOMENTUM_MIN": 2,
        "SCAN_INTERVAL": 60, "MIN_VOL_RATIO": 0.8,
        "REQUIRE_MTF_CONFIRM": true, "EXIT_ON_SIGNAL_FLIP": true,
        "TP1_PARTIAL_CLOSE": true, "TP1_CLOSE_PCT": 60
      },

      // ─── TREND ───────────────────────────────────────────────────────────────
      // Ikuti tren jelas di TF lebih tinggi, sedikit trade tapi kualitas tinggi
      "TREND": {
        "PRIMARY_TF": "15m", "CONFIRM_TF": "1h",
        "EMA_FAST": 13, "EMA_SLOW": 34, "EMA_TREND": 89, "EMA_LONG": 200,
        "RSI_OVERSOLD": 40, "RSI_OVERBOUGHT": 60,
        "MIN_BULL_SCORE": 9, "MIN_BEAR_SCORE": 9,
        "SCAN_MIN_ADX": 22, "ADX_MIN_THRESHOLD": 22,
        "USE_TREND_POWER": true, "MIN_TREND_POWER": 65, "MIN_TREND_POWER_EARLY": 50,
        "MAX_EXTENSION_ATR": 4.0, "ADX_RISING_REQUIRED": true, "DYNAMIC_SL_TP": true,
        "ATR_SL_MULT": 2.0, "ATR_TP1_MULT": 3.0, "ATR_TP2_MULT": 5.0, "ATR_TP3_MULT": 8.0,
        "RISK_PER_TRADE": 0.1, "LEVERAGE": 10, "MAX_MARGIN_PCT": 0.3,
        "MAX_DAILY_LOSS_PCT": 0.1, "MAX_DRAWDOWN_PCT": 0.2,
        "USE_TRAILING_STOP": true, "TRAIL_ACTIVATION_PCT": 0.025, "TRAIL_DISTANCE_PCT": 0.01,
        "USE_BE_FILTER": true, "BE_ACTIVATION_PCT": 0.015, "GRACE_PERIOD_SEC": 30,
        "LOSS_COOLDOWN_SEC": 300, "SWITCH_MIN_ADVANTAGE": 5, "SWITCH_IDLE_MAX_SEC": 300,
        "EARLY_ENTRY_MODE": false, "EARLY_ENTRY_SCORE": 7, "EARLY_MOMENTUM_MIN": 3,
        "SCAN_INTERVAL": 180, "MIN_VOL_RATIO": 1.2,
        "REQUIRE_MTF_CONFIRM": true, "EXIT_ON_SIGNAL_FLIP": true,
        "TP1_PARTIAL_CLOSE": true, "TP1_CLOSE_PCT": 40
      },

      // ─── AGGRESSIVE ──────────────────────────────────────────────────────────
      // Entry lebih sering, syarat longgar, risiko lebih tinggi — untuk pasar volatile
      "AGGRESSIVE": {
        "PRIMARY_TF": "1m", "CONFIRM_TF": "5m",
        "EMA_FAST": 5, "EMA_SLOW": 13, "EMA_TREND": 34, "EMA_LONG": 100,
        "RSI_OVERSOLD": 32, "RSI_OVERBOUGHT": 68,
        "MIN_BULL_SCORE": 5, "MIN_BEAR_SCORE": 5,
        "SCAN_MIN_ADX": 12, "ADX_MIN_THRESHOLD": 12,
        "USE_TREND_POWER": true, "MIN_TREND_POWER": 35, "MIN_TREND_POWER_EARLY": 25,
        "MAX_EXTENSION_ATR": 2.5, "ADX_RISING_REQUIRED": false, "DYNAMIC_SL_TP": false,
        "ATR_SL_MULT": 0.7, "ATR_TP1_MULT": 0.9, "ATR_TP2_MULT": 1.6, "ATR_TP3_MULT": 2.8,
        "RISK_PER_TRADE": 0.2, "LEVERAGE": 30, "MAX_MARGIN_PCT": 0.5,
        "MAX_DAILY_LOSS_PCT": 0.2, "MAX_DRAWDOWN_PCT": 0.35,
        "USE_TRAILING_STOP": true, "TRAIL_ACTIVATION_PCT": 0.006, "TRAIL_DISTANCE_PCT": 0.003,
        "USE_BE_FILTER": true, "BE_ACTIVATION_PCT": 0.004, "GRACE_PERIOD_SEC": 5,
        "LOSS_COOLDOWN_SEC": 30, "SWITCH_MIN_ADVANTAGE": 2, "SWITCH_IDLE_MAX_SEC": 60,
        "EARLY_ENTRY_MODE": true, "EARLY_ENTRY_SCORE": 4, "EARLY_MOMENTUM_MIN": 1,
        "SCAN_INTERVAL": 30, "MIN_VOL_RATIO": 0.6,
        "REQUIRE_MTF_CONFIRM": false, "EXIT_ON_SIGNAL_FLIP": true,
        "TP1_PARTIAL_CLOSE": true, "TP1_CLOSE_PCT": 70
      },

      // ─── SNIPER ──────────────────────────────────────────────────────────────
      // Entry sangat selektif, tunggu konfirmasi penuh, R:R terbaik
      "SNIPER": {
        "PRIMARY_TF": "5m", "CONFIRM_TF": "15m",
        "EMA_FAST": 13, "EMA_SLOW": 34, "EMA_TREND": 89, "EMA_LONG": 200,
        "RSI_OVERSOLD": 38, "RSI_OVERBOUGHT": 62,
        "MIN_BULL_SCORE": 7, "MIN_BEAR_SCORE": 7,
        "SCAN_MIN_ADX": 20, "ADX_MIN_THRESHOLD": 20,
        "USE_TREND_POWER": true, "MIN_TREND_POWER": 60, "MIN_TREND_POWER_EARLY": 40,
        "MAX_EXTENSION_ATR": 5.0, "ADX_RISING_REQUIRED": true, "DYNAMIC_SL_TP": true,
        "MAX_ATR_PCT_ENTRY": 15.0, "MAX_ATR_DISTANCE_MULT": 10.0,
        "ATR_SL_MULT": 1.5, "ATR_TP1_MULT": 2.5, "ATR_TP2_MULT": 4.0, "ATR_TP3_MULT": 6.0,
        "RISK_PER_TRADE": 0.1, "LEVERAGE": 20, "MAX_MARGIN_PCT": 0.35,
        "MAX_DAILY_LOSS_PCT": 0.1, "MAX_DRAWDOWN_PCT": 0.2,
        "USE_TRAILING_STOP": true, "TRAIL_ACTIVATION_PCT": 0.015, "TRAIL_DISTANCE_PCT": 0.006,
        "USE_BE_FILTER": true, "BE_ACTIVATION_PCT": 0.008, "GRACE_PERIOD_SEC": 20,
        "LOSS_COOLDOWN_SEC": 120, "SWITCH_MIN_ADVANTAGE": 3, "SWITCH_IDLE_MAX_SEC": 180,
        "EARLY_ENTRY_MODE": true, "EARLY_ENTRY_SCORE": 6, "EARLY_MOMENTUM_MIN": 3,
        "SCAN_INTERVAL": 60, "MIN_VOL_RATIO": 1.0,
        "REQUIRE_MTF_CONFIRM": true, "EXIT_ON_SIGNAL_FLIP": true,
        "TP1_PARTIAL_CLOSE": true, "TP1_CLOSE_PCT": 50
      }

    };
    
    window.applyPreset = function(mode) {
      if (mode === "CUSTOM" || !window.PRESETS[mode]) return;
      const p = window.PRESETS[mode];
      for (const [k, v] of Object.entries(p)) {
        const el = $('cfg_' + k);
        if (!el) continue;
        if (el.type === 'checkbox') el.checked = v;
        else el.value = v;
      }
    };

    for(const [gName, keys] of Object.entries(groups)) {
      html += `<div class="setting-group"><h3>${gName}</h3>`;
      keys.forEach(k => {
        if(cfg[k] === undefined) return;
        const val = cfg[k];
        html += `<div class="setting-item">
          <label>${k.replace(/_/g,' ')}</label>`;
        
        if(dropdowns[k]) {
          const onchangeAttr = k === 'PRESET_MODE' ? ' onchange="applyPreset(this.value)"' : '';
          html += `<select id="cfg_${k}"${onchangeAttr}>`;
          dropdowns[k].forEach(opt => {
            html += `<option value="${opt}" ${opt===val?'selected':''}>${opt}</option>`;
          });
          html += `</select>`;
        } else if(typeof val === 'boolean') {
          html += `<label class="toggle-switch">
            <input type="checkbox" id="cfg_${k}" ${val?'checked':''}>
            <span class="slider"></span>
          </label>`;
        } else if(Array.isArray(val)) {
          html += `<input type="text" id="cfg_${k}" value="${val.join(',')}" style="width:120px">`;
        } else {
          html += `<input type="${typeof val==='number'?'number':'text'}" id="cfg_${k}" value="${val}" ${typeof val==='number'?'step="any"':''}>`;
        }
        html += `</div>`;
      });
      html += `</div>`;
    }
    form.innerHTML = html;
  } catch(e) { console.error(e); }
}

async function saveConfig() {
  const status = $('saveStatus');
  status.innerHTML = '<span style="color:var(--gold)">⏳ Menyimpan...</span>';
  
  try {
    // Get all current config to preserve unedited keys
    const current = await(await fetch('/api/config')).json();
    const updated = { ...current };

    // Get values from form (input and select)
    document.querySelectorAll('#configForm input, #configForm select').forEach(input => {
      const key = input.id.replace('cfg_', '');
      if(input.type === 'checkbox') {
        updated[key] = input.checked;
      } else {
        let val = input.value;
        // Check if original was an array
        if(Array.isArray(current[key])) {
          updated[key] = val.split(',').map(s => s.trim()).filter(s => s !== '');
        } else {
          if(!isNaN(val) && val.trim() !== '' && typeof current[key] === 'number') {
            updated[key] = parseFloat(val);
          } else {
            updated[key] = val;
          }
        }
      }
    });

    const res = await fetch('/api/config', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(updated)
    });
    
    const result = await res.json();
    if(result.success) {
      status.innerHTML = '<span style="color:var(--green)">✅ Berhasil disimpan! Bot di-update otomatis.</span>';
      setTimeout(() => { status.innerHTML = ''; }, 3000);
    } else {
      status.innerHTML = `<span style="color:var(--red)">❌ Gagal: ${result.error}</span>`;
    }
  } catch(e) {
    status.innerHTML = `<span style="color:var(--red)">❌ Error: ${e.message}</span>`;
  }
}

setInterval(refresh, 3000);
refresh();
</script>
</body></html>
"""

# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MEXC Pro Trading Bot V3")
    parser.add_argument("--dashboard", action="store_true", help="Aktifkan web dashboard di port 5000")
    parser.add_argument("--live",      action="store_true", help="Live trading mode (override DRY_RUN)")
    parser.add_argument("--noscan",    action="store_true", help="Matikan multi-coin scanner")
    args = parser.parse_args()

    if args.live:
        CONFIG["DRY_RUN"] = False
        log.warning("⚠️  LIVE TRADING MODE — Uang nyata digunakan!")
    if args.noscan:
        CONFIG["MULTI_COIN_MODE"] = False
        log.info("Multi-coin scanner dimatikan via flag --noscan")

    bot = TradingBotV3(run_dashboard=args.dashboard)
    bot.run()
