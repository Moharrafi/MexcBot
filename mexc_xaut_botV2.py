"""
XAUT/USDT PRO TRADING BOT - MEXC FUTURES V2
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
import websocket
import pandas as pd
import pandas_ta_remake as ta
import math
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════
#  KONFIGURASI LENGKAP
# ══════════════════════════════════════════════════════════════

CONFIG = {
    # ─── Pair & Timeframe ───────────────────────────────────
    "SYMBOL":               "PIPPIN_USDT",
    "PRIMARY_TF":           "1m",       # [SCALPING] timeframe entry lebih cepat
    "CONFIRM_TF":           "3m",       # [SCALPING] konfirmasi ringan (dari 15m → 3m)
    "CANDLE_LIMIT":         300,

    # ─── Mode ───────────────────────────────────────────────
    "DRY_RUN":              True,
    "VIRTUAL_BALANCE":      100.0,

    # ─── Manajemen Risiko ───────────────────────────────────
    "RISK_PER_TRADE":       0.05,       # 5% risiko per trade — lebih kecil karena frekuensi tinggi
    "LEVERAGE":             20,         # 20x — sedikit dikurangi untuk jaga modal di trade frekuensi tinggi
    "MAX_MARGIN_PCT":       0.15,       # 15% margin per trade — batasi eksposur per posisi
    "USE_KELLY":            False,
    "ATR_SL_MULT":          1.2,        # SL sedikit lebih longgar agar tidak kena noise 1m
    "ATR_TP1_MULT":         1.0,        # TP1 sangat dekat — scalping cepat ambil profit
    "ATR_TP2_MULT":         2.0,        # TP2 moderat
    "ATR_TP3_MULT":         3.5,        # TP3 untuk momentum kuat
    "MAX_OPEN_TRADES":      3,          # Boleh 3 posisi sekaligus untuk frekuensi tinggi
    "MIN_RR_RATIO":         0.8,        # RR sedikit longgar — scalping prioritas hit rate bukan RR besar
    "MAX_DAILY_LOSS_PCT":   0.15,       # Stop harian di -15% — proteksi ketat karena frekuensi tinggi
    "MAX_DRAWDOWN_PCT":     0.30,       # Max drawdown 30% sebelum circuit breaker
    "MARGIN_MODE":          "ISOLATED",
    "ENABLE_SIGNAL_SCANNER": True,
    "EXIT_ON_SIGNAL_FLIP":  True,       # Tetap aktif — penting untuk scalping cepat
    "EXIT_MIN_BULL_SCORE":  2,          # Exit LONG jika skor bull turun di bawah 2 (Early Exit)
    "EXIT_MIN_BEAR_SCORE":  2,          # Exit SHORT jika skor bear turun di bawah 2 (Early Exit)
    "AUTO_REVERSE_ON_FLIP": False,

    # ─── Break-Even & Partial Close ───
    "USE_BE_FILTER":        True,
    "BE_ACTIVATION_PCT":    0.003,      # Pindah BE lebih cepat — setelah profit 0.3% (dari 0.5%)
    "BE_SAFEGUARD_PCT":     0.0002,     # Offset supaya BE tidak jadi minus karena harga loncat (slippage)
    "USE_FAST_RISK_REDUCTION": True,    # Kurangi resiko 50% lebih awal (squeeze minus)
    "FAST_REDUCTION_PCT":   0.001,      # Profit 0.1% untuk aktifkan pengurangan resiko
    "ALLOW_FAST_REENTRY":   True,       # Boleh entry lagi di candle yang sama jika posisi kosong
    "TP1_PARTIAL_CLOSE":    True,
    "TP1_CLOSE_PCT":        50,         # Tutup 50% di TP1
    "TP2_PARTIAL_CLOSE":    True,
    "TP2_CLOSE_PCT":        50,         # Tutup 50% dari SISA di TP2 (sisakan 25% total untuk TP3)

    # ─── Filter Probabilitas TP ─────────────────────────────
    "USE_PROBABILITY_FILTER": True,
    "MIN_VOL_RATIO":          0.7,      # Volume cukup 0.7x rata-rata — lebih longgar agar lebih banyak entry
    "MAX_ATR_DISTANCE_MULT":  3.0,      # Toleransi TP lebih jauh sedikit
    "MIN_TP_DISTANCE_PCT":    0.08,     # Minimal 0.08% gerakan — sangat kecil agar sering entry

    # ─── Trailing Stop ──────────────────────────────────────
    "USE_TRAILING_STOP":    True,
    "TRAIL_ACTIVATION_PCT": 0.005,      # Trailing aktif lebih cepat — setelah profit 0.5%
    "TRAIL_DISTANCE_PCT":   0.003,      # Jarak trailing 0.3% — ketat untuk scalping

    # ─── Indikator ──────────────────────────────────────────
    "RSI_PERIOD":           14,
    "RSI_OVERSOLD":         42,         # Lebih longgar (dari 38 → 42) — lebih sering trigger oversold
    "RSI_OVERBOUGHT":       58,         # Lebih longgar (dari 62 → 58) — lebih sering trigger overbought
    "EMA_FAST":             5,          # Lebih responsif (dari 9 → 5) untuk scalping 1m
    "EMA_SLOW":             13,         # Lebih responsif (dari 21 → 13)
    "EMA_TREND":            34,         # Trend lebih pendek (dari 50 → 34) untuk scalping
    "EMA_LONG":             100,        # Kurangi dari 200 → 100 agar tidak terlalu ketat di 1m
    "MACD_FAST":            6,          # MACD lebih cepat (dari 12 → 6) untuk 1m
    "MACD_SLOW":            13,         # (dari 26 → 13)
    "MACD_SIGNAL":          4,          # (dari 9 → 4)
    "BB_PERIOD":            14,         # BB lebih pendek (dari 20 → 14) lebih sensitif
    "BB_STD":               2.0,
    "ATR_PERIOD":           10,         # ATR lebih responsif (dari 14 → 10) untuk volatilitas 1m
    "STOCH_K":              9,          # Stoch lebih cepat (dari 14 → 9)
    "STOCH_D":              3,
    "STOCH_SMOOTH":         3,
    "STOCH_OVERSOLD":       30,         # Longgarkan (dari 25 → 30) — lebih sering trigger
    "STOCH_OVERBOUGHT":     70,         # Longgarkan (dari 75 → 70)

    # ─── Profit Securing (Wealth Protection) ─────────────────
    "ENABLE_AUTO_SECURE":   True,       # Pindahkan profit ke Spot otomatis
    "SECURE_PROFIT_PCT":    50,         # % profit yang dipindahkan (misal 50%)
    "MIN_SECURE_TRANSFER":  1.0,        # Min $1 baru pindah (limit MEXC)

    # ─── Sinyal Threshold ───────────────────────────────
    "MIN_BULL_SCORE":       6,          # Minimum score untuk sinyal BULL (Naik ke 6 agar lebih selektif)
    "MIN_BEAR_SCORE":       6,          # Minimum score untuk sinyal BEAR

    # ─── Anti-Sideways (ADX) ────────────────────────────
    "USE_ADX_FILTER":       True,       # Tetap aktif
    "ADX_MIN_THRESHOLD":    20,         # Kembali ke 20 (dari 15) untuk deteksi sideways lebih akurat
    "ADX_PERIOD":           14,

    # ─── MTF Confirmation ───────────────────────────────
    "REQUIRE_MTF_CONFIRM":  False,      # Dinonaktifkan sesuai permintaan (Mode Agresif)

    # ─── Session Filter ─────────────────────────────────────
    "USE_SESSION_FILTER":   False,                                                                                                                                  
    # Jam terbaik (UTC): London 07-16, NY 13-22, Overlap 13-16
    "ALLOWED_HOURS_UTC":    list(range(0, 24)),   # 24 jam penuh
    "BLOCK_FRIDAY_CLOSE":   False,
    "BLOCK_SUNDAY_OPEN":    False,

    # ─── News Blackout ──────────────────────────────────────
    # Format: "MM-DD HH:MM UTC" — akan diblokir ±30 menit
    "NEWS_BLACKOUT":        [],         # isi manual, contoh: ["04-10 18:30"]

    # ─── WebSocket ──────────────────────────────────────────
    "WS_RECONNECT_DELAY":   5,          # detik sebelum reconnect                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
    "WS_MAX_RECONNECTS":    10,

    # ─── Loop ───────────────────────────────────────────────
    "POLL_INTERVAL":        5,          # [SCALPING] Lebih responsif (dari 15 → 5 detik)
    "PRICE_UPDATE_INTERVAL": 3,         # [SCALPING] Cek trailing lebih sering (dari 5 → 3 detik)
    "FEE_RATE":             0.0006,     # Estimasi fee MEXC (0.06% taker)

    # ─── Logging & Persistence ──────────────────────────────
    "LOG_FILE":             "xaut_bot.log",
    "LOG_LEVEL":            "INFO",
    "STATE_FILE":           "bot_state.json",
    "JOURNAL_FILE":         "trade_journal.csv",

    # ─── Dashboard ──────────────────────────────────────────
    "DASHBOARD_HOST":       "0.0.0.0",
    "DASHBOARD_PORT":       5000,
    "PRESET_MODE":          "CUSTOM",

    # ─── 🚀 POWER UPGRADES ─────────────────────────────────
    # 1. Loss Streak Cooldown (Anti-Revenge Trading)
    "USE_LOSS_COOLDOWN":     True,
    "LOSS_COOLDOWN_AFTER":   3,          # Setelah N loss berturut, naikkan threshold
    "LOSS_COOLDOWN_BOOST":   1,          # Tambah +N ke MIN_BULL/BEAR_SCORE saat cooldown

    # 2. Confidence-Scaled Sizing (Sinyal kuat = posisi lebih besar)
    "USE_CONFIDENCE_SIZING": True,
    "CONF_SIZE_LOW_PCT":     0.70,       # Score dasar (7-8) → 70% risk
    "CONF_SIZE_MED_PCT":     1.00,       # Score medium (9-10) → 100% risk
    "CONF_SIZE_HIGH_PCT":    1.20,       # Score tinggi (11+) → 120% risk

    # 3. Momentum Acceleration Gate
    "USE_MOMENTUM_GATE":     True,       # Hanya entry saat momentum akselerasi

    # 4. Smart Re-entry Blocker
    "USE_SMART_REENTRY":     True,
    "REENTRY_COOLDOWN_SEC":  120,        # Blokir re-entry selama 2 menit setelah close
    "BLOCK_SAME_SIDE_LOSS":  True,       # Blokir re-entry di side yang baru saja loss

    # 5. Adaptive TP Boost (High Confidence)
    "USE_ADAPTIVE_TP":       True,
    "ADAPTIVE_TP_THRESHOLD": 9,          # Score minimal untuk boost TP
    "ADAPTIVE_TP_MULT":      1.3,        # TP1/TP2 dikali 1.3 saat sinyal kuat
}

# ── PRESETS DEFINITION (Matches Dashboard JS) ──
BOT_PRESETS = {
    "SCALPING_10USD": {
        "PRIMARY_TF": "1m", "CONFIRM_TF": "15m", "REQUIRE_MTF_CONFIRM": False,
        "RISK_PER_TRADE": 0.15, "LEVERAGE": 20, "MAX_MARGIN_PCT": 0.50,
        "ATR_SL_MULT": 1.5, "ATR_TP1_MULT": 1.5, "MIN_RR_RATIO": 1.0,
        "MIN_BULL_SCORE": 6, "MIN_BEAR_SCORE": 6, "ADX_MIN_THRESHOLD": 20,
        "MIN_TP_DISTANCE_PCT": 0.30, "BE_ACTIVATION_PCT": 0.003,
        "BE_SAFEGUARD_PCT": 0.0002, "ALLOW_FAST_REENTRY": True,
        "TP1_PARTIAL_CLOSE": True, "TP1_CLOSE_PCT": 70,
        "TRAIL_ACTIVATION_PCT": 0.005, "TRAIL_DISTANCE_PCT": 0.003,
        "MAX_OPEN_TRADES": 1,
    },
    "SCALPING_PREMIUM": {
        "PRIMARY_TF": "1m", "CONFIRM_TF": "15m", "REQUIRE_MTF_CONFIRM": False,
        "RISK_PER_TRADE": 0.15, "LEVERAGE": 50, "MAX_MARGIN_PCT": 0.30,
        "ATR_SL_MULT": 1.2, "ATR_TP1_MULT": 1.2, "MIN_RR_RATIO": 0.9,
        "MIN_BULL_SCORE": 7, "MIN_BEAR_SCORE": 7, "ADX_MIN_THRESHOLD": 20,
        "MIN_TP_DISTANCE_PCT": 0.35, "BE_ACTIVATION_PCT": 0.003,
        "BE_SAFEGUARD_PCT": 0.0005, "ALLOW_FAST_REENTRY": True,
        "TP1_PARTIAL_CLOSE": True, "TP1_CLOSE_PCT": 70,
        "TRAIL_ACTIVATION_PCT": 0.005, "TRAIL_DISTANCE_PCT": 0.003,
        "MAX_OPEN_TRADES": 3,
    },
    "SCALPING": {
        "PRIMARY_TF": "1m", "CONFIRM_TF": "5m", "REQUIRE_MTF_CONFIRM": False,
        "RISK_PER_TRADE": 0.20, "LEVERAGE": 50, "MAX_MARGIN_PCT": 0.40,
        "ATR_SL_MULT": 1.5, "ATR_TP1_MULT": 1.5, "MIN_RR_RATIO": 1.0,
        "MIN_BULL_SCORE": 6, "MIN_BEAR_SCORE": 6, "ADX_MIN_THRESHOLD": 15,
        "MIN_TP_DISTANCE_PCT": 0.30, "BE_ACTIVATION_PCT": 0.008,
        "BE_SAFEGUARD_PCT": 0.002, "ALLOW_FAST_REENTRY": True,
        "TP1_PARTIAL_CLOSE": True, "TP1_CLOSE_PCT": 70,
        "TRAIL_ACTIVATION_PCT": 0.008, "TRAIL_DISTANCE_PCT": 0.005,
        "MAX_OPEN_TRADES": 3,
    },
    "SMART_SCALPER": {
        "PRIMARY_TF": "5m", "CONFIRM_TF": "15m", "REQUIRE_MTF_CONFIRM": True,
        "RISK_PER_TRADE": 0.10, "LEVERAGE": 25, "MAX_MARGIN_PCT": 0.25,
        "ATR_SL_MULT": 1.5, "ATR_TP1_MULT": 2.0, "MIN_RR_RATIO": 1.2,
        "MIN_BULL_SCORE": 7, "MIN_BEAR_SCORE": 7, "ADX_MIN_THRESHOLD": 22,
        "MIN_TP_DISTANCE_PCT": 0.20, "BE_ACTIVATION_PCT": 0.008,
        "BE_SAFEGUARD_PCT": 0.002, "ALLOW_FAST_REENTRY": True,
        "TP1_PARTIAL_CLOSE": True, "TP1_CLOSE_PCT": 50,
        "TRAIL_ACTIVATION_PCT": 0.012, "TRAIL_DISTANCE_PCT": 0.005,
        "MAX_OPEN_TRADES": 2,
        "ATR_TP2_MULT": 3.5, "ATR_TP3_MULT": 5.0,
        "EXIT_MIN_BULL_SCORE": 3, "EXIT_MIN_BEAR_SCORE": 3,
    },
    "SNIPER": {
        "PRIMARY_TF": "15m", "CONFIRM_TF": "1h", "REQUIRE_MTF_CONFIRM": True,
        "RISK_PER_TRADE": 0.10, "LEVERAGE": 20, "MAX_MARGIN_PCT": 0.20,
        "ATR_SL_MULT": 2.5, "ATR_TP1_MULT": 4.0, "MIN_RR_RATIO": 1.5,
        "MIN_BULL_SCORE": 7, "MIN_BEAR_SCORE": 7, "ADX_MIN_THRESHOLD": 25,
        "MIN_TP_DISTANCE_PCT": 0.50, "BE_ACTIVATION_PCT": 0.008,
        "BE_SAFEGUARD_PCT": 0.0001, "ALLOW_FAST_REENTRY": False,
        "TP1_PARTIAL_CLOSE": True, "TP1_CLOSE_PCT": 40,
        "TRAIL_ACTIVATION_PCT": 0.015, "TRAIL_DISTANCE_PCT": 0.008,
        "MAX_OPEN_TRADES": 2,
    },
    "STANDARD": {
        "PRIMARY_TF": "5m", "CONFIRM_TF": "1h", "REQUIRE_MTF_CONFIRM": False,
        "RISK_PER_TRADE": 0.15, "LEVERAGE": 25, "MAX_MARGIN_PCT": 0.30,
        "ATR_SL_MULT": 2.0, "ATR_TP1_MULT": 2.5, "MIN_RR_RATIO": 1.2,
        "MIN_BULL_SCORE": 6, "MIN_BEAR_SCORE": 6, "ADX_MIN_THRESHOLD": 15,
        "MIN_TP_DISTANCE_PCT": 0.35, "BE_ACTIVATION_PCT": 0.005,
        "BE_SAFEGUARD_PCT": 0.0001, "ALLOW_FAST_REENTRY": False,
        "TP1_PARTIAL_CLOSE": True, "TP1_CLOSE_PCT": 50,
        "TRAIL_ACTIVATION_PCT": 0.01, "TRAIL_DISTANCE_PCT": 0.005,
        "MAX_OPEN_TRADES": 3,
    },
}

# ══════════════════════════════════════════════════════════════
#  ENV & LOGGING
# ══════════════════════════════════════════════════════════════

MEXC_API_KEY    = os.getenv("MEXC_API_KEY", "")
MEXC_API_SECRET = os.getenv("MEXC_API_SECRET", "")
TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT   = os.getenv("TELEGRAM_CHAT_ID", "")
MEXC_BASE_URL   = "https://contract.mexc.com"
MEXC_WS_URL     = "wss://contract.mexc.com/edge"

# Konfigurasi Zona Waktu Indonesia (WIB = UTC+7)
WIB = timezone(timedelta(hours=7))

def wib_converter(*args):
    # Digunakan oleh logging untuk mancatat waktu dalam WIB
    return datetime.now(WIB).timetuple()

# Logging
log_handlers = [logging.StreamHandler()]
if CONFIG["LOG_FILE"]:
    log_handlers.append(logging.FileHandler(CONFIG["LOG_FILE"], encoding="utf-8"))

logging.basicConfig(
    level=getattr(logging, CONFIG["LOG_LEVEL"]),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=log_handlers,
)
# Override converter logger ke WIB
logging.Formatter.converter = wib_converter
log = logging.getLogger("XAUTBot")
logging.getLogger("websocket").setLevel(logging.WARNING)

# ══════════════════════════════════════════════════════════════
#  DATA STRUCTURES
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
    contract_size: float = 1.0
    order_id: Optional[str] = None
    # State dinamis
    trailing_active: bool = False
    trailing_stop: float = 0.0
    highest_price: float = 0.0  # untuk LONG trailing
    lowest_price: float = 0.0   # untuk SHORT trailing
    be_hit: bool = False
    risk_reduced: bool = False
    tp1_hit: bool = False
    tp2_hit: bool = False
    partial_closed: bool = False
    pnl: float = 0.0
    closed: bool = False
    close_reason: str = ""
    closed_at: str = ""
    weak_signal_count: int = 0     # Counter untuk grace period Weak Signal exit
    entry_bull_score: int = 0
    entry_bear_score: int = 0
    entry_confidence: int = 0
    entry_rsi: float = 0.0
    entry_atr: float = 0.0

@dataclass
class BotState:
    balance: float = 0.0                # Saldo Utama (Equity di LIVE, Virtual di DRY)
    available_balance: float = 0.0      # Saldo siap pakai (Hanya di LIVE)
    equity: float = 0.0                 # Total nilai akun (Hanya di LIVE)
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
    circuit_type: str = ""              # "AUTO" atau "MANUAL"
    secured_total: float = 0.0          # Total profit yang sudah dipindah ke Spot
    started_at: str = ""
    iteration: int = 0
    daily_start_balance: float = 0.0
    spot_balance: float = 0.0           # [NEW] Saldo USDT di Spot
    futures_total_equity: float = 0.0   # [NEW] Total Equity di Futures (semua koin)
    # ── Power Upgrade Fields ──
    consecutive_losses: int = 0         # Berapa kali loss berturut-turut
    last_loss_side: str = ""            # Side terakhir yang loss ("LONG"/"SHORT")
    last_close_time: float = 0.0        # Timestamp terakhir posisi ditutup

    def win_rate(self) -> float:
        return (self.winning_trades / self.total_trades * 100) if self.total_trades > 0 else 0.0

    def drawdown(self) -> float:
        if self.peak_balance <= 0:
            return 0.0
        # Proteksi: Jika balance == 0 (API error), jangan hitung drawdown palsu
        if self.balance <= 0:
            return 0.0
        return (self.peak_balance - self.balance) / self.peak_balance * 100

# ══════════════════════════════════════════════════════════════
#  MEXC REST API CLIENT — FUTURES
# ══════════════════════════════════════════════════════════════

class MEXCFuturesClient:
    """Klien untuk MEXC Futures API (Contract V1)."""
    def __init__(self, api_key: str, api_secret: str):
        self.api_key    = api_key
        self.api_secret = api_secret
        self.session    = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": "https://www.mexc.com",
            "Referer": "https://www.mexc.com/futures"
        })
        
        # SSL Handshake Fix: Force TLS 1.2+ and allow legacy ciphers
        class TLSAdapter(requests.adapters.HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                ctx = ssl.create_default_context()
                ctx.set_ciphers('DEFAULT@SECLEVEL=1')
                ctx.check_hostname = False
                kwargs['ssl_context'] = ctx
                return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

        self.session.mount("https://", TLSAdapter())
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass

    def _sign(self, timestamp: str, payload: str = "") -> str:
        message = f"{self.api_key}{timestamp}{payload}"
        return hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha256).hexdigest()

    def _request(self, method: str, ep: str, params: dict = None) -> Optional[dict]:
        timestamp = str(int(time.time() * 1000))
        url = MEXC_BASE_URL + ep
        
        # Untuk MEXC Futures V1:
        # POST: payload adalah JSON string (tanpa spasi agar Signature/WAF konsisten)
        if method == "POST":
            payload = json.dumps(params, separators=(',', ':')) if params else ""
        else:
            payload = ""
        
        headers = {}
        if "/private/" in ep:
            headers = {
                "ApiKey": self.api_key,
                "Request-Time": timestamp,
                "Signature": self._sign(timestamp, payload),
                "Content-Type": "application/json"
            }
        
        try:
            if method == "POST":
                r = self.session.post(url, headers=headers, data=payload, timeout=15)
            else:
                r = self.session.get(url, headers=headers, params=params, timeout=15)
                
            if not r.text:
                log.error(f"Empty response from {ep}")
                return None
            
            try:
                res = r.json()
                # Handle Rate Limit 510
                if str(res.get("code")) == "510":
                    log.warning(f"Rate Limit 510 on {ep}. Sleeping 5s and retrying...")
                    time.sleep(5)
                    # Retry once
                    if method == "POST":
                        r = self.session.post(url, headers=headers, data=payload, timeout=15)
                    else:
                        r = self.session.get(url, headers=headers, params=params, timeout=15)
                    res = r.json()

                if res.get("success") or str(res.get("code")) == "0":
                    return res.get("data")
                else:
                    code = res.get("code")
                    msg = res.get("message", "")
                    # Penjelasan lebih ramah untuk error umum MEXC
                    if str(code) == "2009":
                        msg += " (Tips: Pastikan mode posisi akun adalah 'Hedge Mode' / 'Mode Lindung Nilai')"
                    elif str(code) == "2005":
                        msg += " (Saldo tidak cukup untuk biaya margin)"
                    
                    log.warning(f"API Error {ep}: Code {code} - {msg}")
                    return None
            except Exception as e:
                # Jika gagal parse JSON, log 500 karakter pertama agar tahu alasan block (Cloudflare/WAF)
                log.error(f"JSON Parse Error {ep}: {e} | Status: {r.status_code} | Text: {r.text[:500]}")
                return None
        except Exception as e:
            log.error(f"Network error {ep}: {e}")
            return None

    def get_ticker(self, symbol: str) -> Optional[dict]:
        """Ambil data ticker lengkap termasuk Last Price dan Mark Price (Fair Price)."""
        d = self._request("GET", "/api/v1/contract/ticker", {"symbol": symbol})
        if d:
            return {
                "last": float(d.get("lastPrice", 0)),
                "mark": float(d.get("fairPrice", 0)),
                "index": float(d.get("indexPrice", 0))
            }
        return None
        
    def get_top_volume_coins(self, limit: int = 50) -> List[dict]:
        """Mengambil pasangan koin dengan volume tertinggi di MEXC Futures."""
        d = self._request("GET", "/api/v1/contract/ticker")
        if not d:
            return []
            
        results = []
        for x in d:
            sym = x.get("symbol", "")
            # Filter hanya pair USDT dan abaikan koin tes / index
            if not sym.endswith("_USDT") or "TEST" in sym or sym.startswith("INDEX"):
                continue
                
            try:
                # amount24 adalah Volume (USDT value) - Sesuai dokumentasi MEXC V1
                turnover = float(x.get("amount24", 0))
                change = float(x.get("riseFallRate", 0))
                results.append({
                    "symbol": sym,
                    "turnover": turnover,
                    "last": float(x.get("lastPrice", 0)),
                    "change": change
                })
            except (ValueError, TypeError):
                continue
                
        # Urutkan berdasarkan turnover tertinggi
        results.sort(key=lambda x: x["turnover"], reverse=True)
        return results[:limit]

    def get_all_balances(self) -> List[dict]:
        """Ambil semua saldo aset di Futures."""
        res = self._request("GET", "/api/v1/private/account/assets")
        return res if res else []

    def get_balance(self, asset: str = "USDT") -> Optional[dict]:
        """Mengambil saldo detail (Available & Equity/Total)."""
        d = self._request("GET", f"/api/v1/private/account/asset/{asset}")
        if d is None:
            # Fallback ke get_all_balances jika asset spesifik gagal
            all_bal = self.get_all_balances()
            for b in all_bal:
                if b.get("currency") == asset:
                    d = b
                    break
        
        if d is None: return None
        
        info = {}
        if isinstance(d, list):
            for b in d:
                if b.get("currency") == asset:
                    info = b
                    break
        elif isinstance(d, dict):
            info = d
            
        if not info:
            return {"available": 0.0, "equity": 0.0, "wallet": 0.0, "unrealized": 0.0}
            
        avail = float(info.get("availableBalance") or info.get("availableMargin") or info.get("available") or 0.0)
        pos_margin = float(info.get("positionMargin") or 0.0)
        frozen_margin = float(info.get("orderMargin") or info.get("frozenMargin") or info.get("frozenBalance") or 0.0)
        
        # Robust Equity = Available + Position Margin + Order Margin + Unrealized
        # This keeps the 'balance' stable when margin is allocated to a position.
        unrealized = float(info.get("unrealizedProfit") or info.get("unrealized") or 0.0)
        
        # In MEXC V1, availableBalance + positionMargin + orderMargin is the 'wallet' balance
        wallet = avail + pos_margin + frozen_margin
        
        # Gunakan API equity jika ada (sangat akurat), otherwise hitung manual
        api_equity = float(info.get("equity") or 0.0)
        equity = api_equity if api_equity > 0 else (wallet + unrealized)

        return {
            "available": avail,
            "wallet": wallet,
            "equity": equity,
            "unrealized": unrealized,
            "margin": pos_margin
        }

    def get_klines(self, symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
        tf_map = {
            "1m": "Min1", "5m": "Min5", "15m": "Min15", 
            "30m": "Min30", "1h": "Min60", "4h": "Hour4", "1d": "Day1"
        }
        tf = tf_map.get(interval, "Min1")
        d = self._request("GET", "/api/v1/contract/kline/" + symbol, {"interval": tf, "limit": limit})
        if not d:
            log.warning(f"Klines {tf} kosong untuk {symbol}")
            return pd.DataFrame()
        
        df = pd.DataFrame(d, columns=["time", "open", "close", "high", "low", "vol", "amount"])
        df.rename(columns={"vol": "volume"}, inplace=True)
        df["open_time"] = pd.to_datetime(df["time"], unit="s")
        df.set_index("open_time", inplace=True)
        for col in ["open", "close", "high", "low", "volume"]:
            df[col] = df[col].astype(float)
        return df

    def place_order(self, symbol: str, side: int, order_type: int,
                    lever: int, quantity: float, margin_mode: int = 1) -> Optional[dict]:
        """side: 1=Open Long, 2=Open Short, 3=Close Long, 4=Close Short. type: 5=Market. margin_mode: 1=Isolated, 2=Cross"""
        params = {
            "symbol": symbol, "side": side, "type": order_type,
            "vol": float(quantity), "leverage": int(lever), "openType": margin_mode
        }
        return self._request("POST", "/api/v1/private/order/submit", params)

    def place_stop_order(self, symbol: str, side: int, stop_price: float, quantity: float, leverage: int = 20, margin_mode: int = 1, is_take_profit: bool = False) -> Optional[dict]:
        """Menempatkan Hard SL/TP di exchange via planorder.
        side: 3=Close Long (One-Way), 1=Close Short (One-Way), 4=Close Long (Hedge), 2=Close Short (Hedge)
        is_take_profit: Jika True, triggerType dibalik karena TP trigger di arah berlawanan dari SL.
        
        Logika TriggerType:
        - SL LONG  → harga TURUN ke SL  → triggerType=2 (<=)
        - TP LONG  → harga NAIK ke TP   → triggerType=1 (>=)
        - SL SHORT → harga NAIK ke SL   → triggerType=1 (>=)
        - TP SHORT → harga TURUN ke TP  → triggerType=2 (<=)
        """
        # Base: SL logic
        trigger_type = 2 if side in (3, 4) else 1
        # Flip untuk TP (arah berlawanan dari SL)
        if is_take_profit:
            trigger_type = 1 if trigger_type == 2 else 2
        
        params = {
            "symbol": symbol,
            "side": side,
            "vol": float(quantity),
            "openType": margin_mode, # 1=Isolated, 2=Cross
            "triggerPrice": float(stop_price),
            "triggerType": trigger_type,
            "executeCycle": 1, # Berlaku 24 jam (atau 2 untuk 7 hari)
            "trend": 1, # Latest Price
            "orderType": 5 # 5=Market execution (tutup langsung saat tersentuh)
        }
        if margin_mode == 1:
            params["leverage"] = int(leverage)
            
        return self._request("POST", "/api/v1/private/planorder/place", params)

    def cancel_all_orders(self, symbol: str):
        """Membatalkan semua open orders dan trigger orders untuk koin tertentu."""
        # Cancel normal orders
        self._request("POST", "/api/v1/private/order/cancel_all", {"symbol": symbol})
        # Cancel trigger/stop orders
        self._request("POST", "/api/v1/private/planorder/cancel_all", {"symbol": symbol})

    def get_open_positions(self) -> List[dict]:
        """Mengambil semua posisi terbuka di MEXC Futures dengan fallback endpoint."""
        # Gunakan open_positions yang valid di V1
        for ep in ["/api/v1/private/position/open_positions", "/api/v1/private/position/open_details"]:
            d = self._request("GET", ep)
            if d is not None:
                return d
        return None  # Return None if both endpoints fail

    def get_open_orders(self, symbol: str) -> List[dict]:
        """Mengambil semua normal orders aktif."""
        d = self._request("GET", "/api/v1/private/order/list/open_orders", {"symbol": symbol})
        return d if d else []

    def get_stop_orders(self, symbol: str) -> List[dict]:
        """Mengambil semua trigger orders (SL/TP) aktif dengan fallback."""
        for ep in ["/api/v1/private/stop_order/list/open_orders", "/api/v1/private/stop_order/open_orders"]:
            d = self._request("GET", ep, {"symbol": symbol})
            if d is not None:
                return d
        return []

    def get_contract_detail(self, symbol: str) -> Optional[dict]:
        """Mengambil detail kontrak (presisi harga, volume, dll)."""
        return self._request("GET", "/api/v1/contract/detail", {"symbol": symbol})

class MEXCSpotClient:
    """Klien untuk MEXC Spot API v3 (untuk Transfer)."""
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
        """Transfer dari FUTURES ke SPOT."""
        endpoint = "/api/v3/capital/transfer"
        timestamp = int(time.time() * 1000)
        params = {
            "fromAccountType": "FUTURES",
            "toAccountType": "SPOT",
            "asset": "USDT",
            "amount": str(round(amount, 4)),
            "recvWindow": 60000,
            "timestamp": timestamp
        }
        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        signature = self._sign(query_string)
        url = f"{self.base_url}{endpoint}?{query_string}&signature={signature}"
        
        headers = {"X-MEXC-APIKEY": self.api_key}
        try:
            r = self.session.post(url, headers=headers, timeout=15)
            res = r.json()
            if res.get("tranId") or res.get("id"):
                log.info(f"[SECURE] Berhasil pindah ke Spot: ${amount:.4f}")
                return True
            log.error(f"[SECURE] Gagal transfer: {res}")
            return False
        except Exception as e:
            log.error(f"[SECURE] Error transfer: {e}")
            return False

    def get_spot_balance(self, asset: str = "USDT") -> float:
        """Ambil saldo total estimasi USDT di semua aset Spot (Wealth Protection)."""
        endpoint = "/api/v3/account"
        timestamp = int(time.time() * 1000)
        params = {"timestamp": timestamp}
        query_string = f"timestamp={timestamp}"
        signature = self._sign(query_string)
        url = f"{self.base_url}{endpoint}?{query_string}&signature={signature}"
        
        headers = {"X-MEXC-APIKEY": self.api_key}
        total_usdt = 0.0
        try:
            r = self.session.get(url, headers=headers, timeout=10)
            data = r.json()
            
            # Fetch all prices to estimate values
            price_res = self.session.get(f"{self.base_url}/api/v3/ticker/price", timeout=10)
            prices = {}
            if price_res.status_code == 200:
                for t in price_res.json():
                    if t["symbol"].endswith("USDT"):
                        prices[t["symbol"].replace("USDT","")] = float(t["price"])
            
            if "balances" in data:
                for b in data["balances"]:
                    free = float(b["free"])
                    locked = float(b["locked"])
                    coin_qty = free + locked
                    if coin_qty > 0:
                        if b["asset"] == "USDT" or b["asset"] == "USDC":
                            total_usdt += coin_qty
                        else:
                            # Convert to USDT
                            if b["asset"] in prices:
                                total_usdt += coin_qty * prices[b["asset"]]
                return total_usdt
            return 0.0
        except Exception as e:
            log.error(f"Error fetch spot balance: {e}")
            return 0.0

# ══════════════════════════════════════════════════════════════
#  PRICE FEED — REST POLLING
# ══════════════════════════════════════════════════════════════

class PriceFeed:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.price  = 0.0
        self.mark_price = 0.0 # NEW: For accurate PnL matching MEXC
        self.client = MEXCFuturesClient(MEXC_API_KEY, MEXC_API_SECRET)
        self._callbacks = []
        self._running   = False
        self._lock      = threading.Lock()

    def add_callback(self, cb):
        self._callbacks.append(cb)

    def start(self):
        self._running = True
        threading.Thread(target=self._run, daemon=True, name="PriceFeed").start()
        log.info(f"PriceFeed (REST) dimulai untuk {self.symbol}")

    def stop(self):
        self._running = False

    def get_price(self) -> float:
        with self._lock:
            return self.price

    def _run(self):
        while self._running:
            try:
                data = self.client.get_ticker(self.symbol)
                if data:
                    with self._lock:
                        self.price = data["last"]
                        self.mark_price = data["mark"]
                    for cb in self._callbacks:
                        try: cb(self.price)
                        except: pass
                time.sleep(5)
            except Exception as e:
                log.debug(f"Price update error: {e}")
                time.sleep(10)

# ══════════════════════════════════════════════════════════════
#  ANALISIS TEKNIKAL — MULTI INDIKATOR
# ══════════════════════════════════════════════════════════════

class TechnicalAnalysis:
    def __init__(self, cfg: dict):
        self.cfg = cfg

    def compute(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.cfg
        # RSI
        df["rsi"] = ta.rsi(df["close"], length=c["RSI_PERIOD"])

        # MACD
        macd = ta.macd(df["close"], fast=c["MACD_FAST"], slow=c["MACD_SLOW"], signal=c["MACD_SIGNAL"])
        k = f"MACD_{c['MACD_FAST']}_{c['MACD_SLOW']}_{c['MACD_SIGNAL']}"
        df["macd"]        = macd[k]
        df["macd_signal"] = macd[f"MACDs_{c['MACD_FAST']}_{c['MACD_SLOW']}_{c['MACD_SIGNAL']}"]
        df["macd_hist"]   = macd[f"MACDh_{c['MACD_FAST']}_{c['MACD_SLOW']}_{c['MACD_SIGNAL']}"]

        # Bollinger Bands
        bb = ta.bbands(df["close"], length=c["BB_PERIOD"], std=c["BB_STD"])
        df["bb_upper"] = bb[f"BBU_{c['BB_PERIOD']}_{c['BB_STD']}"]
        df["bb_mid"]   = bb[f"BBM_{c['BB_PERIOD']}_{c['BB_STD']}"]
        df["bb_lower"] = bb[f"BBL_{c['BB_PERIOD']}_{c['BB_STD']}"]
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"] * 100
        df["bb_pct"]   = (df["close"] - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])

        # EMA
        df["ema_fast"]  = ta.ema(df["close"], length=c["EMA_FAST"])
        df["ema_slow"]  = ta.ema(df["close"], length=c["EMA_SLOW"])
        df["ema_trend"] = ta.ema(df["close"], length=c["EMA_TREND"])
        df["ema_long"]  = ta.ema(df["close"], length=c["EMA_LONG"])

        # ATR
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c["ATR_PERIOD"])
        df["atr_pct"] = df["atr"] / df["close"] * 100

        # ADX (Anti-Sideways Filter)
        try:
            adx_period = c.get("ADX_PERIOD", 14)
            adx_result = ta.adx(df["high"], df["low"], df["close"], length=adx_period)
            if adx_result is not None and not adx_result.empty:
                # DMP = DI+, DMN = DI-
                df["adx"] = adx_result[f"ADX_{adx_period}"]
                df["di_plus"] = adx_result[f"DMP_{adx_period}"]
                df["di_minus"] = adx_result[f"DMN_{adx_period}"]
            else:
                df["adx"], df["di_plus"], df["di_minus"] = 25.0, 25.0, 25.0
        except Exception:
            df["adx"], df["di_plus"], df["di_minus"] = 25.0, 25.0, 25.0

        # Stochastic
        stoch = ta.stoch(df["high"], df["low"], df["close"],
                         k=c["STOCH_K"], d=c["STOCH_D"], smooth_k=c["STOCH_SMOOTH"])
        
        # Robust column extraction for Stochastic
        k_col = f"STOCHk_{c['STOCH_K']}_{c['STOCH_D']}_{c['STOCH_SMOOTH']}"
        d_col = f"STOCHd_{c['STOCH_K']}_{c['STOCH_D']}_{c['STOCH_SMOOTH']}"
        
        if k_col in stoch.columns:
            df["stoch_k"] = stoch[k_col]
            df["stoch_d"] = stoch[d_col]
        else:
            # Fallback to identify columns by pattern if exact name fails
            for col in stoch.columns:
                if "STOCHk" in col: df["stoch_k"] = stoch[col]
                if "STOCHd" in col: df["stoch_d"] = stoch[col]

        # OBV (On Balance Volume)
        df["obv"] = ta.obv(df["close"], df["volume"])
        df["obv_ema"] = ta.ema(df["obv"], length=21)

        # VWAP (Volume Weighted Average Price)
        df["vwap"] = ta.vwap(df["high"], df["low"], df["close"], df["volume"])

        # Volume MA & ratio
        df["vol_ma"]    = df["volume"].rolling(20).mean()
        df["vol_ratio"] = (df["volume"] / df["vol_ma"]).round(2)

        # Candle patterns (opsional)
        df["body_size"]  = abs(df["close"] - df["open"])
        df["upper_wick"] = df["high"] - df[["close","open"]].max(axis=1)
        df["lower_wick"] = df[["close","open"]].min(axis=1) - df["low"]
        df["is_bullish_candle"] = df["close"] > df["open"]

        # Ensure no NaNs crash the scanner later
        return df.fillna(0)

    def get_signal(self, df: pd.DataFrame, **kwargs) -> dict:
        c    = self.cfg
        if len(df) < 5:
            return {
                "signal": "NEUTRAL", 
                "bull_score": 0, "bear_score": 0, "max_score": 15, "confidence": 0, "htf_bias": "—",
                "rsi": 50.0, "macd": 0.0, "atr": 0.0, "atr_pct": 0.0,
                "stoch_k": 50.0, "vol_ratio": 1.0, "ema_fast": 0.0, "ema_slow": 0.0,
                "close": df["close"].iloc[-1] if len(df)>0 else 0,
                "bb_upper": 0, "bb_lower": 0, "bb_width": 0, "ema_trend": 0, "vwap": 0, "stoch_d": 0, "macd_signal": 0,
                "adx": 0.0,
                "details": {"Status": "Data tidak cukup"}
            }
            
        # Ambil bar terakhir dan sebelumnya dengan aman
        row   = df.iloc[-1].to_dict()
        prev  = df.iloc[-2].to_dict()
        prev2 = df.iloc[-3].to_dict()

        def clean_val(d, key, default=0.0):
            v = d.get(key)
            if v is None or (isinstance(v, float) and math.isnan(v)):
                return default
            return float(v)

        # ── [SCALPING] ADX Anti-Sideways Filter ──────────────
        adx_val = clean_val(row, "adx", 25.0)
        if c.get("USE_ADX_FILTER", True) and adx_val < c.get("ADX_MIN_THRESHOLD", 20):
            # ... (Logika Neutral tetap sama tapi dengan clean_val)
            close = float(row["close"])
            neutral_result = {
                "signal": "NEUTRAL",
                "bull_score": 0, "bear_score": 0, "max_score": 15, "confidence": 0, "htf_bias": "—",
                "rsi": clean_val(row, "rsi", 50), "macd": clean_val(row, "macd"),
                "atr": clean_val(row, "atr"), "atr_pct": clean_val(row, "atr_pct"),
                "stoch_k": clean_val(row, "stoch_k", 50), "vol_ratio": clean_val(row, "vol_ratio", 1),
                "ema_fast": clean_val(row, "ema_fast"), "ema_slow": clean_val(row, "ema_slow"),
                "close": close,
                "bb_upper": clean_val(row, "bb_upper"), "bb_lower": clean_val(row, "bb_lower"),
                "bb_width": clean_val(row, "bb_width"), "ema_trend": clean_val(row, "ema_trend"),
                "vwap": clean_val(row, "vwap"),
                "stoch_d": clean_val(row, "stoch_d", 50), "macd_signal": clean_val(row, "macd_signal"),
                "adx": adx_val,
                "hold_reason": f"⚠️ Sideways (ADX {adx_val:.1f})",
                "details": {"ADX": f"🔶 ADX {adx_val:.1f} — Ranging"}
            }
            return neutral_result

        bull = 0
        bear = 0
        details = {}
        details["ADX"] = f"✅ ADX {adx_val:.1f} — Market trending (layak entry)"

        # ── 1. RSI ──────────────────────────────────── max ±2
        rsi      = clean_val(row, "rsi", 50.0)
        prev_rsi = clean_val(prev, "rsi", 50.0)
        
        if rsi < c["RSI_OVERSOLD"] and rsi > prev_rsi:
            bull += 2; details["RSI"] = f"{rsi:.1f} 🟢 Recovery from Oversold"
        elif rsi < c["RSI_OVERSOLD"]:
            # Masih turun tajam, kurangi skor bullish
            bull += 1; details["RSI"] = f"{rsi:.1f} 🟡 Oversold (Falling)"
        elif rsi > c["RSI_OVERBOUGHT"] and rsi < prev_rsi:
            bear += 2; details["RSI"] = f"{rsi:.1f} 🔴 Pullback from Overbought"
        elif rsi > c["RSI_OVERBOUGHT"]:
            bear += 1; details["RSI"] = f"{rsi:.1f} 🟠 Overbought (Rising)"
        elif rsi > 50:
            bull += 1; details["RSI"] = f"{rsi:.1f} 🟡 Trend Bullish (RSI > 50)"
        else:
            bear += 1; details["RSI"] = f"{rsi:.1f} 🟠 Trend Bearish (RSI < 50)"

        # ── 2. MACD ─────────────────────────────────── max ±2
        macd        = clean_val(row, "macd")
        macd_sig    = clean_val(row, "macd_signal")
        prev_macd   = clean_val(prev, "macd")
        prev_macd_s = clean_val(prev, "macd_signal")
        macd_hist   = clean_val(row, "macd_hist")
        prev_hist   = clean_val(prev, "macd_hist")

        macd_cross_up   = macd > macd_sig and prev_macd <= prev_macd_s
        macd_cross_down = macd < macd_sig and prev_macd >= prev_macd_s
        macd_hist_up    = macd_hist > prev_hist

        if macd_cross_up:
            bull += 2; details["MACD"] = "🟢 Golden cross"
        elif macd_cross_down:
            bear += 2; details["MACD"] = "🔴 Death cross"
        elif macd > macd_sig and macd_hist_up:
            bull += 1; details["MACD"] = "🟡 Bullish momentum"
        elif macd < macd_sig and not macd_hist_up:
            bear += 1; details["MACD"] = "🟠 Bearish momentum"
        else:
            details["MACD"] = "⚪ Netral"

        # ── 3. Bollinger Bands ──────────────────────── max ±2
        close = row["close"]
        bb_pct = row["bb_pct"]
        if close > row["bb_lower"] and prev["close"] <= prev["bb_lower"]:
            bull += 2; details["BB"] = "🟢 Bounce from lower band"
        elif close < row["bb_upper"] and prev["close"] >= prev["bb_upper"]:
            bear += 2; details["BB"] = "🔴 Rejection from upper band"
        elif bb_pct < 0.2 and row["close"] > prev["close"]:
            bull += 1; details["BB"] = "🟡 Bottoming near lower band"
        elif bb_pct > 0.8 and row["close"] < prev["close"]:
            bear += 1; details["BB"] = "🟠 Topping near upper band"
        elif row["bb_width"] < 1.5:
            details["BB"] = "🟡 Squeeze (Penyempitan) — Breakout terdekat"
        else:
            details["BB"] = f"⚪ Normal ({bb_pct:.0%} position)"

        # ── 4. EMA Trend ────────────────────────────── max ±2
        ema_bull    = row["ema_fast"] > row["ema_slow"]
        above_trend = close > row["ema_trend"]
        above_long  = close > row["ema_long"]
        ema_cross_up   = row["ema_fast"] > row["ema_slow"] and prev["ema_fast"] <= prev["ema_slow"]
        ema_cross_down = row["ema_fast"] < row["ema_slow"] and prev["ema_fast"] >= prev["ema_slow"]

        if ema_cross_up:
            bull += 2; details["EMA"] = f"🟢 Golden cross EMA{c['EMA_FAST']}/{c['EMA_SLOW']}"
        elif ema_cross_down:
            bear += 2; details["EMA"] = f"🔴 Death cross EMA{c['EMA_FAST']}/{c['EMA_SLOW']}"
        elif ema_bull and above_trend and above_long:
            bull += 2; details["EMA"] = "🟢 Full bullish alignment"
        elif not ema_bull and not above_trend and not above_long:
            bear += 2; details["EMA"] = "🔴 Full bearish alignment"
        elif ema_bull:
            bull += 1; details["EMA"] = "🟡 EMA short-term bullish"
        else:
            bear += 1; details["EMA"] = "🟠 EMA short-term bearish"

        # ── 5. Stochastic ───────────────────────────── max ±2
        sk    = clean_val(row, "stoch_k", 50.0)
        sd    = clean_val(row, "stoch_d", 50.0)
        psk   = clean_val(prev, "stoch_k", 50.0)
        psd   = clean_val(prev, "stoch_d", 50.0)

        stoch_cross_up   = sk > sd and psk <= psd
        stoch_cross_down = sk < sd and psk >= psd

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

        # ── 6. OBV Divergence ───────────────────────── max ±1
        obv_trend_up = row["obv"] > row["obv_ema"]
        if obv_trend_up and ema_bull:
            bull += 1; details["OBV"] = "🟢 Volume konfirmasi uptrend"
        elif not obv_trend_up and not ema_bull:
            bear += 1; details["OBV"] = "🔴 Volume konfirmasi downtrend"
        else:
            details["OBV"] = "⚪ OBV divergen"

        # ── 7. VWAP ─────────────────────────────────── max ±1
        vwap_val = clean_val(row, "vwap", None)
        if vwap_val is not None:
            if close > vwap_val:
                bull += 1; details["VWAP"] = f"🟢 Above VWAP ${vwap_val:.2f}"
            else:
                bear += 1; details["VWAP"] = f"🔴 Below VWAP ${vwap_val:.2f}"
        else:
            details["VWAP"] = "⚪ VWAP N/A"

        # ── 8. Volume Spike ─────────────────────────── max ±1
        vol_ratio = row["vol_ratio"]
        if vol_ratio > 1.8:
            if ema_bull:
                bull += 1; details["VOL"] = f"🟢 Volume spike {vol_ratio:.1f}x + uptrend"
            else:
                bear += 1; details["VOL"] = f"🔴 Volume spike {vol_ratio:.1f}x + downtrend"
        else:
            details["VOL"] = f"⚪ Volume normal {vol_ratio:.1f}x"

        # ── 9. [SCALPING] Candle Body Strength ──────── max ±1
        # Candle bertubuh besar = momentum kuat = entry lebih aman
        body_ratio = row["body_size"] / (row["high"] - row["low"] + 1e-9)
        if body_ratio > 0.6:
            if row["is_bullish_candle"]:
                bull += 1; details["CANDLE"] = f"🟢 Strong bullish candle ({body_ratio:.0%} body)"
            else:
                bear += 1; details["CANDLE"] = f"🔴 Strong bearish candle ({body_ratio:.0%} body)"
        elif body_ratio < 0.25:
            details["CANDLE"] = f"⚪ Doji/indecisive ({body_ratio:.0%} body) — Hati-hati"
        else:
            details["CANDLE"] = f"⚪ Candle normal ({body_ratio:.0%} body)"

        # ── Sinyal final ─────────────────────────────
        # Max score: RSI(2) + MACD(2) + BB(2) + EMA(2) + Stoch(2) + OBV(1) + VWAP(1) + Vol(1) + Candle(1) = 14
        max_score = 14
        
        # ── [POWER UPGRADE 1] Loss Streak Cooldown ──
        req_bull_score = c["MIN_BULL_SCORE"]
        req_bear_score = c["MIN_BEAR_SCORE"]
        # bot_state diteruskan dari fetch_and_analyze
        bot_state = kwargs.get("bot_state", None)
        if c.get("USE_LOSS_COOLDOWN") and bot_state:
            cooldown_after = c.get("LOSS_COOLDOWN_AFTER", 3)
            boost_amt = c.get("LOSS_COOLDOWN_BOOST", 1)
            # Jika loss berturut-turut lebih dari batas, naikkan threshold (lebih selektif)
            if bot_state.consecutive_losses >= cooldown_after:
                multiplier = bot_state.consecutive_losses // cooldown_after
                added = boost_amt * multiplier
                req_bull_score += added
                req_bear_score += added
                details["COOLDOWN"] = f"🛡️ Loss Streak ({bot_state.consecutive_losses}). Score minimal naik +{added}."

        # ── [POWER UPGRADE 2] SNIPER BOOST (Deteksi Pantulan Awal) ──
        # Membantu mencapai MIN_SCORE lebih cepat sebelum harga terlanjur lari
        # Jika momentum mulai berbalik dari oversold/overbought, berikan skor tinggi
        if sk < 30 and stoch_cross_up:
            bull += 4
            details["SNIPER"] = "🦅 Sniper Boost LONG (+4): Pantulan awal dari Oversold"
        elif sk > 70 and stoch_cross_down:
            bear += 4
            details["SNIPER"] = "🦅 Sniper Boost SHORT (+4): Rejection awal dari Overbought"

        # ── Evaluasi Sinyal dengan Over-Extension Blocker ──
        # Mencegah bot OP jika harga sudah terlampau lari (over-extended) dari EMA FAST
        if bull >= req_bull_score and bull > bear:
            # Over-Extension Blocker (Distance from EMA Fast)
            ema_dist = (close - row["ema_fast"]) / row["ema_fast"]
            max_dist = c.get("MAX_ENTRY_DISTANCE_PCT", 0.003)
            
            if rsi > 70:
                signal = "NEUTRAL"
                details["BLOCKER"] = f"⛔ Batal LONG: RSI {rsi:.1f} terlalu tinggi (Overbought)"
            elif ema_dist > max_dist:
                signal = "NEUTRAL"
                details["BLOCKER"] = f"⛔ Batal LONG: Harga terlalu jauh dari EMA (+{ema_dist*100:.2f}%) — Rawan Pucuk"
            else:
                signal = "LONG"
                
        elif bear >= req_bear_score and bear > bull:
            ema_dist = (row["ema_fast"] - close) / row["ema_fast"]
            max_dist = c.get("MAX_ENTRY_DISTANCE_PCT", 0.003)
            
            if rsi < 30:
                signal = "NEUTRAL"
                details["BLOCKER"] = f"⛔ Batal SHORT: RSI {rsi:.1f} terlalu rendah (Oversold)"
            elif ema_dist > max_dist:
                signal = "NEUTRAL"
                details["BLOCKER"] = f"⛔ Batal SHORT: Harga terlalu jauh dari EMA (-{ema_dist*100:.2f}%) — Rawan Lembah"
            else:
                signal = "SHORT"
        else:
            signal = "NEUTRAL"

        return {
            "signal":      signal,
            "bull_score":  bull,
            "bear_score":  bear,
            "max_score":   max_score,
            "confidence":  round(max(bull, bear) / max_score * 100),
            "rsi":         rsi,
            "atr":         clean_val(row, "atr"),
            "atr_pct":     clean_val(row, "atr_pct"),
            "close":       close,
            "bb_upper":    clean_val(row, "bb_upper"),
            "bb_lower":    clean_val(row, "bb_lower"),
            "bb_width":    clean_val(row, "bb_width"),
            "ema_fast":    clean_val(row, "ema_fast"),
            "ema_slow":    clean_val(row, "ema_slow"),
            "ema_trend":   clean_val(row, "ema_trend"),
            "vwap":        clean_val(row, "vwap"),
            "stoch_k":     sk,
            "stoch_d":     sd,
            "vol_ratio":   vol_ratio,
            "macd":        clean_val(row, "macd"),
            "macd_signal": clean_val(row, "macd_signal"),
            "adx":         adx_val,
            "details":     details,
        }

    def get_htf_bias(self, df_htf: pd.DataFrame) -> str:
        """Tentukan bias dari higher timeframe (MTF confirmation)."""
        try:
            df_htf = self.compute(df_htf.copy())
            df_htf.dropna(inplace=True)
            row = df_htf.iloc[-1]
            ema_bull = row["ema_fast"] > row["ema_slow"]
            above_trend = row["close"] > row["ema_trend"]
            rsi_bull = row["rsi"] < 60
            macd_bull = row["macd"] > row["macd_signal"]
            score = sum([ema_bull, above_trend, rsi_bull, macd_bull])
            if score >= 3:
                return "BULLISH"
            elif score <= 1:
                return "BEARISH"
            return "NEUTRAL"
        except Exception:
            return "NEUTRAL"

# ══════════════════════════════════════════════════════════════
#  MANAJEMEN RISIKO
# ══════════════════════════════════════════════════════════════

class RiskManager:
    def __init__(self, cfg: dict):
        self.cfg = cfg

    def calculate_levels(self, side: str, entry: float, atr: float, signal_score: int = 0) -> dict:
        c   = self.cfg
        sl  = atr * c["ATR_SL_MULT"]
        tp1 = atr * c["ATR_TP1_MULT"]
        tp2 = atr * c["ATR_TP2_MULT"]
        tp3 = atr * c["ATR_TP3_MULT"]
        
        # ── [POWER UPGRADE 5] Adaptive TP Boost ──
        if c.get("USE_ADAPTIVE_TP") and signal_score >= c.get("ADAPTIVE_TP_THRESHOLD", 9):
            boost = c.get("ADAPTIVE_TP_MULT", 1.3)
            tp1 *= boost
            tp2 *= boost
            tp3 *= boost

        if side == "LONG":
            return {
                "stop_loss":    round(entry - sl, 6),
                "take_profit1": round(entry + tp1, 6),
                "take_profit2": round(entry + tp2, 6),
                "take_profit3": round(entry + tp3, 6),
                "sl_distance":  round(sl, 6),
                "rr_ratio":     round(tp1 / sl, 2),
                "entry":        entry
            }
        else:
            return {
                "stop_loss":    round(entry + sl, 6),
                "take_profit1": round(entry - tp1, 6),
                "take_profit2": round(entry - tp2, 6),
                "take_profit3": round(entry - tp3, 6),
                "sl_distance":  round(sl, 6),
                "rr_ratio":     round(tp1 / sl, 2),
                "entry":        entry
            }

    def position_size(self, balance: float, sl_distance: float, entry_price: float,
                      effective_lev: float, win_rate: float = 0.5, signal_confidence: int = 50) -> float:
        c = self.cfg
        if c["USE_KELLY"] and win_rate > 0:
            # Kelly Criterion: f* = W - (1-W)/R, dimana R = RR ratio
            rr = c["ATR_TP1_MULT"] / c["ATR_SL_MULT"]
            kelly = win_rate - (1 - win_rate) / rr
            risk_pct = min(max(kelly * 0.5, 0.005), 0.04)  # half-kelly, capped
        else:
            risk_pct = c["RISK_PER_TRADE"]

            # ── [POWER UPGRADE 2] Confidence-Scaled Sizing ──
            if c.get("USE_CONFIDENCE_SIZING"):
                conf = signal_confidence
                # Misal max_score ~14, conf % = score / 14 * 100
                # Di bawah kita asumsikan confidence dalam persentase (min 50%)
                if conf < 65:     # (Score 6-8)
                    risk_pct *= c.get("CONF_SIZE_LOW_PCT", 0.70)
                elif conf < 78:   # (Score 9-10)
                    risk_pct *= c.get("CONF_SIZE_MED_PCT", 1.00)
                else:             # (Score 11+)
                    risk_pct *= c.get("CONF_SIZE_HIGH_PCT", 1.20)

        risk_amount = balance * risk_pct
        risk_qty = risk_amount / sl_distance if sl_distance > 0 else 0.0
        
        # Margin cap logic: Jangan sampai margin melebihi MAX_MARGIN_PCT dari saldo asli
        # Margin = (Qty * Entry) / Leverage
        max_margin = balance * c["MAX_MARGIN_PCT"]
        max_qty    = (max_margin * effective_lev) / entry_price if entry_price > 0 else 0.0
        
        final_qty = min(risk_qty, max_qty)
        return round(final_qty, 6)

    def check_rr(self, levels: dict) -> bool:
        return levels["rr_ratio"] >= self.cfg["MIN_RR_RATIO"]

    def check_tp_probability(self, levels: dict, signal: dict) -> Tuple[bool, str]:
        """Cek apakah TP realistis bisa dicapai (Quality Filter)."""
        c = self.cfg
        if not c.get("USE_PROBABILITY_FILTER"):
            return True, ""

        entry = levels["entry"]
        tp1   = levels["take_profit1"]
        atr   = signal["atr"]
        vol   = signal["vol_ratio"]
        
        # 1. Volume Check (Harus ada tenaga)
        if vol < c["MIN_VOL_RATIO"]:
            return False, f"Volume rendah ({vol:.1f}x < {c['MIN_VOL_RATIO']}x)"

        # 2. ATR Space Check (Jangan terlalu jauh dari jangkauan volatilitas)
        dist_to_tp = abs(tp1 - entry)
        atr_mult = dist_to_tp / atr if atr > 0 else 99
        if atr_mult > c["MAX_ATR_DISTANCE_MULT"]:
            return False, f"TP terlalu jauh ({atr_mult:.1f}x ATR)"

        # 3. Minimum Distance Check (Gerakan harus cukup besar)
        dist_pct = (dist_to_tp / entry) * 100
        if dist_pct < c["MIN_TP_DISTANCE_PCT"]:
            return False, f"TP terlalu tipis ({dist_pct:.2f}% < {c['MIN_TP_DISTANCE_PCT']}%)"

        return True, "OK"

    def update_trailing_stop(self, pos: Position, current_price: float) -> Optional[float]:
        """Kalkulasi trailing stop baru. Return new_sl jika berubah, else None."""
        c = self.cfg
        if not c["USE_TRAILING_STOP"]:
            return None

        if pos.side == "LONG":
            # Aktifkan trailing setelah profit TRAIL_ACTIVATION_PCT
            if not pos.trailing_active:
                profit_pct = (current_price - pos.entry_price) / pos.entry_price
                if profit_pct >= c["TRAIL_ACTIVATION_PCT"]:
                    pos.trailing_active = True
                    pos.highest_price   = current_price
                    trail_sl = current_price * (1 - c["TRAIL_DISTANCE_PCT"])
                    # Safeguard: Trailing Stop pertama tidak boleh lebih buruk dari Entry
                    pos.trailing_stop = max(trail_sl, pos.entry_price, pos.stop_loss)
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
                    # Safeguard: Trailing Stop pertama tidak boleh lebih buruk dari Entry
                    pos.trailing_stop = min(trail_sl, pos.entry_price, pos.stop_loss)
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
        now = datetime.now(WIB)
        c   = self.cfg

        if not c["USE_SESSION_FILTER"]:
            return True, "Filter dinonaktifkan"

        hour = now.hour
        weekday = now.weekday()  # 0=Mon, 4=Fri, 5=Sat, 6=Sun

        # Blokir Sabtu penuh
        if weekday == 5:
            return False, "Weekend — pasar sepi (Sabtu)"

        # Blokir Minggu sebelum open
        if weekday == 6 and c["BLOCK_SUNDAY_OPEN"] and hour < 7:
            return False, "Weekend — menunggu open Senin"

        # Blokir Jumat malam (market tutup approaching)
        if weekday == 4 and c["BLOCK_FRIDAY_CLOSE"] and hour >= 20:
            return False, "Jumat malam — likuiditas rendah"

        # Cek jam trading
        if hour not in c["ALLOWED_HOURS_UTC"]:
            return False, f"Di luar jam trading ({hour:02d}:xx UTC)"

        # Cek news blackout
        for news_str in c["NEWS_BLACKOUT"]:
            try:
                news_dt = datetime.strptime(
                    f"{now.year}-{news_str}",
                    "%Y-%m-%d %H:%M"
                ).replace(tzinfo=timezone.utc)
                diff = abs((now - news_dt).total_seconds())
                if diff <= 1800:  # ±30 menit
                    return False, f"News blackout: {news_str}"
            except Exception:
                pass

        # Identifikasi sesi
        if 13 <= hour < 16:
            session = "London/NY Overlap (PRIME)"
        elif 7 <= hour < 13:
            session = "London session"
        elif 16 <= hour < 22:
            session = "New York session"
        else:
            session = "Asia session"

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
                pos.entry_bull_score, pos.entry_bear_score,
                pos.entry_confidence,
                pos.trailing_active,
                round(pos.entry_rsi, 2),
                round(pos.entry_atr, 6),
            ])

    def get_trades(self, date_filter: str = None) -> list:
        """Baca trades.csv dan kembalikan sebagai list of dict. Jika ada date_filter, ambil tanggal tsb."""
        if not os.path.exists(self.filepath):
            return []
        trades = []
        try:
            with open(self.filepath, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if date_filter:
                        # closed_at format: YYYY-MM-DD HH:MM:SS
                        if row["closed_at"].startswith(date_filter):
                            trades.append(row)
                    else:
                        trades.append(row)
            return trades[::-1] # Terbaru di atas
        except Exception as e:
            log.error(f"Error baca riwayat: {e}")
            return []

    def get_total_pnl_for_date(self, date_str: str) -> float:
        """Hitung total PnL untuk tanggal tertentu."""
        trades = self.get_trades(date_str)
        return sum(float(t.get("pnl", 0)) for t in trades)

# ══════════════════════════════════════════════════════════════
#  STATE PERSISTENCE
# ══════════════════════════════════════════════════════════════

class StatePersistence:
    def __init__(self, filepath: str):
        self.filepath = filepath

    def save(self, state: BotState, is_dry_run: bool):
        try:
            data = {
                "balance":         state.balance,
                "peak_balance":    state.peak_balance,
                "total_trades":    state.total_trades,
                "winning_trades":  state.winning_trades,
                "total_pnl":       state.total_pnl,
                "daily_pnl":       state.daily_pnl,
                "daily_reset_date": state.daily_reset_date,
                "circuit_breaker": state.circuit_breaker,
                "circuit_reason":  state.circuit_reason,
                "circuit_type":    state.circuit_type,
                "started_at":      state.started_at,
                "iteration":       state.iteration,
                "secured_total":   state.secured_total,
                "is_dry_run":      is_dry_run,
                "positions": [asdict(p) for p in state.positions if not p.closed],
            }
            with open(self.filepath, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            log.error(f"Gagal menyimpan state: {e}")

    def load(self) -> Optional[dict]:
        if not os.path.exists(self.filepath):
            return None
        try:
            with open(self.filepath) as f:
                return json.load(f)
        except Exception as e:
            log.error(f"Gagal membaca state: {e}")
            return None

# ══════════════════════════════════════════════════════════════
#  NOTIFIKASI TELEGRAM
# ══════════════════════════════════════════════════════════════

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)
        self._queue  = deque()
        self._thread = None
        if self.enabled:
            self._thread = threading.Thread(target=self._worker, daemon=True, name="Telegram")
            self._thread.start()

    def send(self, message: str):
        if self.enabled:
            self._queue.append(message)

    def _worker(self):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        while True:
            if self._queue:
                msg = self._queue.popleft()
                try:
                    requests.post(url, json={
                        "chat_id":    self.chat_id,
                        "text":       msg,
                        "parse_mode": "Markdown",
                    }, timeout=8)
                    time.sleep(0.5)
                except Exception as e:
                    log.warning(f"Telegram error: {e}")
            else:
                time.sleep(1)

# ══════════════════════════════════════════════════════════════
#  BOT UTAMA
# ══════════════════════════════════════════════════════════════

class XAUTBot:
    def load_config(self):
        """Muat konfigurasi dari file config.json, jika tidak ada pakai default CONFIG."""
        path = "config.json"
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    file_cfg = json.load(f)
                    full_cfg = CONFIG.copy()
                    full_cfg.update(file_cfg)
                    
                    # Apply Preset if not CUSTOM
                    preset_name = full_cfg.get("PRESET_MODE", "CUSTOM")
                    if preset_name in BOT_PRESETS:
                        log.info(f"Applying PRESET: {preset_name}")
                        full_cfg.update(BOT_PRESETS[preset_name])
                    
                    log.info("Konfigurasi dimuat dari config.json")
                    return full_cfg
            except Exception as e:
                log.error(f"Gagal memuat config.json: {e}")
        
        # Simpan default ke file
        self.save_config(CONFIG)
        return CONFIG

    def save_config(self, cfg: dict):
        """Simpan konfigurasi ke file config.json secara permanen."""
        try:
            # Pastikan DRY_RUN disinkronkan ke file jika diubah
            with open("config.json", "w") as f:
                json.dump(cfg, f, indent=4)
            log.info("Konfigurasi disimpan ke config.json")
        except Exception as e:
            log.error(f"Gagal menyimpan config.json: {e}")

    def update_config_live(self, new_cfg: dict):
        """Update konfigurasi di memori bot tanpa restart (Hot-Reload)."""
        # Proteksi kunci sensitif agar tidak bisa diubah via dashboard
        for key in ["MEXC_API_KEY", "MEXC_API_SECRET", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]:
             if key in new_cfg: del new_cfg[key]
        
        # Update memori utama
        old_symbol = self.cfg.get("SYMBOL")
        self.cfg.update(new_cfg)
        self.save_config(self.cfg)
        
        # Jika ganti koin atau timeframe, reset price feed dan history
        reset_required = False
        if "SYMBOL" in new_cfg and new_cfg["SYMBOL"] != old_symbol:
            reset_required = True
            log.info(f"Mengganti Pair: {old_symbol} -> {new_cfg['SYMBOL']}")
            self.price_feed.symbol = new_cfg["SYMBOL"]
            self.price_feed.price = 0.0 # Reset harga agar tidak tampil harga koin lama
            
            # [BARU] Reset trauma (Anti-Revenge) agar tidak terbawa ke koin baru
            if self.state.consecutive_losses > 0:
                log.info(f"Mereset Trauma {self.state.last_loss_side} dari {old_symbol}")
            self.state.consecutive_losses = 0
            self.state.last_loss_side = getattr(self.state, 'last_loss_side', "")

        if "PRIMARY_TF" in new_cfg or "CONFIRM_TF" in new_cfg:
            reset_required = True
            log.info("Timeframe berubah — meriset data kline...")

        if reset_required:
            self._last_signal.clear()
            self._price_history.clear()
        
        # Update referensi di sub-modul agar efeknya langsung terasa
        if hasattr(self, 'ta'): self.ta.cfg = self.cfg
        if hasattr(self, 'risk'): self.risk.cfg = self.cfg
        if hasattr(self, 'session'): self.session.cfg = self.cfg
        if hasattr(self, 'journal'): self.journal.filepath = self.cfg["JOURNAL_FILE"]
        if hasattr(self, 'persistence'): self.persistence.filepath = self.cfg["STATE_FILE"]
        
        # Hot-reload mode (Dry vs Live)
        new_mode = self.cfg.get("DRY_RUN", True)
        if new_mode != self.dry_run:
            if not new_mode: # Transisi dari DRY -> LIVE
                log.info("Mendeteksi perpindahan DRY -> LIVE. Menyinkronkan saldo asli dari MEXC...")
                bal_data = self.client.get_balance("USDT")
                if bal_data is not None:
                    self.state.balance = bal_data["equity"]
                    self.state.available_balance = bal_data["available"]
                    self.state.equity = bal_data["equity"]
                    log.info(f"Saldo disinkronkan ke MEXC: Equity ${self.state.equity:,.2f} | Available ${self.state.available_balance:,.2f}")
                else:
                    log.warning("Gagal mengambil saldo dari MEXC (API Error). Pastikan API Key benar.")
                
                # Mereset statistik agar sesi LIVE mulai dari nol (bersih)
                self.state.total_pnl = 0.0
                self.state.daily_pnl = 0.0
                self.state.total_trades = 0
                self.state.winning_trades = 0
                self.state.peak_balance = self.state.balance
                self.state.positions = [] # Hapus posisi virtual
                self.state.circuit_breaker = False
                self.state.circuit_reason = ""
                self.state.secured_total = 0.0  # Reset profit Spot simulasi
            else: # Transisi dari LIVE -> DRY
                log.info("Mendeteksi perpindahan LIVE -> DRY. Mereset ke Virtual Balance...")
                self.state.balance = self.cfg.get("VIRTUAL_BALANCE", 100.0)
                self.state.total_pnl = 0.0
                self.state.daily_pnl = 0.0
                self.state.total_trades = 0
                self.state.winning_trades = 0
                self.state.peak_balance = self.state.balance
                self.state.positions = [] # Hapus posisi live (di memori)
                self.state.circuit_breaker = False
                self.state.circuit_reason = ""
                self.state.secured_total = 0.0  # Reset profit Spot
            
            # Simpan state segera setelah transisi mode
            self.persistence.save(self.state, self.dry_run)

        self.dry_run = new_mode
        log.info(f"🔥 HOT-RELOAD: Konfigurasi diperbarui ({'DRY RUN' if self.dry_run else 'LIVE'}).")

    def __init__(self, run_dashboard: bool = False, cli_overrides: dict = None):
        self._contract_info = {} # Cache untuk detail koin (presisi)
        self.cfg          = self.load_config()
        if cli_overrides:
            self.cfg.update(cli_overrides)
            # Re-sync dry_run if it was overridden
            if "DRY_RUN" in cli_overrides:
                self.dry_run = cli_overrides["DRY_RUN"]
        
        self.client       = MEXCFuturesClient(MEXC_API_KEY, MEXC_API_SECRET)
        self.ta           = TechnicalAnalysis(self.cfg)
        self.risk         = RiskManager(self.cfg)
        self.session      = SessionFilter(self.cfg)
        self.notifier     = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT)
        self.journal      = TradeJournal(self.cfg["JOURNAL_FILE"])
        self.persistence  = StatePersistence(self.cfg["STATE_FILE"])
        self.price_feed   = PriceFeed(self.cfg["SYMBOL"])
        self.state        = BotState()
        # Initialize self.dry_run from config (prioritizing CLI overrides handled above)
        self.dry_run      = self.cfg.get("DRY_RUN", True)
        self.run_dashboard = run_dashboard
        self._last_signal  = {}
        self._price_history = deque(maxlen=500)
        self._trade_id_counter = 0
        self._close_lock = threading.Lock()  # Lock untuk cegah race condition close ganda

        # Muat sinyal hasil scan terakhir agar tidak kosong saat restart
        self._scanned_signals = {}
        if os.path.exists("scanned_signals.json"):
            try:
                with open("scanned_signals.json", "r") as f:
                    self._scanned_signals = json.load(f)
            except: pass

        # Setup spot client SEBELUM init_state (karena _init_state menggunakannya)
        self.spot_client = MEXCSpotClient(MEXC_API_KEY, MEXC_API_SECRET)

        # Setup state (includes position restore — SATU-SATUNYA tempat posisi di-restore)
        self._init_state()

        # WebSocket price feed — callback trailing stop
        self.price_feed.add_callback(self._on_price_update)
        self.price_feed.start()

        # Tunggu sebentar untuk WS connect
        time.sleep(2)

    def _init_state(self):
        """Coba load state dari disk, fallback ke fresh state."""
        saved = self.persistence.load()
        if saved:
            log.info("State sebelumnya ditemukan — resume...")
            self.state.balance         = saved.get("balance", 0)
            self.state.peak_balance    = saved.get("peak_balance", 0)
            self.state.total_trades    = saved.get("total_trades", 0)
            self.state.winning_trades  = saved.get("winning_trades", 0)
            self.state.total_pnl       = saved.get("total_pnl", 0)
            self.state.daily_pnl       = saved.get("daily_pnl", 0)
            self.state.daily_reset_date = saved.get("daily_reset_date", "")
            self.state.circuit_breaker = saved.get("circuit_breaker", False)
            self.state.circuit_reason  = saved.get("circuit_reason", "")
            self.state.circuit_type    = saved.get("circuit_type", "")
            self.state.started_at      = saved.get("started_at", datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"))
            self.state.iteration       = saved.get("iteration", 0)
            self.state.secured_total   = saved.get("secured_total", 0.0)
            self.state.daily_start_balance = saved.get("daily_start_balance", 0.0)
            # Restore open positions
            for p_dict in saved.get("positions", []):
                try:
                    if "symbol" not in p_dict:
                        p_dict["symbol"] = self.cfg.get("SYMBOL", "UNKNOWN")
                    pos = Position(**p_dict)
                    self.state.positions.append(pos)
                    log.info(f"Restored posisi: {pos.id} {pos.symbol} {pos.side} @ ${pos.entry_price}")
                except Exception as e:
                    log.error(f"Gagal restore posisi: {e}")
            
            # ── 1. Jika Live Mode, ALWAYS re-fetch balance dari exchange ──
            if not self.dry_run:
                bal_data = self.client.get_balance("USDT")
                if bal_data is None:
                    log.error("❌ GAGAL AMBIL SALDO LIVE (API Unauthorized atau Error Koneksi).")
                    log.error("Pastikan file .env berisi API Key yang benar dengan izin Futures.")
                else:
                    # Gunakan perhitungan Robust: Saldo = Available + Margin Posisi
                    # Agar tidak drop saat posisi baru dibuka (MEXC quirk)
                    self.state.balance = bal_data["equity"] 
                    self.state.available_balance = bal_data["available"]
                    self.state.equity = bal_data["equity"]
                    self.state.peak_balance = max(self.state.peak_balance, self.state.balance)
                    log.info(f"LIVE MODE: Saldo disinkronkan ke MEXC: Equity ${self.state.equity:,.2f} | Available ${self.state.available_balance:,.2f}")
                    
                    # [NEW] Ambil Spot & All Futures Equity
                    try:
                        self.state.spot_balance = self.spot_client.get_spot_balance("USDT")
                        all_f = self.client.get_all_balances()
                        total_f = 0.0
                        for ab in all_f:
                            # Komprehensif: cek semua kemungkinan nama field saldo di MEXC V1
                            total_f += float(ab.get("equity") or ab.get("marginBalance") or ab.get("totalMarginBalance") or ab.get("cashBalance") or 0.0)
                        self.state.futures_total_equity = total_f
                    except: pass
                    
                    # Safeguard: Jika peak_balance jauh lebih tinggi, reset agar tidak kena Circuit Breaker instan
                    # Reset agresif jika peak jauh di atas balance (agar tidak kena CB drawdown palsu saat restart)
                    if self.state.peak_balance > self.state.balance * 1.3:
                        log.info(f"Relativize Peak: {self.state.peak_balance:.2f} -> {self.state.balance:.2f} (Clean Start)")
                        self.state.peak_balance = self.state.balance
                        self.state.circuit_breaker = False
                        self.state.circuit_reason = ""
                
                # [NEW] Sync positions immediately at startup
                self.sync_positions_from_mexc()
            
            # ── 2. Jika pindah dari Dry -> Live, reset PnL dan Circuit Breaker agar bersih ──
            # Kita bandingkan dengan state yang tersimpan di file
            was_dry = saved.get("is_dry_run", True)
            
            # PENTING: Gunakan current self.dry_run (yang mungkin dari CLI --live) 
            # untuk menentukan apakah kita HARUS dalam mode Live.
            if was_dry and not self.dry_run:
                # PENTING: Jika kita start bot dengan --live tapi state bilang was_dry=True,
                # hal ini dianggap transisi. Kita izinkan ini terjadi agar saldo tersinkron.
                log.info("Mendeteksi perpindahan DRY -> LIVE. Mereset statistik PnL & Circuit Breaker...")
                self.state.total_pnl = 0.0
                self.state.daily_pnl = 0.0
                self.state.total_trades = 0
                self.state.winning_trades = 0
                self.state.circuit_breaker = False
                self.state.circuit_reason = ""
                self.state.peak_balance = self.state.balance
                self.state.secured_total = 0.0
            
            # ── 3. Jika pindah dari Live -> Dry, reset ke Virtual Balance ──
            if not was_dry and self.dry_run:
                log.info("Mendeteksi perpindahan LIVE -> DRY. Mereset ke Virtual Balance...")
                self.state.balance = self.cfg["VIRTUAL_BALANCE"]
                self.state.peak_balance = self.state.balance
                self.state.total_pnl = 0.0
                self.state.daily_pnl = 0.0
                self.state.total_trades = 0
                self.state.winning_trades = 0
                self.state.circuit_breaker = False
                self.state.circuit_reason = ""
                self.state.secured_total = 0.0  # Reset profit Spot
        else:
            self.state.started_at = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
            if self.dry_run:
                self.state.balance = self.cfg["VIRTUAL_BALANCE"]
            else:
                bal_data = self.client.get_balance("USDT")
                if bal_data:
                    self.state.balance = bal_data["equity"]
                    self.state.available_balance = bal_data["available"]
                    self.state.equity = bal_data["equity"]
                else:
                    self.state.balance = 0.0
            self.state.peak_balance = self.state.balance

        mode = "🔵 DRY RUN" if self.dry_run else "🟢 LIVE"
        log.info(f"{mode} | Balance: ${self.state.balance:,.2f} USDT | Symbol: {self.cfg['SYMBOL']}")
        
        # Simpan state setelah init/transisi agar perubahan langsung tersimpan ke disk
        self.persistence.save(self.state, self.dry_run)
        
        # Sanitasi konfigurasi agar aman
        self._sanitize_config()

    def _sanitize_config(self):
        """Memastikan parameter konfigurasi berada dalam batas aman."""
        c = self.cfg
        # Proteksi Risk per Trade (Capped at 25%)
        if c.get("RISK_PER_TRADE", 0) > 0.25:
            log.warning("⚠️ RISK_PER_TRADE terlalu tinggi! Di-reset ke 5% demi keamanan.")
            c["RISK_PER_TRADE"] = 0.05
        
        # Proteksi Leverage (Capped at 125x)
        if c.get("LEVERAGE", 0) > 125:
            log.warning("⚠️ LEVERAGE tidak masuk akal! Di-reset ke 20x.")
            c["LEVERAGE"] = 20
        
        # Proteksi Max Margin per Trade (Capped at 50%)
        if c.get("MAX_MARGIN_PCT", 0) > 0.5:
            log.warning("⚠️ MAX_MARGIN_PCT terlalu tinggi! Di-reset ke 15%.")
            c["MAX_MARGIN_PCT"] = 0.15
            
        # Proteksi Daily Loss (Capped at 50%)
        if c.get("MAX_DAILY_LOSS_PCT", 0) > 0.5:
            log.warning("⚠️ MAX_DAILY_LOSS_PCT terlalu tinggi! Di-reset ke 15%.")
            c["MAX_DAILY_LOSS_PCT"] = 0.15

    def _gen_trade_id(self) -> str:
        self._trade_id_counter += 1
        return f"T{datetime.now(WIB).strftime('%Y%m%d%H%M%S')}-{self._trade_id_counter:04d}"

    # ── Price Update Callback (dari WebSocket) ────────────────

    def _on_price_update(self, price: float):
        """Dipanggil setiap harga baru dari WebSocket. Handle trailing stop real-time."""
        self._price_history.append({"price": price, "ts": time.time()})
        for pos in self.state.positions:
            if pos.closed:
                continue
            # Update trailing stop
            new_sl = self.risk.update_trailing_stop(pos, price)
            if new_sl:
                effective_sl = pos.trailing_stop
                log.debug(f"[TRAIL] {pos.id} SL diperbarui → ${effective_sl:.2f}")

            # ── 1. Cek Fast Risk Reduction (Squeeze Minus) ──
            if self.cfg.get("USE_FAST_RISK_REDUCTION") and not pos.risk_reduced and not pos.be_hit:
                profit_pct = 0.0
                if pos.side == "LONG":
                    profit_pct = (price - pos.entry_price) / pos.entry_price
                else:
                    profit_pct = (pos.entry_price - price) / pos.entry_price
                
                if profit_pct >= self.cfg.get("FAST_REDUCTION_PCT", 0.001):
                    pos.risk_reduced = True
                    # Geser SL 50% lebih dekat ke Entry (mengurangi resiko 50%)
                    if pos.side == "LONG":
                        dist = abs(pos.entry_price - pos.stop_loss)
                        pos.stop_loss = pos.entry_price - (dist * 0.5)
                    else:
                        dist = abs(pos.stop_loss - pos.entry_price)
                        pos.stop_loss = pos.entry_price + (dist * 0.5)
                    
                    log.info(f"[RISK] {pos.id} Profit 0.1% — Risk Reduced 50% (SL: ${pos.stop_loss:.2f})")
                    self.notifier.send(f"🛡️ *Risk Reduction*\n`{pos.id}`\nProfit mencapai 0.1%. SL dipindah 50% lebih dekat ke Entry untuk mengurangi resiko.")

            # ── 2. Cek Break-Even (BE) ──
            if self.cfg.get("USE_BE_FILTER") and not pos.be_hit and not pos.tp1_hit:
                profit_pct = 0.0
                if pos.side == "LONG":
                    profit_pct = (price - pos.entry_price) / pos.entry_price
                else:
                    profit_pct = (pos.entry_price - price) / pos.entry_price
                
                if profit_pct >= self.cfg.get("BE_ACTIVATION_PCT", 0.005):
                    pos.be_hit = True
                    # Tambahkan safeguard offset agar tidak exit minus karena slippage/ticking
                    offset = self.cfg.get("BE_SAFEGUARD_PCT", 0.0002)
                    if pos.side == "LONG":
                        pos.stop_loss = pos.entry_price * (1 + offset)
                    else:
                        pos.stop_loss = pos.entry_price * (1 - offset)
                    
                    log.info(f"[BE] {pos.id} Profit menyentuh BE Activation — SL pindah ke Entry + Offset ${pos.stop_loss:.2f}")
                    self.notifier.send(f"🛡️ *Break-Even Aktif*\n`{pos.id}`\nProfit mencapai target BE. SL dipindah ke Entry + Safeguard.")
                    
                    # UPDATE HARD SL DI EXCHANGE
                    if not self.dry_run:
                        self.client.cancel_all_orders(pos.symbol)
                        
                        # One-Way Mode: Close LONG=3, Close SHORT=1
                        # Hedge Mode: Close LONG=4, Close SHORT=2
                        is_one_way = self.cfg.get("ONE_WAY_MODE", True)
                        if is_one_way:
                            side_map = {"LONG": 3, "SHORT": 1}
                        else:
                            side_map = {"LONG": 4, "SHORT": 2}
                        
                        # Apply precision reset for SL
                        info = self._get_cached_contract_info(pos.symbol)
                        pr_scale = info.get("priceScale", 4)
                        sl_rounded = round(pos.stop_loss, pr_scale)
                        
                        exchange_max_lev = info.get("maxLeverage", self.cfg["LEVERAGE"])
                        effective_lev = min(self.cfg["LEVERAGE"], exchange_max_lev)
                        m_mode = 2 if self.cfg.get("MARGIN_MODE") == "CROSS" else 1
                        
                        self.client.place_stop_order(pos.symbol, side_map[pos.side], sl_rounded, pos.quantity, effective_lev, m_mode)

            # Cek SL real-time
            sl = pos.trailing_stop if pos.trailing_active else pos.stop_loss
            if pos.side == "LONG" and price <= sl:
                reason = "Trailing SL" if pos.trailing_active else "Stop Loss"
                self._close_position(pos, price, reason)
            elif pos.side == "SHORT" and price >= sl:
                reason = "Trailing SL" if pos.trailing_active else "Stop Loss"
                self._close_position(pos, price, reason)

    # ── Daily Reset ───────────────────────────────────────────

    def _check_daily_reset(self):
        today = datetime.now(WIB).strftime("%Y-%m-%d")
        if self.state.daily_reset_date != today:
            if self.state.daily_reset_date:
                log.info(f"Daily reset — PnL kemarin: ${self.state.daily_pnl:+,.2f}")
            self.state.daily_pnl       = 0.0
            self.state.daily_start_balance = self.state.balance
            self.state.daily_reset_date = today

    # ── Circuit Breaker ───────────────────────────────────────

    def _check_circuit_breaker(self) -> bool:
        """Cek apakah bot harus berhenti trading (risk management level atas)."""
        if self.state.circuit_breaker:
            return True

        c = self.cfg

        # Daily loss limit
        # Gunakan daily_start_balance sebagai basis (fallback ke balance sblmnya jika belum ada)
        basis = self.state.daily_start_balance if self.state.daily_start_balance > 0 else (self.state.balance + abs(self.state.daily_pnl))
        daily_loss_pct = abs(self.state.daily_pnl) / max(basis, 1)
        if self.state.daily_pnl < 0 and daily_loss_pct >= c["MAX_DAILY_LOSS_PCT"]:
            self.state.circuit_breaker = True
            self.state.circuit_triggered_at = time.time()
            self.state.circuit_type = "AUTO"
            self.state.circuit_reason  = f"Daily loss limit {c['MAX_DAILY_LOSS_PCT']*100:.0f}% tercapai"
            log.warning(f"⛔ CIRCUIT BREAKER: {self.state.circuit_reason}")
            # PERBAIKAN: CB TIDAK lagi menutup posisi aktif.
            # Biarkan SL/TP yang mengelola posisi. CB hanya BLOKIR ENTRY BARU.
            self.notifier.send(f"⛔ *Circuit Breaker Aktif*\n{self.state.circuit_reason}\nEntry baru diblokir. Posisi aktif tetap berjalan dengan SL/TP.")
            return True

        # Max drawdown
        dd = self.state.drawdown()
        if dd >= c["MAX_DRAWDOWN_PCT"] * 100:
            self.state.circuit_breaker = True
            self.state.circuit_triggered_at = time.time()
            self.state.circuit_type = "AUTO"
            self.state.circuit_reason  = f"Max drawdown {dd:.1f}% tercapai"
            log.warning(f"⛔ CIRCUIT BREAKER: {self.state.circuit_reason}")
            # PERBAIKAN: CB TIDAK lagi menutup posisi aktif.
            # Biarkan SL/TP yang mengelola posisi. CB hanya BLOKIR ENTRY BARU.
            self.notifier.send(f"⛔ *Circuit Breaker Aktif*\n{self.state.circuit_reason}\nEntry baru diblokir. Posisi aktif tetap berjalan dengan SL/TP.")
            return True

        return False

    # ── Fetch & Analyze ───────────────────────────────────────

    def _is_candle_closed(self, df: pd.DataFrame, tolerance_sec: int = 3) -> bool:
        """[SCALPING] Pastikan candle terakhir sudah tutup sebelum entry.
        Mencegah false signal dari candle yang masih berjalan (live candle).
        """
        tf_map = {
            "1m": 60, "3m": 180, "5m": 300,
            "15m": 900, "1h": 3600, "4h": 14400
        }
        tf_sec = tf_map.get(self.cfg["PRIMARY_TF"], 60)

        try:
            last_candle_time = df.index[-1].timestamp()
            expected_close   = last_candle_time + tf_sec
            now              = time.time()
            is_closed = now >= (expected_close - tolerance_sec)
            if not is_closed:
                remaining = expected_close - now
                log.info(f"⏳ [Candle Filter] Candle belum tutup — sisa {remaining:.0f}s")
            return is_closed
        except Exception as e:
            log.warning(f"Candle closed check error (non-fatal): {e}")
            return True  # Jika gagal cek, biarkan lanjut

    def _fetch_df(self, tf: str) -> Optional[pd.DataFrame]:
        log.info(f"🔍 [Heartbeat] Mengambil candle {tf}...")
        df = self.client.get_klines(self.cfg["SYMBOL"], tf, self.cfg["CANDLE_LIMIT"])
        if df is None or len(df) < 5:
            log.error(f"Data candle {tf} tidak cukup ({len(df) if df is not None else 0})")
            return None
        
        log.info(f"📊 [Heartbeat] Menghitung indikator {tf} ({len(df)} bar)...")
        df = self.ta.compute(df)
        df.dropna(inplace=True)
        return df

    def fetch_and_analyze(self) -> Optional[dict]:
        log.info(f"🤖 [Heartbeat] Iterasi #{self.state.iteration}: Memulai analisis market...")
        df_primary = self._fetch_df(self.cfg["PRIMARY_TF"])
        if df_primary is None:
            log.warning("⚠️ [Heartbeat] Gagal mengambil data primary candle.")
            return None

        log.info("🎯 [Heartbeat] Menghitung sinyal indikator...")
        # Lewatkan bot state agar bisa diakses oleh Loss Streak Cooldown
        signal = self.ta.get_signal(df_primary, bot_state=self.state)

        # ── [SCALPING] Candle Closed Filter ──────────────────
        # Jangan entry di candle yang masih berjalan — cegah false signal
        # KECUALI jika ALLOW_FAST_REENTRY aktif dan kita tidak sedang punya posisi
        can_fast_reentry = self.cfg.get("ALLOW_FAST_REENTRY", False) and self.open_positions_count() == 0
        
        if not self._is_candle_closed(df_primary) and not can_fast_reentry:
            signal["signal"]      = "NEUTRAL"
            signal["hold_reason"] = "⏳ Candle belum tutup — menunggu konfirmasi final"
            log.info("🚫 [Candle Filter] Sinyal diblokir — candle primary belum closed")
        elif not self._is_candle_closed(df_primary) and can_fast_reentry:
            log.info("⚡ [Fast Re-entry] Candle belum tutup tapi Fast Re-entry aktif & posisi kosong. Diizinkan.")

        # Multi-timeframe confirmation
        htf_bias = "NEUTRAL"
        if self.cfg["REQUIRE_MTF_CONFIRM"]:
            df_htf = self._fetch_df(self.cfg["CONFIRM_TF"])
            if df_htf is not None:
                htf_bias = self.ta.get_htf_bias(df_htf)

        signal["htf_bias"] = htf_bias

        # MTF filter: jika HTF bertentangan, lemahkan sinyal
        if self.cfg["REQUIRE_MTF_CONFIRM"]:
            if signal["signal"] == "LONG" and htf_bias == "BEARISH":
                signal["signal"] = "NEUTRAL"
                signal["blocked_reason"] = f"⚠️ Konflik Tren: 1m LONG vs 3m BEARISH"
                signal["hold_reason"] = signal["blocked_reason"]
            elif signal["signal"] == "SHORT" and htf_bias == "BULLISH":
                signal["signal"] = "NEUTRAL"
                signal["blocked_reason"] = f"⚠️ Konflik Tren: 1m SHORT vs 3m BULLISH"
                signal["hold_reason"] = signal["blocked_reason"]
            elif signal["signal"] != "NEUTRAL" and htf_bias == "NEUTRAL":
                # Jika 1m ada sinyal tapi 3m masih bingung, kita tunggu juga agar aman
                signal["signal"] = "NEUTRAL"
                signal["blocked_reason"] = "⏳ Menunggu Konfirmasi Timeframe 3m (MTF)"
                signal["hold_reason"] = signal["blocked_reason"]

        # ── [POWER UPGRADE 3] Momentum Acceleration Gate ──
        if self.cfg.get("USE_MOMENTUM_GATE") and signal["signal"] != "NEUTRAL":
            macd_desc = signal["details"].get("MACD", "")
            # Jika sinyal LONG tapi momentum MACD Bearish/Top rejection -> blokir
            if signal["signal"] == "LONG" and "Bearish momentum" in macd_desc:
                signal["signal"] = "NEUTRAL"
                signal["hold_reason"] = "📉 Momentum MACD turun (Blokir LONG pucuk)"
            # Jika sinyal SHORT tapi momentum MACD Bullish/Bottom bounce -> blokir
            elif signal["signal"] == "SHORT" and "Bullish momentum" in macd_desc:
                signal["signal"] = "NEUTRAL"
                signal["hold_reason"] = "📈 Momentum MACD naik (Blokir SHORT bawah)"

        # Gunakan harga WebSocket jika tersedia
        ws_price = self.price_feed.get_price()
        if ws_price > 0:
            signal["live_price"] = ws_price
        else:
            signal["live_price"] = signal["close"]

        self._last_signal = signal
        
        # [NEW] Sync positions & Balance from MEXC if Live to avoid orphan positions and balance drift
        if not self.dry_run: # Sync EVERY iteration for fast detection
            self.sync_positions_from_mexc()
            
            # SINKRONISASI SALDO REAL-TIME (ROBUST)
            bal_data = self.client.get_balance("USDT")
            if bal_data:
                new_equity = bal_data["equity"]
                # PROTEKSI: Jangan update balance jika API mengembalikan 0
                # (sering terjadi saat dana di Spot atau API timeout)
                if new_equity > 0.01:  # Minimal $0.01 agar tidak false
                    self.state.balance = new_equity
                    self.state.available_balance = bal_data["available"]
                    self.state.equity = new_equity
                    # Update peak jika menyentuh ATH baru
                    if self.state.balance > self.state.peak_balance:
                        self.state.peak_balance = self.state.balance
                else:
                    log.warning(f"⚠️ API equity = ${new_equity:.6f} — terlalu kecil, SKIP update balance (pakai balance terakhir ${self.state.balance:.2f})")
                
                # Update Spot also
                try:
                    self.state.spot_balance = self.spot_client.get_spot_balance("USDT")
                    all_f = self.client.get_all_balances()
                    total_f = 0.0
                    for ab in all_f:
                        total_f += float(ab.get("equity") or ab.get("marginBalance") or ab.get("totalMarginBalance") or ab.get("cashBalance") or 0.0)
                    self.state.futures_total_equity = total_f
                except: pass
            
        return signal

    def sync_positions_from_mexc(self):
        """Menarik data posisi aktual dari MEXC dan mengsinkronkannya dengan memori bot.
        
        PENTING (One-Way Mode): MEXC API mengembalikan 2 record per simbol
        (LONG placeholder + SHORT placeholder). Hanya record dengan holdVol > 0
        yang merupakan posisi aktif. Kita harus filter dan dedup berdasarkan
        (symbol, side), bukan hanya symbol.
        """
        if self.dry_run:
            return

        try:
            mexc_positions = self.client.get_open_positions()
            if mexc_positions is None:
                return

            # Buat set (symbol, side) dari posisi aktif di memori bot
            current_pos_keys = set()
            for p in self.state.positions:
                if not p.closed:
                    current_pos_keys.add((p.symbol, p.side))
            
            # Buat set (symbol, side) dari posisi aktif di MEXC (holdVol > 0)
            mexc_active_keys = set()
            mexc_data = {}  # {(symbol, side): {entry, qty}} untuk lookup PnL
            
            for mp in mexc_positions:
                symbol = mp.get("symbol")
                pos_type = mp.get("positionType")
                side = "LONG" if pos_type == 1 else "SHORT"
                qty = float(mp.get("holdVol", 0))
                
                # KRITIS: Skip record dengan holdVol = 0 (placeholder One-Way mode)
                if qty <= 0:
                    continue
                
                mexc_active_keys.add((symbol, side))
                mexc_data[(symbol, side)] = {
                    "entry": float(mp.get("holdAvgPrice") or mp.get("openAvgPrice") or 0),
                    "qty": qty,
                    "unrealised_pnl": float(mp.get("unrealisedPnl", 0)),
                }
                
                # Jika (symbol, side) ini belum ada di pantauan bot, adopsi
                if (symbol, side) not in current_pos_keys:
                    entry = float(mp.get("holdAvgPrice") or mp.get("openAvgPrice") or 0)
                    info = self._get_cached_contract_info(symbol)
                    contract_size = info.get("contractSize", 1.0)
                    
                    # Coba cari SL/TP yang sudah terpasang di MEXC untuk sinkronisasi
                    log.info(f"🔍 [Sync] Mencari Trigger Orders untuk {symbol}...")
                    stop_orders = self.client.get_stop_orders(symbol)
                    found_sl = 0.0
                    found_tp = 0.0
                    
                    if stop_orders:
                        for so in stop_orders:
                            # Filter order yang belum tereksekusi (state 1=wait, 2=triggered)
                            # side 3/4 biasanya untuk nutup posisi
                            s_price = float(so.get("triggerPrice", 0))
                            s_side = so.get("side") # 3 untuk close long, 4 untuk close short (One-Way)
                            
                            # Identifikasi apakah ini SL atau TP berdasarkan jarak dari entry
                            if side == "LONG":
                                if s_price < entry: found_sl = s_price
                                else: found_tp = s_price
                            else:
                                if s_price > entry: found_sl = s_price
                                else: found_tp = s_price

                    # Jika tidak ditemukan di bursa, hitung SL/TP "Safety" berdasarkan ATR saat ini
                    if found_sl == 0 or found_tp == 0:
                        log.info(f"⚠️ [Sync] Trigger tidak ditemukan di bursa. Menghitung profil risiko baru...")
                        klines = self.client.get_klines(symbol, self.cfg["PRIMARY_TF"], 20)
                        if not klines.empty:
                            ta_data = self.ta.compute(klines)
                            atr = ta_data["atr"].iloc[-1]
                            if side == "LONG":
                                if found_sl == 0: found_sl = entry - (atr * self.cfg.get("ATR_SL_MULT", 1.5))
                                if found_tp == 0: found_tp = entry + (atr * self.cfg.get("ATR_TP1_MULT", 1.5))
                            else:
                                if found_sl == 0: found_sl = entry + (atr * self.cfg.get("ATR_SL_MULT", 1.5))
                                if found_tp == 0: found_tp = entry - (atr * self.cfg.get("ATR_TP1_MULT", 1.5))

                    trade_id = f"SYNC-{symbol}-{int(time.time())}"
                    new_pos = Position(
                        id=trade_id,
                        symbol=symbol,
                        side=side,
                        entry_price=entry,
                        quantity=qty,
                        contract_size=contract_size,
                        stop_loss=found_sl, 
                        take_profit1=found_tp,
                        take_profit2=found_tp * 1.05 if side == "LONG" else found_tp * 0.95, # Estimasi TP2
                        take_profit3=found_tp * 1.10 if side == "LONG" else found_tp * 0.90, # Estimasi TP3
                        opened_at=datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"),
                        order_id=str(mp.get("positionId", ""))
                    )
                    self.state.positions.append(new_pos)
                    current_pos_keys.add((symbol, side))
                    log.info(f"🔄 [Sync] Posisi diadopsi: {symbol} {side} (SL: {found_sl}, TP: {found_tp})")
                    self.notifier.send(
                        f"🔄 *Posisi Diadopsi*\n"
                        f"Bot menemukan posisi `{symbol}` {side} di MEXC.\n"
                        f"SL: `${found_sl:.4f}` | TP: `${found_tp:.4f}`"
                    )
            
            # Cek jika ada posisi di memori tapi sudah hilang di MEXC
            # Ini berarti posisi sudah ditutup di MEXC (manual/trigger/TP/SL)
            for p in self.state.positions:
                if not p.closed and (p.symbol, p.side) not in mexc_active_keys:
                    log.warning(f"⚠️ [Sync] Posisi {p.symbol} {p.side} hilang dari MEXC → ditutup external!")
                    
                    # PERBAIKAN KRITIS: Ambil harga SPESIFIK untuk koin posisi ini
                    # BUKAN dari ws_price (yang track koin aktif bot saat ini)
                    close_price = p.entry_price  # Default: entry price (PnL = 0)
                    try:
                        ticker_data = self.client.get_ticker(p.symbol)
                        if ticker_data and ticker_data.get("last", 0) > 0:
                            close_price = ticker_data["last"]
                            log.info(f"📊 [Sync] Harga penutupan {p.symbol}: ${close_price}")
                        else:
                            log.warning(f"⚠️ [Sync] Gagal ambil harga {p.symbol}, PnL diset 0 (aman)")
                    except Exception as e:
                        log.warning(f"⚠️ [Sync] Error ambil harga {p.symbol}: {e}, PnL diset 0")
                    
                    # Hitung PnL berdasarkan harga koin yang benar
                    if p.side == "LONG":
                        pnl = (close_price - p.entry_price) * p.quantity * p.contract_size
                    else:
                        pnl = (p.entry_price - close_price) * p.quantity * p.contract_size
                    
                    # SANITY CHECK: PnL tidak boleh melebihi 50% dari balance
                    # Jika melebihi, kemungkinan harga salah — set PnL = 0
                    if abs(pnl) > self.state.balance * 0.5 and self.state.balance > 0:
                        log.warning(f"⚠️ [Sync] PnL ${pnl:.4f} terlalu besar (>50% balance). Kemungkinan harga salah. Set PnL = 0.")
                        pnl = 0.0
                        close_price = p.entry_price
                    
                    # Update state
                    p.pnl = round(p.pnl + pnl, 4)
                    p.closed = True
                    p.close_reason = "External Close (MEXC Trigger/Manual)"
                    p.closed_at = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")
                    
                    self.state.total_trades += 1
                    self.state.total_pnl += pnl
                    self.state.daily_pnl += pnl
                    if pnl > 0:
                        self.state.winning_trades += 1
                    
                    self.journal.log_trade(p, close_price, self._last_signal)
                    self.persistence.save(self.state, self.dry_run)
                    
                    emoji = "✅" if pnl > 0 else ("❌" if pnl < 0 else "⚪")
                    log.info(f"[SYNC CLOSE {p.side}] {p.id} | PnL: {emoji} ${pnl:+.4f}")
                    self.notifier.send(
                        f"{emoji} *Posisi Ditutup (External)*\n"
                        f"ID: `{p.id}`\n"
                        f"Pair: `{p.symbol}` {p.side}\n"
                        f"PnL: `${pnl:+.4f} USDT`\n"
                        f"Alasan: External Close (Trigger/Manual di MEXC)"
                    )

                    # KRITIS: Bersihkan trigger orders lama (SL/TP) yang masih ada di bursa
                    # agar tidak mengganggu posisi baru berikutnya
                    try:
                        self.client.cancel_all_orders(p.symbol)
                        log.info(f"🧹 [Sync] Trigger orders lama untuk {p.symbol} dibersihkan")
                    except Exception as ce:
                        log.warning(f"⚠️ [Sync] Gagal bersihkan trigger: {ce}")

                    # Update cooldown agar re-entry tidak terlalu cepat
                    self.state.last_close_time = time.time()
                    if pnl < 0:
                        self.state.consecutive_losses += 1
                        self.state.last_loss_side = p.side
                    else:
                        self.state.consecutive_losses = 0

        except Exception as e:
            log.debug(f"Sync positions error: {e}")


    # ── Posisi Management ─────────────────────────────────────

    def open_positions_count(self) -> int:
        return sum(1 for p in self.state.positions if not p.closed)

    def close_all_positions(self, reason: str = "Manual Stop"):
        """Tutup semua posisi terbuka segera."""
        active_positions = [p for p in self.state.positions if not p.closed]
        if not active_positions:
            log.info("Tidak ada posisi terbuka untuk ditutup.")
            return

        log.info(f"Menutup {len(active_positions)} posisi terbuka karena: {reason}")
        
        # Ambil harga terbaru dari feed jika ada
        price = self.price_feed.get_price()
        
        for pos in active_positions:
            # Jika price feed mati, gunakan entry price (PnL 0) sebagai fallback aman
            close_price = price if price > 0 else pos.entry_price
            self._close_position(pos, close_price, reason)
        
        log.info("Semua posisi telah ditutup.")

    def check_exits_candle(self, signal: dict):
        """Cek exit berdasarkan candle (TP/SL) dan Signal Flip."""
        price = signal["live_price"]
        current_sig = signal["signal"]
        
        for pos in self.state.positions:
            if pos.closed:
                continue
            
            # ── 1. Cek Signal Flip (Early Exit) ──
            # PERBAIKAN: Signal Flip hanya berlaku jika posisi SEDANG RUGI.
            # Jika posisi profit, biarkan TP/Trailing Stop yang mengelola.
            if self.cfg.get("EXIT_ON_SIGNAL_FLIP"):
                is_losing = False
                if pos.side == "LONG":
                    is_losing = price < pos.entry_price
                elif pos.side == "SHORT":
                    is_losing = price > pos.entry_price
                
                if is_losing:  # Hanya flip-exit jika posisi sedang rugi
                    if pos.side == "LONG" and current_sig == "SHORT":
                        self._close_position(pos, price, "Signal Flip (Trend Reversal 🔴)")
                        signal["flip_occurred"] = True
                        continue
                    elif pos.side == "SHORT" and current_sig == "LONG":
                        self._close_position(pos, price, "Signal Flip (Trend Reversal 🟢)")
                        signal["flip_occurred"] = True
                        continue

            # ── 1b. Cek Weak Signal Exit (Squeeze Minus) ──
            # PERBAIKAN v2: Weak Signal Exit HANYA BERLAKU JIKA POSISI SEDANG RUGI.
            # Jika posisi masih profit (belum kena entry), biarkan TP/BE/Trailing yang mengelola.
            # Grace period juga dinaikkan agar tidak terlalu sensitif (12 iterasi = ~1 menit).
            weak_grace = self.cfg.get("WEAK_SIGNAL_GRACE", 12)  # Default 12 iterasi (~1 menit)
            
            # Cek apakah posisi sedang rugi
            is_losing_weak = False
            if pos.side == "LONG":
                is_losing_weak = price < pos.entry_price
            elif pos.side == "SHORT":
                is_losing_weak = price > pos.entry_price
            
            if is_losing_weak:  # HANYA exit Weak Signal jika posisi sedang RUGI
                if pos.side == "LONG":
                    score = signal.get("bull_score", 0)
                    thresh = self.cfg.get("EXIT_MIN_BULL_SCORE", 2)
                    if score < thresh:
                        pos.weak_signal_count += 1
                        if pos.weak_signal_count >= weak_grace:
                            self._close_position(pos, price, f"Weak Signal (Bull {score} < {thresh}, {pos.weak_signal_count}x) ⚠️")
                            continue
                        else:
                            log.info(f"⚠️ [Weak] {pos.id} Bull score {score} < {thresh} ({pos.weak_signal_count}/{weak_grace})")
                    else:
                        pos.weak_signal_count = 0  # Reset counter jika sinyal kembali kuat
                elif pos.side == "SHORT":
                    score = signal.get("bear_score", 0)
                    thresh = self.cfg.get("EXIT_MIN_BEAR_SCORE", 2)
                    if score < thresh:
                        pos.weak_signal_count += 1
                        if pos.weak_signal_count >= weak_grace:
                            self._close_position(pos, price, f"Weak Signal (Bear {score} < {thresh}, {pos.weak_signal_count}x) ⚠️")
                            continue
                        else:
                            log.info(f"⚠️ [Weak] {pos.id} Bear score {score} < {thresh} ({pos.weak_signal_count}/{weak_grace})")
                    else:
                        pos.weak_signal_count = 0  # Reset counter jika sinyal kembali kuat
            else:
                # Posisi masih profit → reset weak counter, biarkan TP/Trailing yang kelola
                if pos.weak_signal_count > 0:
                    log.info(f"✅ [Weak Reset] {pos.id} — Posisi masih profit, weak counter di-reset (was {pos.weak_signal_count})")
                    pos.weak_signal_count = 0

            # ── 2. Cek Take Profit ──
            if pos.side == "LONG":
                if not pos.tp1_hit and price >= pos.take_profit1:
                    pos.tp1_hit  = True
                    pos.stop_loss = pos.entry_price  # move to BE otomatis di TP1
                    
                    log.info(f"[TP1] {pos.id} @ ${price:.2f} — SL pindah ke BE ${pos.entry_price:.2f}")
                    
                    # Logika Partial Close
                    if self.cfg.get("TP1_PARTIAL_CLOSE"):
                        self._partial_close_position(pos, price, "Partial Take Profit (TP1) ✅")
                    else:
                        self.notifier.send(
                            f"🎯 *TP1 Tercapai!*\n`{pos.id}` LONG\nHarga: ${price:.2f}\nSL → Break-even: ${pos.entry_price:.2f}"
                        )
                        
                if pos.tp1_hit and price >= pos.take_profit2 and not pos.tp2_hit:
                    pos.tp2_hit = True
                    if self.cfg.get("TP2_PARTIAL_CLOSE"):
                        self._partial_close_position(pos, price, "Partial Take Profit (TP2) ✅✅")
                    else:
                        self._close_position(pos, price, "TP2 ✅✅")
                elif pos.tp2_hit and price >= pos.take_profit3:
                    self._close_position(pos, price, "TP3 ✅✅✅")
            else:
                if not pos.tp1_hit and price <= pos.take_profit1:
                    pos.tp1_hit   = True
                    pos.stop_loss = pos.entry_price
                    
                    log.info(f"[TP1] {pos.id} @ ${price:.2f} — SL pindah ke BE")
                    
                    # Logika Partial Close
                    if self.cfg.get("TP1_PARTIAL_CLOSE"):
                        self._partial_close_position(pos, price, "Partial Take Profit (TP1) ✅")
                    else:
                        self.notifier.send(f"🎯 *TP1 Tercapai!*\n`{pos.id}` SHORT\nSL → Break-even")

                if pos.tp1_hit and price <= pos.take_profit2 and not pos.tp2_hit:
                    pos.tp2_hit = True
                    if self.cfg.get("TP2_PARTIAL_CLOSE"):
                        self._partial_close_position(pos, price, "Partial Take Profit (TP2) ✅✅")
                    else:
                        self._close_position(pos, price, "TP2 ✅✅")
                elif pos.tp2_hit and price <= pos.take_profit3:
                    self._close_position(pos, price, "TP3 ✅✅✅")

    def _partial_close_position(self, pos: Position, price: float, reason: str):
        """Menutup sebagian posisi untuk mengamankan profit."""
        if pos.closed:
            return
            
        # Tentukan rasio berdasarkan level TP
        if "TP2" in reason:
            ratio_key = "TP2_CLOSE_PCT"
        else:
            ratio_key = "TP1_CLOSE_PCT"
            
        close_ratio = self.cfg.get(ratio_key, 50) / 100.0
        qty_to_close = pos.quantity * close_ratio
        
        # Realisasikan PnL untuk porsi yang ditutup (Gunakan estimasi fee)
        notional_exit = price * qty_to_close * pos.contract_size
        fee_est = notional_exit * self.cfg.get("FEE_RATE", 0.0006)
        
        if pos.side == "LONG":
            pnl_raw = (price - pos.entry_price) * qty_to_close * pos.contract_size
        else:
            pnl_raw = (pos.entry_price - price) * qty_to_close * pos.contract_size
            
        pnl = pnl_raw - fee_est
        
        # Update State
        self.state.total_pnl += pnl 
        self.state.daily_pnl += pnl
        self.state.balance   += pnl
        
        # Simpan pnl yang terealisasi ke objek posisi agar muncul di total dashboard nanti
        pos.pnl += pnl
        
        # Sisa posisi (Jika dry_run, potong di sini. Jika live, akan dipotong setelah berhasil di exchange)
        if self.dry_run:
            pos.quantity -= qty_to_close
        pos.partial_closed = True
        
        # Pindahkan SL ke Break-Even (Sesuai janji notifikasi)
        pos.stop_loss = pos.entry_price
        log.info(f"🛡️ [BREAK-EVEN] SL dipindahkan ke ${pos.stop_loss} untuk posisi {pos.id}")
        
        # UPDATE DI EXCHANGE (Live Mode)
        if not self.dry_run:
            log.info(f"📤 [LIVE PARTIAL] Menutup {close_ratio*100}% posisi {pos.symbol} di bursa...")
            
            # Deteksi Mode Satu Arah
            is_one_way = self.cfg.get("ONE_WAY_MODE", True)
            
            # Sesuaikan presisi volume untuk partial qty
            info = self._get_cached_contract_info(pos.symbol)
            vol_scale = info.get("volScale", 0)
            qty_close_final = round(qty_to_close, vol_scale)
            if vol_scale == 0: qty_close_final = int(qty_close_final)
            if qty_close_final < info.get("minVol", 1): qty_close_final = info.get("minVol", 1)

            if is_one_way:
                # One-way: tutup LONG = Sell (3), tutup SHORT = Buy (1)
                side_map_close = {"LONG": 3, "SHORT": 1}
            else:
                # Hedge: tutup LONG = 4 (Close Long), tutup SHORT = 2 (Close Short)
                side_map_close = {"LONG": 4, "SHORT": 2}
            
            # Ambil max leverage bursa agar tidak kena Code 2006
            exchange_max_lev = info.get("maxLeverage", self.cfg["LEVERAGE"])
            effective_lev = min(self.cfg["LEVERAGE"], exchange_max_lev)
            
            m_mode = 2 if self.cfg.get("MARGIN_MODE") == "CROSS" else 1
            res = self.client.place_order(pos.symbol, side_map_close[pos.side], 5, effective_lev, qty_close_final, m_mode)
            
            if res:
                log.info(f"✅ Partial Close (TP1) berhasil dieksekusi: {res}")
                # Update pos.quantity based on WHAT WAS ACTUALLY CLOSED at bursa
                pos.quantity = round(pos.quantity - qty_close_final, vol_scale)
                if vol_scale == 0: pos.quantity = int(pos.quantity)

                # 2. Update Hard Stop Loss di bursa untuk sisa Qty
                self.client.cancel_all_orders(pos.symbol)
                
                if is_one_way:
                    # One-Way SL tetap menggunakan side berlawanan
                    side_map_sl = {"LONG": 3, "SHORT": 1}
                else:
                    side_map_sl = {"LONG": 4, "SHORT": 2}
                
                # Sisa qty setelah pembulatan bursa
                remaining_qty_fixed = pos.quantity
                
                if remaining_qty_fixed > 0:
                    self.client.place_stop_order(pos.symbol, side_map_sl[pos.side], pos.stop_loss, remaining_qty_fixed, effective_lev, m_mode)
                    log.info(f"🛡️ Hard SL diperbarui untuk sisa {remaining_qty_fixed} unit.")
            else:
                log.error(f"❌ GAGAL eksekusi Partial Close di bursa! Coba lagi secara manual.")
                return # Jangan lanjut update state jika gagal di live

        
        log.info(f"[PARTIAL {pos.side}] {pos.id} | Closed {close_ratio*100}% | PnL Secured: ${pnl:.4f}")
        
        # Kirim notifikasi
        self.notifier.send(
            f"💰 *Partial Profit Secured*\n`{pos.id}` {pos.side}\n"
            f"Porsi ditutup: `{close_ratio*100}%`\n"
            f"Profit Aman: `${pnl:.4f}`\n"
            f"Sisa Qty: `{pos.quantity:.4f}` running with SL in Break-Even."
        )
        
        # Update balance asli jika Live Mode untuk menghindari drift
        if not self.dry_run:
            bal_data = self.client.get_balance("USDT")
            if bal_data is not None:
                self.state.balance = bal_data["equity"]
                self.state.available_balance = bal_data["available"]
                self.state.equity = bal_data["equity"]
                log.info(f"🔄 Balance Synced (Partial): Total ${self.state.equity:,.2f}")

        # Optional: Pindahkan profit ke Spot jika fitur aktif
        if pnl > 0 and self.cfg.get("ENABLE_AUTO_SECURE"):
            secure_amt = pnl * (self.cfg.get("SECURE_PROFIT_PCT", 50) / 100.0)
            if secure_amt >= self.cfg.get("MIN_SECURE_TRANSFER", 1.0):
                if self.dry_run:
                    self.state.balance -= secure_amt
                    self.state.secured_total += secure_amt
                else:
                    if self.spot_client.transfer_to_spot(secure_amt):
                        self.state.balance -= secure_amt
                        self.state.secured_total += secure_amt
        
        self.persistence.save(self.state, self.dry_run)

    def _close_position(self, pos: Position, price: float, reason: str):
        # Thread-safe: Cegah race condition antara PriceFeed dan MainLoop
        with self._close_lock:
            if pos.closed:
                return
            self._close_position_inner(pos, price, reason)

    def _close_position_inner(self, pos: Position, price: float, reason: str):
        """Inner close logic — harus dipanggil dari dalam self._close_lock."""
        # Kalkulasi PnL Bersih (Estimasi Fee)
        notional_entry = pos.entry_price * pos.quantity * pos.contract_size
        notional_exit  = price * pos.quantity * pos.contract_size
        fee_total = (notional_entry + notional_exit) * self.cfg.get("FEE_RATE", 0.0006)

        if pos.side == "LONG":
            pnl_raw = (price - pos.entry_price) * pos.quantity * pos.contract_size
        else:
            pnl_raw = (pos.entry_price - price) * pos.quantity * pos.contract_size
            
        pnl = pnl_raw - fee_total

        # Eksekusi Penutupan di Bursa (Live Mode)
        if not self.dry_run:
            log.info(f"📤 [LIVE CLOSE] Mengeksekusi Market Close untuk {pos.symbol}...")
            # 1. Batalkan semua SL/TP trigger orders yang tersisa
            self.client.cancel_all_orders(pos.symbol)
            
            # Deteksi Mode Satu Arah
            is_one_way = self.cfg.get("ONE_WAY_MODE", True)
            if is_one_way:
                # One-way: tutup LONG = Sell (3), tutup SHORT = Buy (1)
                close_side = 3 if pos.side == "LONG" else 1
            else:
                # Hedge: tutup LONG = 4 (Close Long), tutup SHORT = 2 (Close Short)
                close_side = 4 if pos.side == "LONG" else 2
            
            # Rounding quantity for final close
            info = self._get_cached_contract_info(pos.symbol)
            vol_scale = info.get("volScale", 0)
            final_qty = round(pos.quantity, vol_scale)
            if vol_scale == 0: final_qty = int(final_qty)

            # Ambil max leverage bursa agar tidak kena Code 2006
            exchange_max_lev = info.get("maxLeverage", self.cfg["LEVERAGE"])
            effective_lev = min(self.cfg["LEVERAGE"], exchange_max_lev)
            
            # 2. Kirim Market Order untuk menutup posisi
            m_mode = 2 if self.cfg.get("MARGIN_MODE") == "CROSS" else 1
            close_result = self.client.place_order(pos.symbol, close_side, 5, effective_lev, final_qty, m_mode)
            
            if not close_result:
                log.error(f"❌ GAGAL menutup posisi {pos.symbol} di bursa secara otomatis!")
                log.error(f"Posisi tetap 'OPEN' di memori bot untuk sinkronisasi berikutnya.")
                return # ABORT marking as closed! Let sync_positions fix it later.

            log.info(f"✅ Berhasil menutup posisi di bursa: {close_result}")

        # --- SEKARANG baru update state karena sudah dipastikan closed (atau Dry Run) ---
        pos.pnl          = round(pos.pnl + pnl, 4)
        pos.closed       = True
        pos.close_reason = reason
        pos.closed_at    = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S")

        self.state.total_trades  += 1
        self.state.total_pnl     += pnl
        self.state.daily_pnl     += pnl
        self.state.balance       += pnl
        self.state.last_close_time = time.time()
        
        if pnl > 0:
            self.state.winning_trades += 1
            self.state.consecutive_losses = 0
            self.state.last_loss_side = ""
        else:
            self.state.consecutive_losses += 1
            self.state.last_loss_side = pos.side
            
        if self.state.balance > self.state.peak_balance:
            self.state.peak_balance = self.state.balance

        emoji = "✅" if pnl > 0 else "❌"
        log.info(
            f"[CLOSE {pos.side}] {reason} | {pos.id} | "
            f"Entry: ${pos.entry_price:.6f} → Exit: ${price:.6f} | "
            f"PnL: {emoji} ${pnl:+.4f} | Balance: ${self.state.balance:,.2f}"
        )

        self.journal.log_trade(pos, price, self._last_signal)
        self.persistence.save(self.state, self.dry_run)


        # ─── Profit Stripping ────────────────────────────────
        if pnl > 0 and self.cfg.get("ENABLE_AUTO_SECURE"):
            secure_pct = self.cfg.get("SECURE_PROFIT_PCT", 50) / 100.0
            secure_amt = pnl * secure_pct
            if secure_amt >= self.cfg.get("MIN_SECURE_TRANSFER", 1.0):
                if self.dry_run:
                    # SIMULASI: Hanya update state agar tampil di dashboard
                    self.state.balance -= secure_amt
                    self.state.secured_total += secure_amt
                    self.persistence.save(self.state, self.dry_run)
                    log.info(f"🔵 [DRY RUN] Simulasi secure profit: ${secure_amt:.2f}")
                else:
                    # LIVE: Benar-benar transfer
                    if self.spot_client.transfer_to_spot(secure_amt):
                        self.state.balance -= secure_amt
                        self.state.secured_total += secure_amt
                        self.persistence.save(self.state, self.dry_run)
                        log.info(f"[SECURE] ${secure_amt:.2f} dipindahkan ke Spot. Sisa Balance: ${self.state.balance:.2f}")

        self.notifier.send(
            f"{emoji} *Posisi Ditutup*\n"
            f"ID: `{pos.id}`\n"
            f"Pair: `{self.cfg['SYMBOL']}` {pos.side}\n"
            f"Entry: `${pos.entry_price:.2f}` → Exit: `${price:.2f}`\n"
            f"PnL: `${pnl:+.4f} USDT`\n"
            f"Alasan: {reason}\n"
            f"Trailing: {'Ya' if pos.trailing_active else 'Tidak'}\n"
            f"Balance: `${self.state.balance:,.2f}`\n"
            f"Win Rate: `{self.state.win_rate():.1f}%`"
        )

        # State was already saved and notification sent.
        pass


    def open_position(self, side: str, signal: dict):
        if self.open_positions_count() >= self.cfg["MAX_OPEN_TRADES"]:
            msg = "Max posisi tercapai"
            log.info(msg)
            signal["hold_reason"] = msg
            return

        # ── [POWER UPGRADE 4] Smart Re-entry Blocker ──
        if self.cfg.get("USE_SMART_REENTRY"):
            # A) Blokir jika open terlalu cepat setelah close terakhir
            time_since_close = time.time() - self.state.last_close_time
            cooldown_sec = self.cfg.get("REENTRY_COOLDOWN_SEC", 120)
            if self.state.last_close_time > 0 and time_since_close < cooldown_sec:
                msg = f"⏳ Cooldown aktif: sisa {int(cooldown_sec - time_since_close)}s sebelum boleh re-entry"
                log.info(f"🚫 [Smart Re-entry] {msg}")
                signal["hold_reason"] = msg
                return
            
            # B) Blokir jika barusan loss di arah yang sama persis (Anti-Revenge Trading)
            if self.cfg.get("BLOCK_SAME_SIDE_LOSS") and self.state.consecutive_losses > 0:
                if side == self.state.last_loss_side:
                    msg = f"🚫 Blokir Re-entry {side}: Trade terakhir loss di side ini. Tunggu konfirmasi arah lain."
                    log.info(f"🚫 [Anti-Revenge] {msg}")
                    signal["hold_reason"] = msg
                    return

        # ── [ANTI-DUPLIKAT] Cegah buka posisi ganda di simbol+side yang sama ──
        # Di One-Way mode, order tambahan di side yang sama akan meng-average posisi di MEXC
        # tapi bot mengira itu posisi terpisah → desync fatal.
        target_symbol = self.cfg["SYMBOL"]
        for p in self.state.positions:
            if not p.closed and p.symbol == target_symbol and p.side == side:
                msg = f"Sudah ada posisi {side} aktif di {target_symbol} ({p.id}) — skip duplikat"
                log.info(f"🚫 [Anti-Duplikat] {msg}")
                signal["hold_reason"] = msg
                return

        try:
            entry  = signal["live_price"]
            atr    = signal["atr"]
            
            # ── [SANITY CHECK] Cegah Bug Harga/Saldo ───────
            # Jika harga entry berbeda lebih dari 5% dengan close candle terakhir (PRIMARY_TF),
            # kemungkinan ada anomali data (seperti bug harga = saldo).
            last_close = signal.get("close", 0)
            if last_close > 0:
                diff_pct = abs(entry - last_close) / last_close
                if diff_pct > 0.05: # Threshold 5%
                    msg = f"⚠️ Harga Anomali: Entry ${entry} vs Close ${last_close} (Diff {diff_pct*100:.1f}%)"
                    log.error(f"❌ [Sanity Check] {msg} — ENTRY DIBLOKIR!")
                    self.notifier.send(f"🚨 *Anomali Harga Terdeteksi*\nBot memblokir entri karena harga tidak wajar.\nEntry: `{entry}`\nClose: `{last_close}`")
                    signal["hold_reason"] = "Anomali Harga"
                    return

            levels = self.risk.calculate_levels(side, entry, atr, signal_score=signal.get("confidence", 50))

            if not self.risk.check_rr(levels):
                msg = f"RR too low ({levels['rr_ratio']:.2f})"
                log.info(f"{msg} — skip")
                signal["hold_reason"] = msg
                return

            # ── 3. Probabilitas TP Check ──
            prob_ok, prob_msg = self.risk.check_tp_probability(levels, signal)
            if not prob_ok:
                log.info(f"HOLD: {prob_msg}")
                # Kita simpan alasan hold di signal agar UI bisa menampilkan
                signal["hold_reason"] = prob_msg
                return
        except Exception as e:
            msg = f"Error kalkulasi level/prob: {e}"
            log.error(msg)
            signal["hold_reason"] = "Error Parameter"
            return

        # Ambil max leverage bursa UNTUK sizing agar tidak 'over-size' jika leverage disunat
        info = self._get_cached_contract_info(self.cfg["SYMBOL"])
        exchange_max_lev = info.get("maxLeverage", self.cfg["LEVERAGE"])
        effective_lev_sizing = min(self.cfg["LEVERAGE"], exchange_max_lev)

        qty = self.risk.position_size(
            self.state.balance,
            levels["sl_distance"],
            entry,
            effective_lev_sizing, # PASS EFFECTIVE LEV
            win_rate=self.state.win_rate() / 100,
            signal_confidence=signal.get("confidence", 50)
        )
        if qty <= 0:
            msg = "Position size = 0 (Check Risikonya atau Saldo Terlalu Kecil)"
            log.warning(msg)
            signal["hold_reason"] = msg
            return

        # Sizing: RiskManager already gave us 'notional' quantity (raw coins)
        info = self._get_cached_contract_info(self.cfg["SYMBOL"])
        contract_size = info.get("contractSize", 1)
        vol_scale = info.get("volScale", 0)
        
        # MEXC Vol = Raw Quantity / Contract Size
        qty_units = qty / contract_size
        qty_total = round(qty_units, vol_scale)
        
        if vol_scale == 0: qty_total = int(qty_total)
        
        if qty_total < info.get("minVol", 1): 
            qty_total = info.get("minVol", 1)
        
        # Ambil max leverage bursa agar tidak kena Code 2006
        exchange_max_lev = info.get("maxLeverage", self.cfg["LEVERAGE"])
        effective_lev = min(self.cfg["LEVERAGE"], exchange_max_lev)

        # Real cost = (units * contract_size * price) / leverage
        cost = (qty_total * contract_size * entry) / effective_lev
        usable_balance = self.state.available_balance if not self.dry_run else self.state.balance
        if cost > usable_balance * 0.95:
            msg = f"Cost ${cost:.2f} > Avail ${usable_balance:.2f} (Saldo Kurang)"
            log.warning(msg)
            signal["hold_reason"] = msg
            return
        
        # Min Notional Check (MEXC minimal ~$5)
        notional = qty_total * contract_size * entry
        if notional < 5.0:
            msg = f"Order terlalu kecil (${notional:.2f} < $5.0)"
            log.info(msg)
            signal["hold_reason"] = msg
            return

        trade_id = self._gen_trade_id()
        pos = Position(
            id=trade_id,
            symbol=self.cfg["SYMBOL"],
            side=side,
            entry_price=entry,
            quantity=qty_total,
            contract_size=contract_size,
            stop_loss=levels["stop_loss"],
            take_profit1=levels["take_profit1"],
            take_profit2=levels["take_profit2"],
            take_profit3=levels["take_profit3"],
            opened_at=datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S"),
            highest_price=entry,
            lowest_price=entry,
            entry_bull_score=signal.get("bull_score", 0),
            entry_bear_score=signal.get("bear_score", 0),
            entry_confidence=signal.get("confidence", 0),
            entry_rsi=signal.get("rsi", 0.0),
            entry_atr=signal.get("atr", 0.0),
        )

        if not self.dry_run:
            if self.cfg.get("ONE_WAY_MODE", True):
                # One-way: Buy=1, Sell=3
                side_map = {"LONG": 1, "SHORT": 3}
                side_map_sl = {"LONG": 3, "SHORT": 1}
            else:
                # Hedge mode
                side_map = {"LONG": 1, "SHORT": 3}
                side_map_sl = {"LONG": 4, "SHORT": 2}

            # Ambil max leverage bursa agar tidak kena Code 2006
            exchange_max_lev = info.get("maxLeverage", self.cfg["LEVERAGE"])
            effective_lev = min(self.cfg["LEVERAGE"], exchange_max_lev)
            
            m_mode = 2 if self.cfg.get("MARGIN_MODE") == "CROSS" else 1

            # KRITIS: Bersihkan semua trigger orders lama sebelum buka posisi baru
            # Ini mencegah orphaned SL/TP dari posisi sebelumnya mengganggu
            self.client.cancel_all_orders(self.cfg["SYMBOL"])
            log.info(f"🧹 Trigger orders lama dibersihkan sebelum open posisi baru")

            result = self.client.place_order(self.cfg["SYMBOL"], side_map[side], 5, effective_lev, qty_total, m_mode)
            if result:
                pos.order_id = str(result)
                log.info(f"Futures Order Placed: {result}")
                
                # PASANG HARD STOP LOSS DI EXCHANGE SEGERA
                sl_result = self.client.place_stop_order(pos.symbol, side_map_sl[pos.side], pos.stop_loss, pos.quantity, effective_lev, m_mode)
                if sl_result:
                    log.info(f"✅ Hard Stop Loss placed at exchange: ${pos.stop_loss}")
                else:
                    log.warning(f"⚠️ Gagal memasang Hard SL di bursa (Internal SL tetap aktif)")

                # PASANG HARD TAKE PROFIT (TP1) DI EXCHANGE SEGERA
                tp_result = self.client.place_stop_order(pos.symbol, side_map_sl[pos.side], pos.take_profit1, pos.quantity, effective_lev, m_mode, is_take_profit=True)
                if tp_result:
                    log.info(f"✅ Hard Take Profit (TP1) placed at exchange: ${pos.take_profit1}")
                else:
                    log.warning(f"⚠️ Gagal memasang Hard TP di bursa (Internal TP tetap aktif)")
            else:
                msg = "Gagal place order Futures (API Error)"
                log.error(msg)
                signal["hold_reason"] = msg
                return

        self.state.positions.append(pos)
        self.persistence.save(self.state, self.dry_run)

        log.info(
            f"[OPEN {side}] {trade_id} | Entry: ${entry:.6f} | "
            f"Qty: {qty_total} | SL: ${levels['stop_loss']:.6f} | "
            f"TP1: ${levels['take_profit1']:.6f} | TP2: ${levels['take_profit2']:.6f} | "
            f"RR: 1:{levels['rr_ratio']:.2f} | Conf: {signal['confidence']}%"
        )

        prefix = "🔵 [DRY RUN] " if self.dry_run else ""
        session_ok, session_name = self.session.is_trading_allowed()
        self.notifier.send(
            f"{prefix}🚀 *Posisi Dibuka*\n"
            f"ID: `{trade_id}`\n"
            f"Pair: `{self.cfg['SYMBOL']}` *{side}*\n"
            f"Entry: `${entry:.2f}`\n"
            f"Qty: `{qty_total} {self.cfg['SYMBOL'].split('_')[0]}` (Lev {self.cfg['LEVERAGE']}x)\n"
            f"Stop Loss: `${levels['stop_loss']:.2f}`\n"
            f"TP1: `${levels['take_profit1']:.2f}` | TP2: `${levels['take_profit2']:.2f}`\n"
            f"RR: `1:{levels['rr_ratio']:.2f}` | Conf: `{signal['confidence']}%`\n"
            f"Trailing: {'Aktif' if self.cfg['USE_TRAILING_STOP'] else 'Off'}\n"
            f"HTF Bias: `{signal.get('htf_bias','?')}`\n"
            f"Sesi: {session_name}\n"
            f"Bull: `{signal['bull_score']}` | Bear: `{signal['bear_score']}`"
        )


    # ── Signal Scanner (Background) ───────────────────────────

    def _run_signal_scanner(self):
        """Metode background untuk memindai koin teratas dan mencari sinyal entry."""
        log.info("📡 [Scanner] Background Signal Scanner dimulai...")
        while True:
            try:
                # 1. Ambil koin dengan volume tertinggi
                top_coins = self.client.get_top_volume_coins(limit=80)
                if not top_coins:
                    time.sleep(300)
                    continue

                log.info(f"📡 [Scanner] Memindai {len(top_coins)} koin untuk sinyal entry...")
                
                new_signals = {}
                for coin in top_coins:
                    symbol = coin["symbol"]
                    # Jangan scan koin yang sedang di-trade agar tidak bentrok atau redundant
                    if symbol == self.cfg["SYMBOL"]:
                        continue
                    
                    try:
                        # Ambil klines - Gunakan data historis jika API gagal sementara
                        df = self.client.get_klines(symbol, self.cfg["PRIMARY_TF"], limit=100)
                        
                        # Retrying sekali jika kena rate limit 510
                        if df is None:
                            time.sleep(5.0)
                            df = self.client.get_klines(symbol, self.cfg["PRIMARY_TF"], limit=100)

                        if df is not None and len(df) >= 35:
                            df = self.ta.compute(df)
                            sig = self.ta.get_signal(df)
                            
                            # Simpan sinyal dan ADX
                            res = {
                                "signal":     sig["signal"],
                                "bull":       sig["bull_score"],
                                "bear":       sig["bear_score"],
                                "price":      sig["close"],
                                "adx":        sig["adx"]
                            }
                            new_signals[symbol] = res
                            # Update secara incremental agar dashboard tidak kosong terlalu lama
                            self._scanned_signals[symbol] = res
                            
                            log.info(f"✅ [Scanner] Scan {symbol} Berhasil (ADX: {res['adx']:.1f})")
                            
                            # Simpan ke file sesekali agar tidak hilang saat restart
                            if len(new_signals) % 5 == 0:
                                try:
                                    with open("scanned_signals.json", "w") as f:
                                        json.dump(self._scanned_signals, f)
                                except: pass
                        else:
                            log.info(f"⏭️ [Scanner] Skip {symbol} (Alasan: Data candle tidak cukup/None)")

                        # Sleep antar koin (Diturunkan agar lebih cepat)
                        time.sleep(1.0)
                    except Exception as e:
                        log.warning(f"❌ [Scanner] Gagal scan {symbol}: {e}")
                
                log.info(f"📡 [Scanner] Satu putaran selesai. Berhasil memproses {len(new_signals)} koin.")
                
                # Tunggu 5 menit sebelum scan ulang (agar tidak spam API)
                time.sleep(300)
            except Exception as e:
                log.error(f"[Scanner] Error: {e}")
                time.sleep(60)


    # ── Status & Display ──────────────────────────────────────

    def print_status(self, signal: dict, monitoring_only: bool = False):
        try:
            ws_price = self.price_feed.get_price()
            live_price = ws_price if ws_price > 0 else signal.get("close", 0)
            ok, session = self.session.is_trading_allowed()
            bull = signal.get("bull_score", 0)
            bear = signal.get("bear_score", 0)
            ms = signal.get("max_score", 14)
            conf = signal.get("confidence", 0)
            rsi = signal.get("rsi", 0)
            sig = signal.get("signal", "NEUTRAL")
            htf = signal.get("htf_bias", "?")

            status_prefix = "🛡️ [MONITORING] " if monitoring_only else ""
            log.info(
                f"{status_prefix}Price: ${live_price:,.2f} | Sig: {sig} | "
                f"Bull: {bull}/{ms} Bear: {bear}/{ms} Conf: {conf}% | "
                f"RSI: {rsi:.1f} | HTF: {htf} | "
                f"Bal: ${self.state.balance:,.2f} | Trades: {self.state.total_trades} | "
                f"WR: {self.state.win_rate():.1f}% | Sesi: {session}"
            )
        except Exception as e:
            log.warning(f"print_status error (non-fatal): {e}")

    # ── Dashboard State (untuk Flask) ─────────────────────────

    def get_dashboard_state(self) -> dict:
        ws_price = self.price_feed.get_price()
        sig      = self._last_signal or {}
        
        # ── PERBAIKAN KRITIS: Ambil Mark Prices untuk semua koin yang ada posisinya ──
        # Tujuannya agar PnL di bot = PnL di MEXC (yang pakai Mark Price)
        # Dan koin yang berbeda dihitung pakai harga masing-masing.
        active_positions_list = [p for p in self.state.positions if not p.closed]
        unique_symbols = list(set([p.symbol for p in active_positions_list]))
        
        # Ambil harga ticker terbaru untuk semua koin aktif
        prices_map = {}
        for sym in unique_symbols:
            try:
                # Prioritaskan WebSocket jika simbolnya sama dengan koin utama
                if sym == self.cfg["SYMBOL"]:
                    mark_p = getattr(self.price_feed, 'mark_price', 0)
                    price_l = ws_price if ws_price > 0 else 0
                    
                    # Jika data WS masih 0, paksa ambil dari Ticker API
                    if mark_p <= 0 and price_l <= 0:
                        ticker = self.client.get_ticker(sym)
                        if ticker:
                            prices_map[sym] = ticker["mark"] if ticker["mark"] > 0 else ticker["last"]
                    else:
                        prices_map[sym] = mark_p if mark_p > 0 else price_l
                else:
                    # Ambil dari API untuk koin lain atau jika WS belum siap
                    ticker = self.client.get_ticker(sym)
                    if ticker:
                        prices_map[sym] = ticker["mark"] if ticker["mark"] > 0 else ticker["last"]
            except: pass

        # Hitung floating PnL real-time untuk semua posisi terbuka
        total_floating_pnl = 0.0
        active_positions = []
        for p in active_positions_list:
            # Gunakan harga spesifik koin ini, fallback ke entry price jika gagal ambil harga
            curr = prices_map.get(p.symbol, p.entry_price)
            
            if p.side == "LONG":
                fpnl = (curr - p.entry_price) * p.quantity * p.contract_size
            else:
                fpnl = (p.entry_price - curr) * p.quantity * p.contract_size
            
            total_floating_pnl += fpnl
            active_positions.append({
                "id": p.id, "symbol": p.symbol, "side": p.side, "entry_price": p.entry_price,
                "current_price": round(curr, 6),
                "quantity": p.quantity, "stop_loss": p.stop_loss,
                "take_profit1": p.take_profit1, "take_profit2": p.take_profit2,
                "take_profit3": p.take_profit3, "pnl": round(p.pnl, 4),
                "live_pnl": round(fpnl, 4),
                "trailing_active": p.trailing_active,
                "risk_reduced": p.risk_reduced
            })

        # Equity di state sudah termasuk Unrealized (Equity = Wallet + Unrealized)
        display_balance = self.state.balance
        display_total_pnl = self.state.total_pnl + total_floating_pnl

        return {
            "balance":        round(display_balance, 2),
            "equity":         round(self.state.equity, 2),
            "available":      round(self.state.available_balance, 2),
            "is_dry_run":     self.dry_run,
            "peak_balance":   round(max(self.state.peak_balance, display_balance), 2),
            "leverage":       self.cfg["LEVERAGE"],
            "total_pnl":      round(display_total_pnl, 4),
            "daily_pnl":      round(self.state.daily_pnl + total_floating_pnl, 4),
            "win_rate":       round(self.state.win_rate(), 1),
            "total_trades":   self.state.total_trades,
            "drawdown":       round((self.state.peak_balance - display_balance)/self.state.peak_balance*100 if self.state.peak_balance > 0 else 0, 2),
            "iteration":      self.state.iteration,
            "started_at":     self.state.started_at,
            "circuit_breaker": self.state.circuit_breaker,
            "circuit_reason":  self.state.circuit_reason,
            "circuit_type":    self.state.circuit_type,
            "circuit_triggered_at": self.state.circuit_triggered_at,
            "server_time":    time.time(),
            "signal":         sig.get("signal", "NEUTRAL"),
            "bull_score":     sig.get("bull_score", 0),
            "bear_score":     sig.get("bear_score", 0),
            "confidence":     sig.get("confidence", 0),
            "htf_bias":       sig.get("htf_bias", "—"),
            "rsi":            round(sig.get("rsi", 0), 2),
            "stoch_k":        round(sig.get("stoch_k", 0), 2),
            "stoch_d":        round(sig.get("stoch_d", 0), 2),
            "atr":            round(sig.get("atr", 0), 6),
            "vol_ratio":      round(sig.get("vol_ratio", 0), 2),
            "adx":            round(sig.get("adx", 0), 1),
            "live_price":     round(ws_price if ws_price > 0 else sig.get("close", 0), 6),
            "hold_reason":    sig.get("hold_reason", ""),
            "session":        self.session.is_trading_allowed()[1],
            "secured_total":  round(self.state.secured_total, 2),
            "spot_balance":   round(self.state.spot_balance, 2),
            "futures_equity": round(self.state.futures_total_equity, 2),
            "positions":      active_positions,
            "is_dry_run":     self.dry_run,
            "symbol":         self.cfg["SYMBOL"],
            "primary_tf":     self.cfg["PRIMARY_TF"],
            "confirm_tf":     self.cfg["CONFIRM_TF"],
            "scanned_signals": self._scanned_signals
        }


    # ── Main Loop ─────────────────────────────────────────────

    def run(self):
        log.info("=" * 64)
        log.info("  XAUT/USDT Pro Bot v2.0 dimulai")
        log.info("=" * 64)

        # Graceful shutdown
        def _shutdown(sig, frame):
            log.info("Sinyal shutdown diterima...")
            self.price_feed.stop()
            self.persistence.save(self.state, self.dry_run)
            self.notifier.send("⛔ Bot XAUT/USDT dihentikan. State disimpan.")
            sys.exit(0)
        os_signal.signal(os_signal.SIGINT, _shutdown)
        os_signal.signal(os_signal.SIGTERM, _shutdown)

        # Start dashboard kalau diminta
        if self.run_dashboard:
            self._start_dashboard()
            # Jalankan scanner di thread terpisah agar tidak mengganggu bot utama
            threading.Thread(target=self._run_signal_scanner, daemon=True, name="SignalScanner").start()

        mode_str = "DRY RUN" if self.dry_run else "LIVE TRADING"
        self.notifier.send(
            f"🤖 *XAUT/USDT Pro Bot v2.0 Dimulai*\n"
            f"Mode: `{mode_str}`\n"
            f"Symbol: `{self.cfg['SYMBOL']}`\n"
            f"TF: `{self.cfg['PRIMARY_TF']}` / `{self.cfg['CONFIRM_TF']}`\n"
            f"Balance: `${self.state.balance:,.2f} USDT`\n"
            f"WebSocket: {'Aktif' if self.price_feed.price > 0 else 'Connecting...'}\n"
            f"Trailing Stop: {'Aktif' if self.cfg['USE_TRAILING_STOP'] else 'Off'}\n"
            f"MTF Confirm: {'Ya' if self.cfg['REQUIRE_MTF_CONFIRM'] else 'Tidak'}"
        )

        while True:
            try:
                self.state.iteration += 1
                self._check_daily_reset()

                # 1. Analisis Sinyal (Selalu berjalan agar dashboard update)
                signal = self.fetch_and_analyze()
                if signal is None:
                    log.info("⏳ [Heartbeat] Data tidak lengkap — menunggu 30s...")
                    time.sleep(30)
                    continue

                # 2. Cek apakah trading (ENTRY) diizinkan — CEK DULU sebelum exit
                # Agar CB tidak mengganggu posisi aktif yang sedang berjalan
                circuit_blocked = self._check_circuit_breaker()

                # 3. EKSEKUSI EXIT (SL/TP/FLIP) — HARUS DI LUAR BLOKIR TRADING
                # Agar SL/TP tetap terpantau meskipun bot sedang pause/circuit breaker
                self.check_exits_candle(signal)
                ok_session, session_info = self.session.is_trading_allowed()
                
                trading_blocked = circuit_blocked or not ok_session
                
                if trading_blocked:
                    log.info(f"🛡️ [MONITORING] Trading dipause: {self.state.circuit_reason if circuit_blocked else session_info}")
                    self.print_status(signal, monitoring_only=True)
                    time.sleep(15 if circuit_blocked else 60)
                    continue

                # 4. Eksekusi Trading (ENTRY BARU)
                log.info(f"⚖️ [Heartbeat] Memeriksa syarat entri ({signal['signal']})...")
                
                # Check Auto-Reverse Policy
                can_entry = True
                if signal.get("flip_occurred") and not self.cfg.get("AUTO_REVERSE_ON_FLIP", True):
                    log.info("🚫 [Flip] Auto-reverse dimatikan — Menunggu iterasi berikutnya untuk entri.")
                    can_entry = False
                    signal["blocked_reason"] = "Auto-reverse disabled on flip"
                
                if can_entry:
                    if signal["signal"] == "LONG":
                        self.open_position("LONG", signal)
                    elif signal["signal"] == "SHORT":
                        self.open_position("SHORT", signal)
                elif "blocked_reason" in signal:
                    log.info(f"🚫 [Heartbeat] Sinyal diblokir: {signal['blocked_reason']}")

                # Tampilkan status
                self.print_status(signal)

                # Simpan state setiap iterasi
                self.persistence.save(self.state, self.dry_run)

                log.info(f"Iterasi #{self.state.iteration} selesai. Tunggu {self.cfg['POLL_INTERVAL']}s...")
                time.sleep(self.cfg["POLL_INTERVAL"])

            except KeyboardInterrupt:
                log.info("KeyboardInterrupt — shutdown...")
                self.price_feed.stop()
                self.persistence.save(self.state, self.dry_run)
                self.notifier.send("⛔ Bot dihentikan manual. State disimpan.")
                break
            except Exception as e:
                log.exception(f"Error loop utama: {e}")
                time.sleep(30)

    def _get_cached_contract_info(self, symbol: str) -> dict:
        """Helper untuk ambil info kontrak dengan cache."""
        if symbol in self._contract_info:
            return self._contract_info[symbol]
        
        info = self.client.get_contract_detail(symbol)
        if info:
            self._contract_info[symbol] = info
            return info
        return {"volScale": 0, "priceScale": 4, "minVol": 1}

    # ── Dashboard Web (Flask) ─────────────────────────────────

    def _start_dashboard(self):
        """Jalankan Flask dashboard di thread terpisah."""
        try:
            from flask import Flask, jsonify, render_template_string, request
            app = Flask(__name__)

            @app.route("/")
            def index():
                return render_template_string(DASHBOARD_HTML)

            @app.route("/api/state")
            def api_state():
                try:
                    raw_state = self.get_dashboard_state()
                    
                    # Sanitize state for JSON compliance (handle NaN, Infinity, Decimal)
                    import math
                    from decimal import Decimal
                    
                    def sanitize_val(v):
                        if isinstance(v, float):
                            if math.isnan(v) or math.isinf(v): return 0.0
                            return v
                        if isinstance(v, Decimal):
                            return float(v)
                        if isinstance(v, dict):
                            return {k: sanitize_val(val) for k, val in v.items()}
                        if isinstance(v, list):
                            return [sanitize_val(val) for val in v]
                        return v

                    sanitized_state = sanitize_val(raw_state)
                    return jsonify(sanitized_state)
                except Exception as e:
                    log.exception(f"Error api_state: {e}")
                    return jsonify({"error": str(e), "status": "error"}), 500

            @app.route("/api/reset_circuit", methods=["POST"])
            def reset_circuit():
                # Server-side validation: must wait 10 mins (600s) hanya jika AUTO
                if self.state.circuit_type == "AUTO":
                    elapsed = time.time() - self.state.circuit_triggered_at
                    if elapsed < 600:
                        return jsonify({"success": False, "error": f"Harap tunggu {int(600-elapsed)} detik (Auto Circuit)"}), 403
                
                self.state.circuit_breaker = False
                self.state.circuit_reason = ""
                self.state.circuit_type = ""
                # Penting: Reset peak_balance ke saat ini supaya drawdown jadi 0% lagi
                self.state.peak_balance = self.state.balance
                self.state.daily_pnl = 0.0
                
                self.persistence.save(self.state, self.dry_run)
                log.info("Circuit breaker direset manual via dashboard — New start point set")
                return jsonify({"success": True})

            @app.route("/api/manual_stop", methods=["POST"])
            def manual_stop():
                # Tutup semua posisi dulu agar sinkron dengan MEXC
                self.close_all_positions(reason="Berhenti Manual")
                
                self.state.circuit_breaker = True
                self.state.circuit_type = "MANUAL"
                self.state.circuit_reason = "Berhenti manual oleh pengguna"
                self.state.circuit_triggered_at = time.time()
                self.persistence.save(self.state, self.dry_run)
                log.info("BOT DIHENTIKAN MANUAL VIA DASHBOARD — SEMUA POSISI DITUTUP")
                return jsonify({"success": True})

            @app.route("/api/sync_positions", methods=["POST"])
            def sync_positions():
                if self.dry_run:
                    return jsonify({"success": False, "error": "Tidak tersedia dalam mode Dry Run"})
                self.sync_positions_from_mexc()
                return jsonify({"success": True})

            @app.route("/api/history")
            def api_history():
                try:
                    date_filter = request.args.get("date") # format YYYY-MM-DD
                    history = self.journal.get_trades(date_filter)
                    
                    total_pnl = 0.0
                    total_win = 0.0
                    total_loss = 0.0
                    
                    for t in history:
                        pnl = float(t.get("pnl", 0))
                        total_pnl += pnl
                        if pnl > 0:
                            total_win += pnl
                        else:
                            total_loss += pnl
                    
                    return jsonify({
                        "history": history,
                        "date": date_filter,
                        "total_pnl": round(total_pnl, 4),
                        "total_win": round(total_win, 4),
                        "total_loss": round(total_loss, 4)
                    })
                except Exception as e:
                    log.exception(f"Error api_history: {e}")
                    return jsonify({"history": [], "date": None, "total_pnl": 0, "total_win": 0, "total_loss": 0}), 200

            @app.route("/api/reset_all", methods=["POST"])
            def api_reset_all():
                """Reset total bot ke kondisi awal dengan saldo $100."""
                log.info("RESET TOTAL DIJALANKAN — Saldo kembali ke $100")
                self.state.balance = 100.0
                self.state.peak_balance = 100.0
                self.state.total_pnl = 0.0
                self.state.daily_pnl = 0.0
                self.state.total_trades = 0
                self.state.winning_trades = 0
                self.state.iteration = 0
                self.state.circuit_breaker = False
                self.state.secured_total = 0.0 # Reset tabungan spot juga di dashboard
                
                # Kosongkan file riwayat jika diminta (user bilang mulai dari awal)
                if os.path.exists(self.journal.filepath):
                    os.remove(self.journal.filepath)
                    self.journal._ensure_header()
                
                self.persistence.save(self.state, self.dry_run)
                return jsonify({"success": True})

            @app.route("/api/config", methods=["GET", "POST"])
            def api_config():
                if request.method == "GET":
                    # Filter kunci sensitif agar tidak bocor ke dashboard
                    hide = ["MEXC_API_KEY", "MEXC_API_SECRET", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID", "TELEGRAM_CHAT"]
                    safe_cfg = {k: v for k, v in self.cfg.items() if k not in hide}
                    return jsonify(safe_cfg)
                
                # POST: Update konfigurasi
                try:
                    new_data = request.json
                    if not new_data:
                        return jsonify({"success": False, "error": "Data kosong"}), 400
                        
                    # Validasi ganti koin
                    if "SYMBOL" in new_data and new_data["SYMBOL"] != self.cfg.get("SYMBOL"):
                        active_pos = [p for p in self.state.positions if not p.closed]
                        if len(active_pos) > 0:
                            return jsonify({
                                "success": False, 
                                "error": f"Tidak bisa ganti koin karena masih ada {len(active_pos)} posisi terbuka pada {self.cfg.get('SYMBOL')}. Harap blokir dan hentikan manual bot di Home terlebih dahulu."
                            }), 400
                    
                    self.update_config_live(new_data)
                    return jsonify({"success": True})
                except Exception as e:
                    log.error(f"Error update config: {e}")
                    return jsonify({"success": False, "error": str(e)}), 500
                    
            # Cache memori sederhana untuk /api/top_coins
            self._top_coins_cache = []
            self._top_coins_last_fetch = 0
            
            @app.route("/api/top_coins")
            def api_top_coins():
                try:
                    now = time.time()
                    # Kurangi cache dari 300 detik ke 30 detik agar harga di pencarian lebih akurat
                    if now - getattr(self, "_top_coins_last_fetch", 0) > 30 or not getattr(self, "_top_coins_cache", []):
                        log.info("Fetching top volume coins dari MEXC...")
                        coins = self.client.get_top_volume_coins(limit=100)
                        if coins:
                            self._top_coins_cache = coins
                            self._top_coins_last_fetch = now
                    
                    return jsonify({"success": True, "data": self._top_coins_cache, "signals": self._scanned_signals})
                except Exception as e:
                    log.error(f"Error fetch top coins: {e}")
                    return jsonify({"success": False, "data": [], "signals": {}})

            @app.route("/api/analysis")
            def api_analysis():
                """Endpoint untuk data visualisasi dan statistik performa trading."""
                try:
                    # Ambil semua data trades (kronologis)
                    trades = self.journal.get_trades()
                    trades = trades[::-1] # Urutan paling awal ke terbaru
                    
                    if not trades:
                        return jsonify({"success": False, "error": "Belum ada riwayat trading untuk dianalisis."})

                    # 1. Equity Curve & Statistik Dasar
                    equity_data = [] # {date, balance}
                    current_equity = self.cfg.get("VIRTUAL_BALANCE", 100.0)
                    
                    wins, losses = 0, 0
                    gross_profit, gross_loss = 0.0, 0.0
                    symbols_stats = {} # {symbol: pnl}
                    
                    # Titik awal equity
                    equity_data.append({
                        "id": "Start",
                        "x": trades[0]["opened_at"],
                        "y": round(current_equity, 2)
                    })
                    
                    for t in trades:
                        pnl = float(t.get("pnl", 0))
                        sym = t.get("symbol", "UNKNOWN")
                        
                        current_equity += pnl
                        equity_data.append({
                            "id": t["trade_id"],
                            "x": t["closed_at"],
                            "y": round(current_equity, 2)
                        })
                        
                        if pnl > 0:
                            wins += 1
                            gross_profit += pnl
                        else:
                            losses += 1
                            gross_loss += abs(pnl)
                            
                        symbols_stats[sym] = round(symbols_stats.get(sym, 0) + pnl, 2)
                    
                    # 2. Kalkulasi Metrik Advanced
                    total_trades = wins + losses
                    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
                    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (gross_profit if gross_profit > 0 else 0)
                    avg_win = (gross_profit / wins) if wins > 0 else 0
                    avg_loss = (gross_loss / losses) if losses > 0 else 0
                    
                    # Siapkan data bar chart koin
                    symbol_labels = list(symbols_stats.keys())
                    symbol_values = list(symbols_stats.values())
                    
                    return jsonify({
                        "success": True,
                        "metrics": {
                            "win_rate": round(win_rate, 1),
                            "profit_factor": round(profit_factor, 2),
                            "avg_win": round(avg_win, 2),
                            "avg_loss": round(avg_loss, 2),
                            "total_trades": total_trades,
                            "gross_profit": round(gross_profit, 2),
                            "gross_loss": round(gross_loss, 2)
                        },
                        "charts": {
                            "equity": equity_data,
                            "win_loss": [wins, losses],
                            "symbols": {"labels": symbol_labels, "values": symbol_values}
                        }
                    })
                except Exception as e:
                    log.error(f"Error analysis API: {e}")
                    return jsonify({"success": False, "error": str(e)})

            @app.route("/api/health")
            def health():
                return jsonify({"status": "ok", "ts": datetime.now(WIB).isoformat()})

            t = threading.Thread(
                target=lambda: app.run(
                    host=self.cfg["DASHBOARD_HOST"],
                    port=self.cfg["DASHBOARD_PORT"],
                    debug=False, use_reloader=False,
                ),
                daemon=True,
                name="Dashboard",
            )
            t.start()
            log.info(f"Dashboard berjalan di http://localhost:{self.cfg['DASHBOARD_PORT']}")
            self.notifier.send(f"📊 Dashboard: http://localhost:{self.cfg['DASHBOARD_PORT']}")
        except ImportError:
            log.warning("Flask tidak terinstall — dashboard tidak aktif. pip install flask")


# ══════════════════════════════════════════════════════════════
#  DASHBOARD HTML (inline template)
# ══════════════════════════════════════════════════════════════

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="theme-color" content="#0a0e1a">
<title>MEXC Pro Bot</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800&display=swap" rel="stylesheet">
<script src="https://unpkg.com/@phosphor-icons/web"></script>
<script src="https://cdn.jsdelivr.net/npm/apexcharts"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
:root{--bg:#0a0e1a;--card:#111827;--border:#1e3a5f;--gold:#f0c040;--green:#4ade80;--red:#f87171;--blue:#60a5fa;--gray:#6b7280;--text:#e0e6ff;--sub:#9ca3af}
body{font-family:'Inter',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding-bottom:70px}

/* ── Header ── */
.hdr{background:var(--card);border-bottom:1.5px solid #1e3a5f;padding:12px 16px;position:sticky;top:0;z-index:30;backdrop-filter:blur(12px)}
.hdr-top{display:flex;align-items:center;justify-content:space-between}
.logo{font-size:18px;font-weight:800;color:var(--gold)}
.hdr-price{font-size:20px;font-weight:700;color:var(--gold);text-align:right}
.hdr-price small{font-size:11px;color:var(--sub);display:block;font-weight:400}
.btn-stop{background:#ef4444;color:white;border:none;width:32px;height:32px;border-radius:50%;font-weight:800;cursor:pointer;display:flex;align-items:center;justify-content:center;box-shadow:0 0 10px rgba(239,68,68,0.4)}

.badge-row{display:flex;gap:6px;margin-top:8px;align-items:center}
.badge{font-size:10px;padding:3px 8px;border-radius:4px;font-weight:700;background:#1a2a3a;color:var(--sub)}
.badge-long{color:var(--green);background:rgba(74,222,128,0.1)}
.badge-short{color:var(--red);background:rgba(248,113,113,0.1)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--green);animation:pulse 1.5s infinite;display:inline-block;margin-left:4px}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}

/* ── Navigation ── */
.nav{position:fixed;bottom:0;left:0;right:0;background:var(--card);border-top:1px solid var(--border);display:flex;height:60px;z-index:100}
.nav-item{flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:4px;color:var(--gray);font-size:11px;font-weight:600;cursor:pointer;transition:all 0.3s}
.nav-item.active{color:var(--gold)}
.nav-item i{font-size:24px;margin-bottom:2px}

/* ── Tabs ── */
.tab-content{display:none;animation:fadeIn 0.3s ease}
.tab-content.active{display:block}
@keyframes fadeIn{from{opacity:0;transform:translateY(5px)}to{opacity:1;transform:translateY(0)}}

/* ── Home Content ── */
.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;padding:12px}
.card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:12px}
.card-label{font-size:10px;color:var(--gray);text-transform:uppercase;margin-bottom:4px}
.card-val{font-size:20px;font-weight:800;color:var(--gold)}
.card-sub{font-size:11px;color:var(--sub);margin-top:2px}
.card-wide{grid-column:1/-1}
.pos{color:var(--green)}.neg{color:var(--red)}

.sec{padding:0 12px 12px}
.sec-title{font-size:10px;color:var(--gray);text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;padding-bottom:4px;border-bottom:1px solid var(--border)}

.sig-box{padding:14px;border-radius:10px;margin-bottom:12px;border:1px solid;text-align:center}
.sig-long{border-color:var(--green);background:#0d2b0d}
.sig-short{border-color:var(--red);background:#2b0d0d}
.sig-neutral{border-color:#374151;background:var(--card)}
.sig-dir{font-size:24px;font-weight:800}
.sig-dir.long{color:var(--green)}
.sig-dir.short{color:var(--red)}

.ind-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;margin-bottom:12px}
.ind-item{background:var(--card);border:1px solid var(--border);border-radius:6px;padding:8px}
.ind-val{font-size:14px;font-weight:700;margin-top:2px}

.pos-card{background:#0d1a2e;border:1px solid var(--border);border-radius:8px;padding:12px;margin-bottom:8px}
.pos-hdr{display:flex;align-items:center;gap:6px;margin-bottom:8px}
.pos-grid{display:grid;grid-template-columns:1fr 1fr;gap:4px 12px;font-size:12px}
.pos-grid dt{color:var(--gray)}.pos-grid dd{font-weight:600;text-align:right}

/* ── History Styles ── */
.hist-filter{padding:12px;background:var(--card);border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between}
.date-input{background:var(--bg);color:var(--text);border:1px solid var(--border);padding:6px 10px;border-radius:6px;font-family:inherit}
.hist-stats{padding:12px;display:grid;grid-template-columns:1fr;gap:10px}
.hist-table{width:100%;border-collapse:collapse;margin-top:10px;font-size:12px}
.hist-table th{text-align:left;color:var(--gray);padding:8px;border-bottom:1px solid var(--border)}
.hist-table td{padding:10px 8px;border-bottom:1px solid #111e35}
.hist-side{font-weight:800;font-size:10px;padding:2px 4px;border-radius:3px}

/* ── Analysis Tab Styles ── */
.ana-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;padding:12px}
.ana-card{background:rgba(17,24,39,0.7);border:1px solid rgba(234,179,8,0.1);border-radius:12px;padding:12px;backdrop-filter:blur(8px)}
.ana-label{font-size:9px;color:var(--gray);text-transform:uppercase;font-weight:700;letter-spacing:0.5px;margin-bottom:4px}
.ana-val{font-size:16px;font-weight:800;color:var(--text)}
.ana-chart-card{background:var(--card);border:1px solid var(--border);border-radius:16px;margin:0 12px 12px;padding:16px;overflow:hidden}
.ana-chart-title{font-size:12px;font-weight:800;color:var(--gold);margin-bottom:15px;display:flex;align-items:center;gap:8px}
.ana-chart-title i{font-size:16px;color:var(--blue)}
.chart-container{min-height:250px;width:100%}

/* ── Circuit Breaker ── */
.cb{background:#3a1a0d;border:1px solid #c2410c;border-radius:8px;padding:12px;margin:12px;color:#fb923c;font-weight:600;font-size:13px;display:none;text-align:center}
.cb-box{margin-top:10px;display:flex;justify-content:center}
.btn-reset{background:var(--gold);color:var(--bg);border:none;padding:10px 20px;border-radius:6px;font-weight:800;cursor:pointer;display:flex;align-items:center;gap:8px}
.spinner{width:16px;height:16px;border:2px solid rgba(0,0,0,0.1);border-top-color:currentColor;border-radius:50%;animation:spin 0.8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

.btn-reset-all{display:block;width:calc(100% - 24px);margin:20px 12px;padding:12px;background:#1e3a5f;color:var(--red);border:1px solid var(--red);border-radius:8px;font-weight:700;text-align:center;cursor:pointer}

.form-sec{background:rgba(26,42,58,0.5);border:1px solid rgba(234,179,8,0.15);border-radius:16px;padding:20px;margin-bottom:20px;backdrop-filter:blur(10px);box-shadow:0 8px 32px rgba(0,0,0,0.2)}
.form-title{font-size:13px;font-weight:900;color:var(--gold);margin-bottom:16px;display:flex;align-items:center;gap:10px;letter-spacing:1px;text-shadow:0 0 10px rgba(234,179,8,0.3)}
.form-title i{font-size:18px}
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
.form-group{display:flex;flex-direction:column;gap:6px;min-width:0}
.form-label{font-size:9px;color:var(--gray);text-transform:uppercase;font-weight:700;letter-spacing:0.5px;margin-left:2px}
.form-input{width:100%;box-sizing:border-box;background:rgba(15,23,42,0.8);color:var(--text);border:1px solid var(--border);padding:12px;border-radius:10px;font-size:14px;font-family:inherit;transition:all 0.3s;box-shadow:inset 0 2px 4px rgba(0,0,0,0.1)}
.form-input:focus{outline:none;border-color:var(--gold);background:rgba(15,23,42,1);box-shadow:0 0 0 3px rgba(234,179,8,0.15)}
.btn-save-cfg{display:block;width:100%;padding:16px;background:var(--gold);color:var(--bg);border:none;border-radius:12px;font-weight:800;font-size:15px;cursor:pointer;margin-top:25px;transition:all 0.2s ease;text-transform:uppercase;letter-spacing:1px;box-shadow:0 4px 15px rgba(0,0,0,0.2)}
.btn-save-cfg:hover{transform:translateY(-2px);filter:brightness(1.1);box-shadow:0 6px 20px rgba(240,192,64,0.2)}
.btn-save-cfg:active{transform:translateY(0)}
.btn-save-cfg:disabled{opacity:0.5;cursor:not-allowed;transform:none}

/* ── Custom Coin Selector ── */
.coin-selector-btn{width:100%;background:rgba(15,23,42,0.8);color:var(--text);border:1px solid var(--border);padding:14px;border-radius:12px;font-size:14px;display:flex;align-items:center;justify-content:space-between;cursor:pointer;transition:all 0.3s;box-shadow:inset 0 2px 4px rgba(0,0,0,0.1)}
.coin-selector-btn:hover{border-color:var(--gold);background:rgba(15,23,42,1)}
.coin-selector-btn i{color:var(--gold);font-size:18px}
.coin-info-mini{display:flex;align-items:center;gap:10px}
.coin-logo-mini{width:24px;height:24px;background:var(--gold);color:var(--bg);border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:10px}

.search-modal{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(10,14,26,0.95);backdrop-filter:blur(10px);z-index:2000;display:none;flex-direction:column;padding:20px 16px;animation:modalIn 0.3s ease}
.search-modal.active{display:flex}
.search-hdr{display:flex;align-items:center;gap:12px;margin-bottom:20px}
.search-input-wrapper{flex:1;position:relative}
.search-input-wrapper i{position:absolute;left:12px;top:50%;transform:translateY(-50%);color:var(--gray)}
.search-input{width:100%;background:rgba(15,23,42,1);border:1px solid var(--border);border-radius:12px;padding:12px 12px 12px 40px;color:white;font-family:inherit;font-size:16px}
.search-input:focus{outline:none;border-color:var(--gold)}
.close-search{background:none;border:none;color:white;font-size:24px;cursor:pointer;padding:4px}
.coin-list{flex:1;overflow-y:auto;border-top:1px solid var(--border);padding-top:10px}
.coin-item{display:flex;align-items:center;justify-content:space-between;padding:14px;border-radius:12px;margin-bottom:8px;background:rgba(30,41,59,0.3);cursor:pointer;transition:all 0.2s}
.coin-item:hover{background:rgba(234,179,8,0.1);transform:translateX(5px)}
.coin-main{display:flex;align-items:center;gap:12px}
.coin-sym-box{display:flex;flex-direction:column}
.coin-sym-name{font-weight:800;font-size:16px;color:white}
.coin-sym-sub{font-size:10px;color:var(--gray);text-transform:uppercase}
.coin-stats{text-align:right}
.coin-price{font-weight:700;font-size:14px;color:var(--text)}
.coin-change{font-size:11px;font-weight:600}

.help-icon{font-size:14px;color:var(--gray);cursor:pointer;margin-left:4px;transition:all 0.2s}
.help-icon:hover{color:var(--gold)}

.modal-overlay{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.85);backdrop-filter:blur(5px);display:none;align-items:center;justify-content:center;z-index:1000;padding:20px}
.modal-content{background:var(--card);border:1px solid var(--border);border-radius:20px;padding:24px;max-width:340px;width:100%;box-shadow:0 20px 50px rgba(0,0,0,0.5);animation:modalIn 0.3s ease}
@keyframes modalIn{from{opacity:0;transform:scale(0.95)}to{opacity:1;transform:scale(1)}}
.modal-title{font-weight:800;color:var(--gold);margin-bottom:12px;display:flex;align-items:center;gap:8px}
.modal-text{font-size:13px;line-height:1.6;color:var(--text)}
.modal-btn{background:var(--border);color:var(--text);border:none;padding:12px;border-radius:12px;width:100%;margin-top:20px;font-weight:700;cursor:pointer}

@media(max-width:400px){ .form-grid{grid-template-columns:1fr} }
</style>
</head>
<body>

<header class="hdr">
  <div class="hdr-top">
    <div class="logo" id="symbolLogo">... <span style="font-size:10px; opacity:0.5">v2.1</span></div>
    <div style="display:flex;align-items:center;gap:12px">
        <div class="hdr-price" id="currPrice">$--<small>Futures</small></div>
        <button class="btn-stop" onclick="manualStop()">✖</button>
    </div>
  </div>
  <div class="badge-row">
    <span class="badge" id="modeBadge">DRY RUN</span>
    <span class="badge" id="tfBadge">TF: — / —</span>
    <span class="badge" id="levBadge">LEV 50X</span>
    <span class="badge" id="sessBadge">—</span>
    <span class="dot" id="wsDot"></span>
  </div>
</header>

<!-- Tab: HOME -->
<div id="tabHome" class="tab-content active">
    <div class="cb" id="cbBanner">
      ⛔ CIRCUIT BREAKER — <span id="cbReason"></span>
      <div style="margin-top:10px"><button id="btnReset" onclick="resetCircuit()" style="background:var(--gold);border:none;padding:8px 16px;border-radius:6px;font-weight:800">🚀 Mulai Lagi</button></div>
    </div>
    <div class="grid">
      <div class="card" style="grid-column: 1 / -1;">
        <div class="card-label">Total Account Equity (Net Worth)</div>
        <div class="card-val" id="totalNetWorth">$0.00</div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px">
          <div style="font-size:11px;color:var(--sub)">Futures: <span id="balance" style="color:var(--gold);font-weight:700">$0.00</span></div>
          <div style="font-size:11px;color:var(--sub)">Spot: <span id="spotBalance" style="color:var(--gold);font-weight:700">$0.00</span></div>
        </div>
        <div class="card-sub" id="pnlSub">Avail Futures: $0.00</div>
      </div>
      <div class="card"><div class="card-label">Total PnL</div><div class="card-val" id="totalPnl">$0.00</div><div class="card-sub" id="dailyPnl">Daily: $0</div></div>
      <div class="card"><div class="card-label">Drawdown</div><div class="card-val neg" id="drawdown">0%</div><div class="card-sub">Max Decline</div></div>
      <div class="card"><div class="card-label">Aman di Spot</div><div class="card-val" id="securedTot">$0.00</div><div class="card-sub">Wealth Protection</div></div>
      <div class="card"><div class="card-label">Win Rate</div><div class="card-val" id="winRate">0%</div><div class="card-sub" id="tradeCount">0 trades</div></div>
      <div class="card"><div class="card-label">Iterasi</div><div class="card-val" id="iteration">#0</div><div class="card-sub" id="startedAt">—</div></div>
    </div>
    <div class="sec">
      <div class="sec-title">Analisis Sinyal</div>
      <div class="sig-box sig-neutral" id="signalBox">
        <div class="sig-dir" id="sigVal">NEUTRAL</div>
        <div id="sigDetails" style="font-size:12px;margin-top:6px;color:var(--sub)">Bull: 0 | Bear: 0 | Conf: 0%</div>
      </div>
      <div class="ind-grid">
        <div class="ind-item"><div class="card-label">RSI</div><div class="ind-val" id="iRsi">0</div></div>
        <div class="ind-item"><div class="card-label">ATR</div><div class="ind-val" id="iAtr">$0</div></div>
        <div class="ind-item"><div class="card-label">Stoch</div><div class="ind-val" id="iStoch">0</div></div>
        <div class="ind-item"><div class="card-label">ADX</div><div class="ind-val" id="iAdx" style="color:var(--gray)">0</div></div>
      </div>
    </div>
    <div class="sec">
      <div class="sec-title">Posisi Terbuka</div>
      <div id="positions"></div>
    </div>
</div>

<!-- Tab: HISTORY -->
<div id="tabHistory" class="tab-content">
    <div class="hist-filter">
        <span style="font-size:13px;font-weight:700">Filter Tanggal:</span>
        <input type="date" id="histDate" class="date-input" onchange="loadHistory()">
    </div>
    <div class="hist-stats grid" style="grid-template-columns:repeat(3,1fr)">
        <div class="card" style="padding:10px">
            <div class="card-label" style="font-size:8px">TOTAL WIN</div>
            <div class="card-val pos" id="histTotalWin" style="font-size:14px">$0.00</div>
        </div>
        <div class="card" style="padding:10px">
            <div class="card-label" style="font-size:8px">TOTAL LOSS</div>
            <div class="card-val neg" id="histTotalLoss" style="font-size:14px">$0.00</div>
        </div>
        <div class="card" style="padding:10px">
            <div class="card-label" style="font-size:8px">NET DAILY</div>
            <div class="card-val" id="histDailyPnl" style="font-size:14px">$0.00</div>
        </div>
    </div>
    <div class="sec">
        <table class="hist-table">
            <thead><tr><th>Side</th><th>Koin</th><th>Entry/Exit</th><th style="text-align:right">PnL</th></tr></thead>
            <tbody id="histBody"></tbody>
        </table>
    </div>
    <div id="btnResetAll" class="btn-reset-all" onclick="resetAll()">🔥 RESET BOT (MULAI $100)</div>
</div>

        <div id="tabAnalysis" class="tab-content">
            <div class="ana-grid">
                <div class="ana-card">
                    <div class="ana-label">Win Rate</div>
                    <div class="ana-val" id="anaWinRate">0%</div>
                </div>
                <div class="ana-card">
                    <div class="ana-label">Profit Factor</div>
                    <div class="ana-val" id="anaPF">0.00</div>
                </div>
                <div class="ana-card">
                    <div class="ana-label">Avg Win</div>
                    <div class="ana-val pos" id="anaAvgWin">+$0.00</div>
                </div>
                <div class="ana-card">
                    <div class="ana-label">Avg Loss</div>
                    <div class="ana-val neg" id="anaAvgLoss">-$0.00</div>
                </div>
            </div>

            <div class="ana-chart-card">
                <div class="ana-chart-title"><i class="ph ph-trend-up"></i> Pertumbuhan Saldo (Equity Curve)</div>
                <div id="equityChart" class="chart-container"></div>
            </div>

            <div class="ana-chart-card">
                <div class="ana-chart-title"><i class="ph ph-chart-pie"></i> Distribusi Win/Loss</div>
                <div id="winLossChart" class="chart-container"></div>
            </div>

            <div class="ana-chart-card">
                <div class="ana-chart-title"><i class="ph ph-coins"></i> Profit per Koin</div>
                <div id="symbolChart" class="chart-container"></div>
            </div>
            
            <div style="padding:20px; text-align:center; opacity:0.3; font-size:10px">PRO ANALYTICS ENGINE V2.0</div>
        </div>

<div id="tabSettings" class="tab-content" style="padding:12px;padding-bottom:100px">
    <!-- Section: MODE PRESET -->
    <div class="form-sec" style="border-color:var(--blue)">
        <div class="form-title" style="color:var(--blue)">
            <i class="ph ph-magic-wand"></i> GUNAKAN MODE PRESET
        </div>
        <div class="form-group">
            <select id="cfg_PRESET_MODE" class="form-input" onchange="applyPreset(this.value)" style="border-color:var(--blue); font-weight:800">
                <option value="CUSTOM">🛠️ CUSTOM (Bebas Edit Sendiri)</option>
                <option value="SMART_SCALPER">🧠 SMART SCALPER (Win Rate Tinggi ⭐)</option>
                <option value="SCALPING_10USD">💰 SCALPING 10 USD (Khusus Saldo Kecil)</option>
                <option value="SCALPING_PREMIUM">⚡ SCALPING PREMIUM (Akurasi & Win Rate)</option>
                <option value="SCALPING">⚡ SCALPING (Tinggi Resiko, Cepat)</option>
                <option value="SNIPER">🎯 SNIPER (Sabar, Akurasi Tinggi)</option>
                <option value="STANDARD">⚖️ STANDARD (Seimbang & Aman)</option>
            </select>
            <div id="preset_note" style="font-size:10px; color:var(--gray); margin-top:6px; font-style:italic">
                Note: Mode Custom memungkinkan Anda mengedit semua angka di bawah secara manual.
            </div>
        </div>
    </div>

    <div class="form-sec" style="border-color:var(--gold)">
        <div class="form-title" style="color:var(--gold)">
            <i class="ph ph-clock"></i> TIMEFRAME & KONFIRMASI (PRIMARY & FILTER)
        </div>
        <div class="form-grid">
            <div class="form-group">
                <label class="form-label">Primary TF (Entry) <i class="ph ph-question help-icon" onclick="showHelp('primary_tf_h')"></i></label>
                <select id="cfg_PRIMARY_TF" class="form-input">
                    <option value="1m">1m (Scalping)</option>
                    <option value="5m">5m (Standard)</option>
                    <option value="15m">15m (Sniper)</option>
                    <option value="1h">1h (Slow)</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">Confirm TF (Filter) <i class="ph ph-question help-icon" onclick="showHelp('confirm_tf_h')"></i></label>
                <select id="cfg_CONFIRM_TF" class="form-input">
                    <option value="1m">1m</option>
                    <option value="5m">5m</option>
                    <option value="15m">15m</option>
                    <option value="1h">1h</option>
                    <option value="4h">4h</option>
                </select>
            </div>
        </div>
        <div class="form-group" style="margin-top:12px">
            <label class="form-label">Wajib Konfirmasi MTF? <i class="ph ph-question help-icon" onclick="showHelp('mtf_h')"></i></label>
            <select id="cfg_REQUIRE_MTF_CONFIRM" class="form-input">
                <option value="true">Ya (Lebih Aman)</option>
                <option value="false">Tidak (Lebih Agresif)</option>
            </select>
        </div>
    </div>

    <div class="form-sec" style="border-color:var(--gold)">
        <div class="form-title" style="color:var(--gold)">
            <i class="ph ph-flask"></i> MODE EKSEKUSI
        </div>
        <div class="form-group">
            <label class="form-label">Gunakan Mode Dry Run (Simulasi)?</label>
            <select id="cfg_DRY_RUN" class="form-input" style="font-weight:800; color:var(--gold)">
                <option value="true">✅ YA (Gunakan Saldo Virtual)</option>
                <option value="false">⚠️ TIDAK (LIVE TRADING - UANG ASLI)</option>
            </select>
            <div style="font-size:10px; color:var(--sub); margin-top:6px; font-style:italic">
                Note: Jika dimatikan (LIVE), pastikan API Key MEXC Anda memiliki saldo USDT dan izin Futures.
            </div>
        </div>
    </div>

    <div class="form-sec">
        <div class="form-title"><i class="ph ph-currency-btc"></i> PILIHAN KOIN (PAIR)</div>
        <div class="form-group">
            <label class="form-label" style="display:flex; justify-content:space-between;">
                <span>Pilih Koin Trading <i class="ph ph-question help-icon" onclick="showHelp('symbol_h')"></i></span>
                <span id="txtLoadCoins" style="color:var(--gold); cursor:pointer;" onclick="loadCoins()">⟳ Refresh</span>
            </label>
            
            <!-- Custom Searchable Selector -->
            <div id="coinSelector" class="coin-selector-btn" onclick="openCoinSearch()">
                <div class="coin-info-mini">
                    <div class="coin-logo-mini" id="selectedCoinLogo">?</div>
                    <div style="font-weight:700; font-size:15px" id="selectedCoinName">Memuat...</div>
                </div>
                <i class="ph ph-caret-right"></i>
            </div>
            <input type="hidden" id="cfg_SYMBOL" class="form-input">

            <div style="font-size:10px; color:var(--red); margin-top:10px; font-weight:600">
                Peringatan: Ganti koin wajib dalam posisi tanpa trade / floating!
            </div>
        </div>
    </div>

    <div class="form-sec">
        <div class="form-title"><i class="ph ph-shield-check"></i> MANAJEMEN RISIKO</div>
        <div class="form-grid">
            <div class="form-group"><label class="form-label">Risk % Per Trade <i class="ph ph-question help-icon" onclick="showHelp('risk_h')"></i></label><input type="number" id="cfg_RISK_PER_TRADE" step="0.01" class="form-input"></div>
            <div class="form-group"><label class="form-label">Max Margin % <i class="ph ph-question help-icon" onclick="showHelp('margin_h')"></i></label><input type="number" id="cfg_MAX_MARGIN_PCT" step="0.01" class="form-input"></div>
            <div class="form-group"><label class="form-label">Leverage <i class="ph ph-question help-icon" onclick="showHelp('lev_h')"></i></label><input type="number" id="cfg_LEVERAGE" class="form-input"></div>
            <div class="form-group"><label class="form-label">Max Positions <i class="ph ph-question help-icon" onclick="showHelp('max_h')"></i></label><input type="number" id="cfg_MAX_OPEN_TRADES" class="form-input"></div>
        </div>
        <div class="form-grid" style="margin-top:12px">
            <div class="form-group"><label class="form-label">Daily Loss Limit % <i class="ph ph-question help-icon" onclick="showHelp('loss_h')"></i></label><input type="number" id="cfg_MAX_DAILY_LOSS_PCT" step="0.01" class="form-input"></div>
            <div class="form-group"><label class="form-label">Max Drawdown % <i class="ph ph-question help-icon" onclick="showHelp('dd_h')"></i></label><input type="number" id="cfg_MAX_DRAWDOWN_PCT" step="0.01" class="form-input"></div>
        </div>
        <div class="form-grid" style="margin-top:12px; border-top:1px solid rgba(255,255,255,0.05); padding-top:12px">
            <div class="form-group">
                <label class="form-label">BE Activation % <i class="ph ph-question help-icon" onclick="showHelp('be_h')"></i></label>
                <input type="number" id="cfg_BE_ACTIVATION_PCT" step="0.001" class="form-input" placeholder="0.005">
            </div>
            <div class="form-group">
                <label class="form-label">BE Safeguard % <i class="ph ph-question help-icon" onclick="showHelp('bes_h')"></i></label>
                <input type="number" id="cfg_BE_SAFEGUARD_PCT" step="0.0001" class="form-input" placeholder="0.0002">
            </div>
            <div class="form-group">
                <label class="form-label">Allow Fast Re-entry <i class="ph ph-question help-icon" onclick="showHelp('fast_h')"></i></label>
                <select id="cfg_ALLOW_FAST_REENTRY" class="form-input">
                    <option value="true">Ya (Agresif)</option>
                    <option value="false">Tidak (Sabar)</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">Partial TP1 <i class="ph ph-question help-icon" onclick="showHelp('ptp_h')"></i></label>
                <select id="cfg_TP1_PARTIAL_CLOSE" class="form-input">
                    <option value="true">Aktif</option>
                    <option value="false">Mati</option>
                </select>
            </div>
            <div class="form-group">
                <label class="form-label">Close % at TP1 <i class="ph ph-question help-icon" onclick="showHelp('ptpc_h')"></i></label>
                <input type="number" id="cfg_TP1_CLOSE_PCT" class="form-input" placeholder="50">
            </div>
        </div>
    </div>

    <div class="form-sec">
        <div class="form-title"><i class="ph ph-target"></i> TARGET PROFIT & SL</div>
        <div class="form-grid">
            <div class="form-group"><label class="form-label">ATR SL Mult <i class="ph ph-question help-icon" onclick="showHelp('sl_h')"></i></label><input type="number" id="cfg_ATR_SL_MULT" step="0.1" class="form-input"></div>
            <div class="form-group"><label class="form-label">ATR TP1 Mult <i class="ph ph-question help-icon" onclick="showHelp('tp_h')"></i></label><input type="number" id="cfg_ATR_TP1_MULT" step="0.1" class="form-input"></div>
            <div class="form-group"><label class="form-label">ATR TP2 Mult <i class="ph ph-question help-icon" onclick="showHelp('tp_h')"></i></label><input type="number" id="cfg_ATR_TP2_MULT" step="0.1" class="form-input"></div>
            <div class="form-group"><label class="form-label">ATR TP3 Mult <i class="ph ph-question help-icon" onclick="showHelp('tp_h')"></i></label><input type="number" id="cfg_ATR_TP3_MULT" step="0.1" class="form-input"></div>
        </div>
        <div class="form-grid" style="margin-top:10px">
            <div class="form-group"><label class="form-label">Trailing Activation % <i class="ph ph-question help-icon" onclick="showHelp('tp_h')"></i></label><input type="number" id="cfg_TRAIL_ACTIVATION_PCT" step="0.001" class="form-input"></div>
            <div class="form-group"><label class="form-label">Min TP Distance % <i class="ph ph-question help-icon" onclick="showHelp('min_tp_h')"></i></label><input type="number" id="cfg_MIN_TP_DISTANCE_PCT" step="0.01" class="form-input"></div>
        </div>
    </div>

    <div class="form-sec">
        <div class="form-title"><i class="ph ph-chart-line"></i> SINYAL & INDIKATOR</div>
        <div class="form-grid">
            <div class="form-group"><label class="form-label">Min Bull Score <i class="ph ph-question help-icon" onclick="showHelp('score_h')"></i></label><input type="number" id="cfg_MIN_BULL_SCORE" class="form-input"></div>
            <div class="form-group"><label class="form-label">Min Bear Score <i class="ph ph-question help-icon" onclick="showHelp('score_h')"></i></label><input type="number" id="cfg_MIN_BEAR_SCORE" class="form-input"></div>
            <div class="form-group"><label class="form-label">ADX Min Threshold <i class="ph ph-question help-icon" onclick="showHelp('adx_h')"></i></label><input type="number" id="cfg_ADX_MIN_THRESHOLD" class="form-input"></div>
            <div class="form-group"><label class="form-label">Min Volume Ratio <i class="ph ph-question help-icon" onclick="showHelp('signal')"></i></label><input type="number" id="cfg_MIN_VOL_RATIO" step="0.1" class="form-input"></div>
            <div class="form-group"><label class="form-label">Poll Interval (Sec) <i class="ph ph-question help-icon" onclick="showHelp('signal')"></i></label><input type="number" id="cfg_POLL_INTERVAL" class="form-input"></div>
        </div>
    </div>

    <div class="form-sec">
        <div class="form-title"><i class="ph ph-piggy-bank"></i> AMAN DI SPOT (WEALTH PROTECTION)</div>
        <div class="form-grid">
            <div class="form-group"><label class="form-label">Enable Secure <i class="ph ph-question help-icon" onclick="showHelp('sec_h')"></i></label>
                <select id="cfg_ENABLE_AUTO_SECURE" class="form-input">
                    <option value="true">Ya</option>
                    <option value="false">Tidak</option>
                </select>
            </div>
            <div class="form-group"><label class="form-label">% Profit Dipindah <i class="ph ph-question help-icon" onclick="showHelp('secp_h')"></i></label><input type="number" id="cfg_SECURE_PROFIT_PCT" class="form-input"></div>
        </div>
    </div>

    <button id="btnSaveConfig" class="btn-save-cfg" onclick="saveSettings()">SIMPAN PENGATURAN</button>
</div>

<!-- Premium Search Modal -->
<div id="coinSearchModal" class="search-modal">
    <div class="search-hdr">
        <button class="close-search" onclick="closeCoinSearch()"><i class="ph ph-arrow-left"></i></button>
        <div class="search-input-wrapper">
            <i class="ph ph-magnifying-glass"></i>
            <input type="text" id="coinSearchInput" class="search-input" placeholder="Cari koin (contoh: BTC, PEPE)..." oninput="filterCoins(this.value)">
        </div>
    </div>
    <div id="coinListContainer" class="coin-list">
        <!-- Coin items populated by JS -->
    </div>
</div>

<!-- Modal Bantuan -->
<div class="modal-overlay" id="helpModal" onclick="closeHelp()">
    <div class="modal-content" onclick="event.stopPropagation()">
        <div class="modal-title"><i class="ph ph-info"></i> <span id="helpTitle">Bantuan</span></div>
        <div class="modal-text" id="helpBody">...</div>
        <button class="modal-btn" onclick="closeHelp()">Mengerti</button>
    </div>
</div>

<!-- Navigation -->
<nav class="nav">
    <div class="nav-item active" onclick="switchTab('home', this)">
        <i class="ph ph-house"></i><span>Home</span>
    </div>
    <div class="nav-item" onclick="switchTab('history', this)">
        <i class="ph ph-scroll"></i><span>Riwayat</span>
    </div>
    <div class="nav-item" onclick="switchTab('analysis', this)">
        <i class="ph ph-chart-line-up"></i><span>Analisis</span>
    </div>
    <div class="nav-item" onclick="switchTab('settings', this)">
        <i class="ph ph-gear"></i><span>Setelan</span>
    </div>
</nav>

<script>
const $ = id => document.getElementById(id);
const fmt = v => v>=0 ? '+$'+v.toFixed(4) : '\\u2212$'+Math.abs(v).toFixed(4);

let currentTab = 'home';
let countdownInterval = null;

// Tab Switching
function switchTab(tab, el) {
    currentTab = tab;
    document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    
    // Aktifkan tab berdasarkan ID (format: tabName)
    const tabId = 'tab' + tab.charAt(0).toUpperCase() + tab.slice(1);
    const target = $(tabId);
    if(target) target.classList.add('active');
    
    // Aktifkan link di nav bar
    if(el) {
        el.classList.add('active');
    } else {
        const navItems = document.querySelectorAll('.nav-item');
        if(tab === 'home') navItems[0].classList.add('active');
        else if(tab === 'history') navItems[1].classList.add('active');
        else if(tab === 'analysis') navItems[2].classList.add('active');
        else if(tab === 'settings') navItems[3].classList.add('active');
    }
    
    // Auto-sync positions when switching to home tab
    if(tab === 'home' && !window.firstLoadSynced) {
       fetch('/api/sync_positions', {method:'POST'}).then(() => refresh());
       window.firstLoadSynced = true;
    }
    
    if(tab === 'history') {
        if(!$('histDate').value) {
            $('histDate').value = new Date().toISOString().split('T')[0];
        }
        loadHistory();
    } else if(tab === 'analysis') {
        loadAnalysis();
    } else if(tab === 'settings') {
        loadSettings();
    }
}

// Analisis & Charting
let chEquity, chWinLoss, chSymbols;

async function loadAnalysis() {
    try {
        const r = await fetch('/api/analysis');
        const d = await r.json();
        if(!d.success) {
            console.warn('Analysis failed or no data:', d.error);
            return;
        }

        $('anaWinRate').textContent = d.metrics.win_rate + '%';
        $('anaPF').textContent = d.metrics.profit_factor;
        $('anaAvgWin').textContent = '+$' + d.metrics.avg_win;
        $('anaAvgLoss').textContent = '-$' + d.metrics.avg_loss;

        // Inisialisasi Chart Apex
        const eqData = d.charts.equity;
        const finalVal = eqData[eqData.length - 1].y;
        const startVal = eqData[0].y;
        const chartColor = finalVal >= startVal ? '#4ade80' : '#f87171';

        // Cari titik tertinggi (Peak)
        let peakPoint = eqData[0];
        eqData.forEach(p => { if(p.y > peakPoint.y) peakPoint = p; });

        const equityOpt = {
            series: [{ name: 'Saldo', data: eqData }],
            chart: { type: 'area', height: 250, toolbar: { show: false }, background: 'transparent' },
            colors: [chartColor],
            fill: { type: 'gradient', gradient: { shadeIntensity: 1, opacityFrom: 0.4, opacityTo: 0.05 } },
            stroke: { curve: 'smooth', width: 3 },
            annotations: {
                points: [
                    {
                        x: new Date(peakPoint.x).getTime(),
                        y: peakPoint.y,
                        marker: { size: 6, fillColors: '#f0c040', strokeColor: '#fff', strokeWidth: 2 },
                        label: { 
                            text: "ATH: $" + peakPoint.y, 
                            borderWidth: 0,
                            style: { color: '#000', background: '#f0c040', fontWeight: 700, padding: { left: 5, right: 5, top: 3, bottom: 3 } } 
                        }
                    },
                    {
                        x: new Date(eqData[eqData.length - 1].x).getTime(),
                        y: finalVal,
                        marker: { size: 6, fillColors: chartColor, strokeColor: '#fff', strokeWidth: 2 },
                        label: { 
                            text: "Now: $" + finalVal, 
                            borderWidth: 0,
                            textAnchor: 'end',
                            offsetY: -15,
                            offsetX: -10,
                            style: { color: '#fff', background: chartColor, fontWeight: 700, padding: { left: 5, right: 5, top: 3, bottom: 3 } } 
                        }
                    }
                ]
            },
            dataLabels: { enabled: false },
            grid: { borderColor: '#1e3a5f', strokeDashArray: 4, padding: { right: 50 } },
            xaxis: { type: 'datetime', labels: { style: { colors: '#9ca3af', fontSize: '10px' } } },
            yaxis: { labels: { style: { colors: '#9ca3af' }, formatter: (v) => '$' + v.toFixed(0) } },
            theme: { mode: 'dark' }
        };
        if(chEquity) chEquity.destroy();
        chEquity = new ApexCharts($("equityChart"), equityOpt); chEquity.render();

        const wlOpt = {
            series: d.charts.win_loss,
            chart: { type: 'donut', height: 250 },
            labels: ['Wins ($' + d.metrics.gross_profit + ')', 'Losses ($' + d.metrics.gross_loss + ')'],
            colors: ['#4ade80', '#f87171'],
            stroke: { show: false },
            legend: { position: 'bottom', labels: { colors: '#e0e6ff' } },
            dataLabels: { enabled: true, dropShadow: { enabled: false } },
            theme: { mode: 'dark' }
        };
        if(chWinLoss) chWinLoss.destroy();
        chWinLoss = new ApexCharts($("winLossChart"), wlOpt); chWinLoss.render();

        const symOpt = {
            series: [{ name: 'PnL', data: d.charts.symbols.values }],
            chart: { type: 'bar', height: 250, toolbar: { show: false } },
            xaxis: { categories: d.charts.symbols.labels, labels: { style: { colors: '#9ca3af' } } },
            colors: [function({ value }) {
                return value >= 0 ? '#4ade80' : '#f87171';
            }],
            plotOptions: {
                bar: { distributed: true }
            },
            legend: { show: false },
            theme: { mode: 'dark' }
        };
        if(chSymbols) chSymbols.destroy();
        chSymbols = new ApexCharts($("symbolChart"), symOpt); chSymbols.render();

    } catch (e) { console.error('Error load analysis:', e); }
}

async function loadSettings() {
    try {
        await loadCoins(); // Populate dropdown first
        
        const r = await fetch('/api/config');
        const cfg = await r.json();
        for (const [key, val] of Object.entries(cfg)) {
            const el = $('cfg_' + key);
            if (el) {
                if(key === 'SYMBOL') {
                    selectCoin(val, true); // Update custom UI
                }
                el.value = val;
            }
        }
        applyPreset($('cfg_PRESET_MODE').value);
    } catch (e) { console.error(e); }
}

let allCoinsData = [];
let allSignalsData = {};

async function loadCoins() {
    const txt = $('txtLoadCoins');
    if(txt) txt.innerHTML = '<span class="spinner" style="width:10px;height:10px;display:inline-block"></span>';
    
    try {
        const r = await fetch('/api/top_coins');
        const res = await r.json();
        if(res.success && res.data.length > 0) {
            allCoinsData = res.data;
            allSignalsData = res.signals || {};
            renderCoinList(allCoinsData);
        }
    } catch(e) { console.error("Error load coins", e); }
    if(txt) txt.innerHTML = '⟳ Refresh';
}

function renderCoinList(data) {
    const container = $('coinListContainer');
    let html = '';
    
    // Sort logic for recommended: Bull/Bear >= 4 AND ADX > 20 (Strong Trending)
    const recommended = data.filter(c => {
        const s = allSignalsData[c.symbol];
        return s && (s.bull >= 4 || s.bear >= 4) && s.adx >= 20;
    }).sort((a,b) => {
        const sa = allSignalsData[a.symbol], sb = allSignalsData[b.symbol];
        // Priority: Highest score first
        return Math.max(sb.bull, sb.bear) - Math.max(sa.bull, sa.bear);
    });

    if(recommended.length > 0) {
        html += '<div class="sec-title" style="margin-top:10px; color:var(--green)">🔥 TREN KUAT (ADX > 20)</div>';
        recommended.slice(0, 10).forEach(c => html += createCoinItem(c, true));
    } else {
        // Fallback: Show highest scores if no strong trend found
        const topScores = data.filter(c => {
            const s = allSignalsData[c.symbol];
            return s && (s.bull >= 4 || s.bear >= 4);
        }).sort((a,b) => {
            const sa = allSignalsData[a.symbol], sb = allSignalsData[b.symbol];
            return Math.max(sb.bull, sb.bear) - Math.max(sa.bull, sa.bear);
        }).slice(0, 5);
        
        if(topScores.length > 0) {
            html += '<div class="sec-title" style="margin-top:10px">✨ SINYAL TERBAIK SAAT INI</div>';
            topScores.forEach(c => html += createCoinItem(c, true));
        }
    }

    html += '<div class="sec-title" style="margin-top:20px">💎 SEMUA KOIN (MEXC TOP 80)</div>';
    data.forEach(c => html += createCoinItem(c, false));
    
    container.innerHTML = html;
}

function createCoinItem(c, isRec) {
    const shortSym = c.symbol.replace('_USDT', '');
    const sig = allSignalsData[c.symbol];
    const chg = (c.change * 100).toFixed(2);
    const color = (c.change || 0) >= 0 ? 'var(--green)' : 'var(--red)';
    const vol = c.turnover > 1e9 ? (c.turnover/1e9).toFixed(1) + 'B' : (c.turnover/1e6).toFixed(1) + 'M';
    
    // Fallback for missing last price
    const lastPrice = parseFloat(c.last || 0);
    const displayPrice = isNaN(lastPrice) ? '0.00' : lastPrice.toFixed(lastPrice < 1 ? 6 : 4);

    // ADX & Signal Info compact
    let sigInfo = '';
    if(sig) {
        const adxNum = parseFloat(sig.adx || 0);
        const adxVal = adxNum.toFixed(1);
        const isStrongTrend = adxNum >= 25;
        const trendBadge = isStrongTrend ? `<span style="color:var(--gold); font-size:9px">⭐</span>` : '';
        
        // Sesuai permintaan: Hanya tampilkan ADX jika di atas 20
        if (adxNum > 20) {
            let adxColor = 'var(--gold)';
            if(adxNum >= 30) adxColor = 'var(--green)';

            if(sig.bull >= 4 || sig.bear >= 4) {
                const score = sig.signal === 'LONG' ? sig.bull : sig.bear;
                const sigColor = sig.signal === 'LONG' ? 'var(--green)' : 'var(--red)';
                sigInfo = `<span style="font-size:10px; color:${sigColor}; font-weight:800; margin-left:6px">${sig.signal}${score} | <span style="color:${adxColor}">A:${adxVal}${trendBadge}</span></span>`;
            } else {
                sigInfo = `<span style="font-size:10px; color:${adxColor}; background:rgba(255,255,255,0.05); padding:2px 4px; border-radius:4px; margin-left:6px">ADX:${adxVal}${trendBadge}</span>`;
            }
        } else if (sig.bull >= 4 || sig.bear >= 4) {
            // Jika ADX rendah tapi sinyal tetap kuat, tampilkan sinyal saja tanpa ADX
            const score = sig.signal === 'LONG' ? sig.bull : sig.bear;
            const sigColor = sig.signal === 'LONG' ? 'var(--green)' : 'var(--red)';
            sigInfo = `<span style="font-size:10px; color:${sigColor}; font-weight:800; margin-left:6px">${sig.signal}${score}</span>`;
        }
    } else {
        // Placeholder saat scanner sedang bekerja
        sigInfo = `<span style="font-size:10px; color:#fff; opacity:0.6; margin-right:2px">A:..</span>`;
    }

    return `
    <div class="coin-item" onclick="selectCoin('${c.symbol}')">
        <div class="coin-main">
            <div class="coin-logo-mini">${shortSym[0]}</div>
            <div class="coin-sym-box">
                <span class="coin-sym-name">${shortSym}</span>
                <span class="coin-sym-sub">Vol: $${vol}</span>
            </div>
        </div>
        <div class="coin-stats">
            <div class="coin-price" style="margin-bottom:2px">$${displayPrice}</div>
            <div class="coin-change" style="display:flex; align-items:center; justify-content:flex-end">
                ${sigInfo}
                <span style="color:${color}; font-weight:700; margin-left:6px">${(c.change||0)>=0?'+':''}${chg}%</span>
            </div>
        </div>
    </div>`;
}

function filterCoins(query) {
    const q = query.trim().toUpperCase();
    if (!q) {
        renderCoinList(allCoinsData);
        return;
    }
    
    let filtered = allCoinsData.filter(c => c.symbol.includes(q) || c.symbol.includes(q + '_USDT'));
    
    // Jika tidak ada hasil atau hanya sedikit, tambahkan opsi manual
    const container = $('coinListContainer');
    renderCoinList(filtered);
    
    // Tambahkan tombol manual jika kata kunci cukup unik
    if (q.length >= 2) {
        const fullSym = q.includes('_USDT') ? q : q + '_USDT';
        const manualHtml = `
            <div class="sec-title" style="margin-top:20px; color:var(--blue)">📍 KOIN KHUSUS (CUSTOM)</div>
            <div class="coin-item" onclick="selectCoin('${fullSym}')" style="border: 1px dashed var(--blue)">
                <div class="coin-main">
                    <div class="coin-logo-mini" style="background:var(--blue)">+</div>
                    <div class="coin-sym-box">
                        <span class="coin-sym-name">${fullSym}</span>
                        <span class="coin-sym-sub">Klik untuk gunakan koin ini</span>
                    </div>
                </div>
                <i class="ph ph-plus-circle" style="color:var(--blue)"></i>
            </div>`;
        container.innerHTML += manualHtml;
    }
}

function openCoinSearch() {
    // Check if locked by preset
    if ($('cfg_PRESET_MODE').value !== 'CUSTOM' && $('cfg_PRESET_MODE').value !== '') {
       // Optional: Notify user why they can't change coin if you want, 
       // but here SYMBOL is usually allowed to change even in preset.
    }
    $('coinSearchModal').classList.add('active');
    $('coinSearchInput').focus();
}

function closeCoinSearch() {
    $('coinSearchModal').classList.remove('active');
}

function selectCoin(symbol, skipClose = false) {
    if(!symbol) return;
    $('cfg_SYMBOL').value = symbol;
    const short = symbol.replace('_USDT', '');
    $('selectedCoinName').textContent = short + ' / USDT';
    $('selectedCoinLogo').textContent = short[0];
    
    // Also update Header Logo
    if($('symbolLogo')) $('symbolLogo').textContent = short;

    if(!skipClose) closeCoinSearch();
}

async function saveSettings() {
    const btn = $('btnSaveConfig');
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner"></span> Menyimpan...';
    try {
        const payload = {};
        document.querySelectorAll('.form-input').forEach(input => {
            const key = input.id.replace('cfg_', '');
            let val = input.value;
            if (input.type === 'number') val = parseFloat(val);
            if (val === 'true') val = true;
            if (val === 'false') val = false;
            payload[key] = val;
        });
        const r = await fetch('/api/config', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        const res = await r.json();
        if(res.success) alert('✅ Pengaturan berhasil disimpan!');
        else alert('❌ Gagal menyimpan: ' + (res.error || 'Terjadi kesalahan sistem'));
    } catch (e) { alert('❌ Error: ' + e); }
    finally {
        btn.disabled = false;
        btn.innerHTML = 'SIMPAN PENGATURAN';
    }
}

// Reset Logic
async function manualStop() {
    if(!confirm('Berhenti trading manual?')) return;
    await fetch('/api/manual_stop', {method:'POST'});
    refresh();
}

async function resetCircuit() {
    if(!confirm('Restart trading?')) return;
    const r = await fetch('/api/reset_circuit', {method:'POST'});
    const res = await r.json();
    if(res.success) refresh(); else alert(res.error);
}

async function resetAll() {
    if(!confirm('BAHAYA: Ini akan meriset saldo ke $100 dan menghapus riwayat. Lanjut?')) return;
    const r = await fetch('/api/reset_all', {method:'POST'});
    if((await r.json()).success) {
        alert('Bot berhasil di-reset ke $100!');
        window.location.reload();
    }
}

// Help Modal Logic — Comprehensive Field Descriptions
const helpData = {
    preset_desc: {title: 'Mode Preset', text: 'Pilih template strategi yang sudah diuji. Jika memilih preset selain Custom, field di bawah akan terkunci otomatis untuk menjaga integritas strategi.'},
    coin: {title: 'Pilihan Koin', text: 'Pilih pasangan koin yang ingin di-trade. Daftar diambil dari 100 koin dengan volume tertinggi di MEXC.'},
    symbol_h: {title: 'Koin Trades', text: 'Koin yang saat ini dipantau oleh bot. Ganti hanya saat TIDAK ADA posisi terbuka.'},
    risk: {title: 'Manajemen Risiko', text: 'Pusat kendali keuangan bot untuk mengatur besarnya modal dan batasan rugi.'},
    risk_h: {title: 'Risk % Per Trade', text: 'Persentase saldo bersih yang dipertaruhkan untuk satu transaksi. Disarankan 0.1-0.2% (Agresif).'},
    margin_h: {title: 'Max Margin %', text: 'Batas maksimal uang jaminan (margin) yang bisa dipakai per trade. Keamanan berlapis.'},
    lev_h: {title: 'Leverage', text: 'Daya ungkit modal. Semakin tinggi leverage, semakin cepat profit/loss dan semakin dekat harga likuidasi.'},
    max_h: {title: 'Max Positions', text: 'Jumlah maksimal koin berbeda yang boleh di-trade secara bersamaan.'},
    loss_h: {title: 'Daily Loss Limit %', text: 'Rem otomatis. Jika rugi harian menyentuh angka ini, bot berhenti trade sampai besok pagi WIB.'},
    dd_h: {title: 'Max Drawdown %', text: 'Batas penurunan saldo dari titik tertinggi sepanjang masa. Safety net terakhir.'},
    target: {title: 'Target Profit & SL', text: 'Pengaturan jarak ambil untung dan stop rugi berdasarkan volatilitas (ATR).'},
    sl_h: {title: 'ATR SL Mult', text: 'Jarak Stop Loss dikali nilai volatilitas. 1.5 - 2.0 ideal untuk scalping.'},
    tp_h: {title: 'ATR TP1 Mult', text: 'Target profit pertama. Minimal 1.5 agar sebanding dengan resiko (RR 1:1).'},
    signal: {title: 'Sinyal & Indikator', text: 'Setting sensitivitas bot dalam mendeteksi trend pasar.'},
    score_h: {title: 'Bull/Bear Score', text: 'Skor minimal akumulasi indikator (RSI, EMA, MACD, STOCH) sebelum buka posisi.'},
    adx_h: {title: 'ADX Filter', text: 'Mengatur batas minimum kekuatan tren. Di bawah angka ini, market dianggap sideways (ranging) dan bot tidak akan masuk. Disarankan: 20-25.'},
    min_tp_h: {title: 'Min TP Distance', text: 'Batas minimal potensi profit (TP1) agar bot mau masuk. Jika jarak TP1 terlalu tipis, bot akan HOLD untuk menghindari fee yang lebih besar dari profit.'},
    spot: {title: 'Tabungan Spot', text: 'Fitur Wealth Protection untuk mengamankan profit asli Anda.'},
    sec_h: {title: 'Auto Move', text: 'Jika aktif, sebagian profit akan langsung ditransfer ke wallet Spot Anda.'},
    secp_h: {title: 'Persentase Pindah', text: 'Berapa persen dari profit per trade yang mau "dicelengi". Disarankan 50%.'},
    be_h: {title: 'Break-Even Activation', text: 'Pindahkan SL ke Entry secara otomatis saat profit sudah mencapai X% untuk menghilangkan risiko.'},
    bes_h: {title: 'BE Safeguard Offset', text: 'Memberikan sedikit jarak aman (buffer) saat BE aktif agar jika harga loncat sedikit (slippage), Anda tidak rugi biaya fee/adanya tick lari. Disarankan 0.0002%.'},
    fast_h: {title: 'Fast Re-entry', text: 'Jika aktif, bot akan langsung mencari sinyal baru setelah posisi ditutup tanpa menunggu candle saat ini selesai. Sangat bagus untuk market cepat.'},
    ptp_h: {title: 'TP1 Profit Securing', text: 'Jika aktif, bot akan menjual sebagian posisi saat menyentuh TP1 untuk mengamankan profit di bank.'},
    ptpc_h: {title: 'Persentase Jual TP1', text: 'Berapa persen posisi yang mau dijual di TP1. Sisa posisi akan lanjut mengejar TP2/TP3.'},
    primary_tf_h: {title: 'Primary Timeframe', text: 'Timeframe utama yang digunakan bot untuk mencari sinyal entry dan exit. 1m sangat cepat (Scalping), sedang 15m/1h lebih lambat namun lebih akurat (Trend Following).'},
    confirm_tf_h: {title: 'Confirmation Timeframe', text: 'Timeframe kedua yang digunakan untuk memfilter tren besar. Biasanya lebih tinggi dari Primary TF.'},
    mtf_h: {title: 'Multi-Timeframe Confirmation', text: 'Jika diaktifkan, bot hanya akan masuk jika tren di Primary TF searah dengan Confirm TF. Ini sangat dianjurkan untuk mengurangi risiko "False Signal".'}
};

const presets = {
    CUSTOM: null,
    SCALPING_10USD: { PRIMARY_TF: '1m', CONFIRM_TF: '15m', REQUIRE_MTF_CONFIRM: false, RISK_PER_TRADE: 0.15, LEVERAGE: 20, MAX_MARGIN_PCT: 0.50, ATR_SL_MULT: 1.5, ATR_TP1_MULT: 1.5, MIN_RR_RATIO: 1.0, MIN_BULL_SCORE: 6, MIN_BEAR_SCORE: 6, ADX_MIN_THRESHOLD: 20, MIN_TP_DISTANCE_PCT: 0.30, BE_ACTIVATION_PCT: 0.003, BE_SAFEGUARD_PCT: 0.0002, ALLOW_FAST_REENTRY: true, TP1_PARTIAL_CLOSE: true, TP1_CLOSE_PCT: 70, TRAIL_ACTIVATION_PCT: 0.005, TRAIL_DISTANCE_PCT: 0.003 },
    SCALPING_PREMIUM: { PRIMARY_TF: '1m', CONFIRM_TF: '15m', REQUIRE_MTF_CONFIRM: false, RISK_PER_TRADE: 0.15, LEVERAGE: 50, MAX_MARGIN_PCT: 0.30, ATR_SL_MULT: 1.2, ATR_TP1_MULT: 1.2, MIN_RR_RATIO: 0.9, MIN_BULL_SCORE: 7, MIN_BEAR_SCORE: 7, ADX_MIN_THRESHOLD: 20, MIN_TP_DISTANCE_PCT: 0.35, BE_ACTIVATION_PCT: 0.003, BE_SAFEGUARD_PCT: 0.0005, ALLOW_FAST_REENTRY: true, TP1_PARTIAL_CLOSE: true, TP1_CLOSE_PCT: 70, TRAIL_ACTIVATION_PCT: 0.005, TRAIL_DISTANCE_PCT: 0.003 },
    SCALPING: { PRIMARY_TF: '1m', CONFIRM_TF: '5m', REQUIRE_MTF_CONFIRM: false, RISK_PER_TRADE: 0.20, LEVERAGE: 50, MAX_MARGIN_PCT: 0.40, ATR_SL_MULT: 1.5, ATR_TP1_MULT: 1.5, MIN_RR_RATIO: 1.0, MIN_BULL_SCORE: 6, MIN_BEAR_SCORE: 6, ADX_MIN_THRESHOLD: 15, MIN_TP_DISTANCE_PCT: 0.30, BE_ACTIVATION_PCT: 0.008, BE_SAFEGUARD_PCT: 0.002, ALLOW_FAST_REENTRY: true, TP1_PARTIAL_CLOSE: true, TP1_CLOSE_PCT: 70, TRAIL_ACTIVATION_PCT: 0.008, TRAIL_DISTANCE_PCT: 0.005 },
    SMART_SCALPER: { PRIMARY_TF: '5m', CONFIRM_TF: '15m', REQUIRE_MTF_CONFIRM: true, RISK_PER_TRADE: 0.10, LEVERAGE: 25, MAX_MARGIN_PCT: 0.25, ATR_SL_MULT: 1.5, ATR_TP1_MULT: 2.0, MIN_RR_RATIO: 1.2, MIN_BULL_SCORE: 7, MIN_BEAR_SCORE: 7, ADX_MIN_THRESHOLD: 22, MIN_TP_DISTANCE_PCT: 0.20, BE_ACTIVATION_PCT: 0.008, BE_SAFEGUARD_PCT: 0.002, ALLOW_FAST_REENTRY: true, TP1_PARTIAL_CLOSE: true, TP1_CLOSE_PCT: 50, TRAIL_ACTIVATION_PCT: 0.012, TRAIL_DISTANCE_PCT: 0.005 },
    SNIPER: { PRIMARY_TF: '15m', CONFIRM_TF: '1h', REQUIRE_MTF_CONFIRM: true, RISK_PER_TRADE: 0.10, LEVERAGE: 20, MAX_MARGIN_PCT: 0.20, ATR_SL_MULT: 2.5, ATR_TP1_MULT: 4.0, MIN_RR_RATIO: 1.5, MIN_BULL_SCORE: 7, MIN_BEAR_SCORE: 7, ADX_MIN_THRESHOLD: 25, MIN_TP_DISTANCE_PCT: 0.50, BE_ACTIVATION_PCT: 0.008, BE_SAFEGUARD_PCT: 0.0001, ALLOW_FAST_REENTRY: false, TP1_PARTIAL_CLOSE: true, TP1_CLOSE_PCT: 40, TRAIL_ACTIVATION_PCT: 0.015, TRAIL_DISTANCE_PCT: 0.008 },
    STANDARD: { PRIMARY_TF: '5m', CONFIRM_TF: '1h', REQUIRE_MTF_CONFIRM: false, RISK_PER_TRADE: 0.15, LEVERAGE: 25, MAX_MARGIN_PCT: 0.30, ATR_SL_MULT: 2.0, ATR_TP1_MULT: 2.5, MIN_RR_RATIO: 1.2, MIN_BULL_SCORE: 6, MIN_BEAR_SCORE: 6, ADX_MIN_THRESHOLD: 15, MIN_TP_DISTANCE_PCT: 0.35, BE_ACTIVATION_PCT: 0.005, BE_SAFEGUARD_PCT: 0.0001, ALLOW_FAST_REENTRY: false, TP1_PARTIAL_CLOSE: true, TP1_CLOSE_PCT: 50, TRAIL_ACTIVATION_PCT: 0.01, TRAIL_DISTANCE_PCT: 0.005 }
};

function applyPreset(mode) {
    const data = presets[mode];
    const inputs = document.querySelectorAll('.form-input');
    
    inputs.forEach(inp => {
        const key = inp.id.replace('cfg_', '');
        // Biarkan 'Preset Mode', 'Execution Mode (Dry Run)', dan 'Symbol' tetap bisa diakses
        if(['PRESET_MODE', 'DRY_RUN', 'SYMBOL'].includes(key)) return;
        
        if(mode === 'CUSTOM') {
            inp.disabled = false;
            inp.style.opacity = '1';
            inp.style.background = '';
        } else if(data && data.hasOwnProperty(key)) {
            inp.value = data[key];
            inp.disabled = true;
            inp.style.opacity = '0.5';
            inp.style.background = '#1e293b';
        } else if(data) {
            // Field yang tidak ada di preset tetap dikunci di mode selain custom
            inp.disabled = true;
            inp.style.opacity = '0.5';
            inp.style.background = '#1e293b';
        }
    });
    
    $('preset_note').innerHTML = mode === 'CUSTOM' ? 
        '<span style="color:var(--green)">🔓 Mode Custom: Semua pengaturan bebas diubah.</span>' : 
        '<span style="color:var(--blue)">🔒 Mode ' + mode.replace('_', ' ') + ': Pengaturan dikunci otomatis untuk akurasi strategi.</span>';
}

function showHelp(type) {
    const data = helpData[type];
    if(!data) return;
    $('helpTitle').textContent = data.title;
    $('helpBody').textContent = data.text;
    $('helpModal').style.display = 'flex';
}

function closeHelp() {
    $('helpModal').style.display = 'none';
}

// Data Fetching
async function loadHistory() {
    const date = $('histDate').value;
    const r = await fetch('/api/history?date=' + date);
    const d = await r.json();
    
    $('histTotalWin').textContent = fmt(d.total_win);
    $('histTotalLoss').textContent = fmt(d.total_loss);
    $('histDailyPnl').textContent = fmt(d.total_pnl);
    $('histDailyPnl').className = 'card-val ' + (d.total_pnl >= 0 ? 'pos' : 'neg');
    
    const body = $('histBody');
    if(d.history.length === 0) {
        body.innerHTML = '<tr><td colspan="4" style="text-align:center;color:var(--gray);padding:20px">Tidak ada riwayat</td></tr>';
        return;
    }
    
    body.innerHTML = d.history.map(t => {
        const symLabel = (t.symbol || 'UNK').replace('_USDT', '');
        return `
        <tr>
            <td>
                <span class="hist-side ${t.side==='LONG'?'badge-long':'badge-short'}">${t.side}</span><br>
                <small style="color:var(--gray);font-size:9px">${t.closed_at.split(' ')[1]}</small>
            </td>
            <td style="font-weight:700;color:var(--gold);font-size:11px">
                ${symLabel}
            </td>
            <td style="font-size:10px">
                In: $${parseFloat(t.entry_price).toFixed(6)}<br>
                Out: $${parseFloat(t.exit_price).toFixed(6)}
            </td>
            <td style="text-align:right;font-weight:700" class="${t.pnl>=0?'pos':'neg'}">
                ${t.pnl >= 0 ? '+' : ''}${parseFloat(t.pnl).toFixed(2)}
            </td>
        </tr>`;
    }).join('');
}

async function refresh(){
  if(currentTab !== 'home') return;
  try{
    const d = await(await fetch('/api/state')).json();
    
    // Header & Badges
    const symShort = d.symbol ? d.symbol.replace('_USDT', '') : '...';
    $('symbolLogo').textContent = symShort;
    $('currPrice').innerHTML = '$' + d.live_price.toFixed(6) + '<small>' + symShort + ' Futures</small>';
    $('modeBadge').textContent = d.is_dry_run ? 'DRY RUN' : 'LIVE';
    $('tfBadge').textContent = 'TF: ' + (d.primary_tf || '—') + ' / ' + (d.confirm_tf || '—');
    $('levBadge').textContent = 'LEV ' + d.leverage + 'X';
    $('sessBadge').textContent = d.session || '—';
    $('wsDot').className = d.live_price > 0 ? 'dot' : 'dot ws-off';
    
    // Hide reset button if in LIVE mode
    const resetBtn = $('btnResetAll');
    if(resetBtn) resetBtn.style.display = d.is_dry_run ? 'block' : 'none';

    // Circuit Breaker
    if(d.circuit_breaker) {
        $('cbBanner').style.display='block';
        $('cbReason').textContent=d.circuit_reason;
        updateCircuitTimer(d);
    } else {
        $('cbBanner').style.display='none';
    }

    // Stats
    const fEquity = d.futures_equity || d.balance || 0;
    const netWorth = (fEquity) + (d.spot_balance || 0);
    $('totalNetWorth').textContent = '$' + netWorth.toFixed(2);
    $('balance').textContent = '$' + fEquity.toFixed(2);
    $('spotBalance').textContent = '$' + (d.spot_balance || 0).toFixed(2);

    if (d.is_dry_run) {
        $('pnlSub').textContent = 'Peak: $' + d.peak_balance.toFixed(2);
    } else {
        $('pnlSub').innerHTML = `<span style="color:var(--gold)">Avail Futures: $${d.available.toFixed(2)}</span>`;
    }
    const tp = $('totalPnl'); tp.textContent = fmt(d.total_pnl); tp.className = 'card-val '+(d.total_pnl>=0?'pos':'neg');
    $('dailyPnl').textContent = 'Daily: ' + fmt(d.daily_pnl);
    $('drawdown').textContent = d.drawdown + '%';
    $('securedTot').textContent = '$' + d.secured_total.toFixed(2);
    $('winRate').textContent = d.win_rate + '%';
    $('tradeCount').textContent = d.total_trades + ' trades';
    $('iteration').textContent = '#' + d.iteration;
    $('startedAt').textContent = d.started_at;

    // Analysis
    let sig = d.signal || 'NEUTRAL';
    const box = $('signalBox');
    const sv = $('sigVal');
    
    if(d.hold_reason) {
        sig = 'HOLD';
        box.className = 'sig-box sig-neutral';
        sv.textContent = 'HOLD';
        sv.style.color = 'var(--gold)';
        // Tampilkan alasan hold
        $('sigDetails').innerHTML = `<span style="color:var(--gold);font-weight:700">⚠️ ${d.hold_reason}</span>`;
    } else {
        box.className = 'sig-box ' + (sig==='LONG'?'sig-long':sig==='SHORT'?'sig-short':'sig-neutral');
        sv.textContent = sig;
        sv.className = 'sig-dir ' + sig.toLowerCase();
        sv.style.color = '';
        $('sigDetails').innerHTML = `Bull: <span class="pos" id="bullScore">${d.bull_score}</span> | Bear: <span class="neg" id="bearScore">${d.bear_score}</span> | Conf: <b id="confVal">${d.confidence}</b>%`;
    }
    
    $('iRsi').textContent = d.rsi;
    $('iAtr').textContent = '$' + (d.atr < 0.01 ? d.atr.toFixed(6) : d.atr.toFixed(4));
    $('iStoch').textContent = d.stoch_k;
    // ADX — warna merah jika sideways, hijau jika trending
    if($('iAdx')) {
        const adxEl = $('iAdx');
        const adxVal = d.adx || 0;
        adxEl.textContent = adxVal.toFixed(1);
        adxEl.style.color = adxVal >= 20 ? (adxVal >= 30 ? 'var(--green)' : 'var(--gold)') : 'var(--red)';
        adxEl.title = adxVal >= 30 ? 'Tren kuat ✅' : adxVal >= 20 ? 'Tren moderat' : 'Sideways ⚠️ Entry diblokir';
    }

    // Positions
    const pDiv = $('positions');
    if(!d.positions || d.positions.length === 0) {
        pDiv.innerHTML = '<div style="color:var(--gray);font-size:13px;text-align:center;padding:20px">Tidak ada posisi terbuka</div>';
    } else {
        pDiv.innerHTML = d.positions.map(p => {
            const badges = [];
            if(p.trailing_active) badges.push('<span style="background:var(--blue);color:#fff;padding:1px 6px;border-radius:4px;font-size:9px;font-weight:700">TRAILING</span>');
            if(p.risk_reduced) badges.push('<span style="background:var(--gold);color:#000;padding:1px 6px;border-radius:4px;font-size:9px;font-weight:700">RISK-50%</span>');
            const badgeHtml = badges.length ? '<div style="display:flex;gap:4px;margin-top:4px">' + badges.join('') + '</div>' : '';
            return `
            <div class="pos-card">
                <div class="pos-hdr" style="justify-content:space-between">
                    <div style="display:flex; align-items:center; gap:6px">
                        <span class="badge ${p.side==='LONG'?'badge-long':'badge-short'}">${p.side}</span>
                        <span style="font-weight:900;font-size:14px;color:var(--gold)">${p.symbol.replace('_USDT','')}</span>
                    </div>
                    <span style="font-size:10px; color:var(--gray)">${p.id}</span>
                </div>
                ${badgeHtml}
                <div class="pos-grid" style="margin-top:10px">
                    <dt>Harga Entry</dt><dd>$${parseFloat(p.entry_price).toFixed(6)}</dd>
                    <dt>Harga Mark</dt><dd style="color:var(--blue)">$${parseFloat(p.current_price).toFixed(6)}</dd>
                    <dt>Live PnL</dt><dd class="${p.live_pnl>=0?'pos':'neg'}" style="font-size:14px; font-weight:800">${p.live_pnl >= 0 ? '+' : ''}$${p.live_pnl.toFixed(4)}</dd>
                    <dt>Stop Loss</dt><dd style="color:var(--red)">$${p.stop_loss.toFixed(6)}</dd>
                    <dt>Target 1</dt><dd style="color:var(--green)">$${p.take_profit1.toFixed(6)}</dd>
                    <dt>Target 2/3</dt><dd style="color:var(--green)">$${(p.take_profit2 || 0).toFixed(4)} / $${(p.take_profit3 || 0).toFixed(4)}</dd>
                    <dt>Quantity</dt><dd>${p.quantity}</dd>
                </div>
            </div>
        `;

        }).join('');
    }
  }catch(e){console.error(e)}
}

function updateCircuitTimer(d) {
  const triggeredAt = d.circuit_triggered_at;
  const cooldown = 600;
  if(countdownInterval) clearInterval(countdownInterval);
  const tick = () => {
    let remaining = Math.max(0, cooldown - (Date.now()/1000 - triggeredAt));
    if(d.circuit_type === 'MANUAL') remaining = 0; // Bypass cooldown jika manual
    
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

setInterval(refresh, 3000);
refresh();
</script>
</body>
</html>
"""
# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MEXC Pro Trading Bot v2.0")
    parser.add_argument("--dashboard", action="store_true", help="Aktifkan web dashboard di port 5000")
    parser.add_argument("--live", action="store_true", help="Gunakan live trading (override DRY_RUN)")
    args = parser.parse_args()

    cli_overrides = {}
    if args.live:
        cli_overrides["DRY_RUN"] = False
        log.warning("⚠️  LIVE TRADING MODE AKTIF — Uang nyata digunakan!")

    bot = XAUTBot(run_dashboard=args.dashboard, cli_overrides=cli_overrides if cli_overrides else None)
    bot.run()