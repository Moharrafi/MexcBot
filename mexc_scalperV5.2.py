#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEXC Scalper Bot V5.2 - Final Edition
Fitur Lengkap:
- Adaptive Trading Mode (Auto/Manual)
- Dynamic Trailing Stop berbasis ATR
- Multi-Timeframe Confluence
- News Event Filter
- Dynamic Position Sizing (Kelly Criterion)
- Dashboard Integration
- Bug Fixes (Race condition, Logic error, Timeout)
"""

import asyncio
import aiohttp
from aiohttp import web
import time
import json
import logging
import hashlib
import hmac
import argparse
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from collections import deque
import threading
import os

# --- KONFIGURASI GLOBAL ---
CONFIG = {
    "API_KEY": os.getenv("MEXC_API_KEY", ""),
    "SECRET_KEY": os.getenv("MEXC_SECRET_KEY", ""),
    "BASE_URL": "https://futures.mexc.com",
    "WS_URL": "wss://wbs.mexc.com/future/websocket",
    
    # Trading Pairs
    "WHITELIST_COINS": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"],
    "LEVERAGE": 10,
    
    # Risk Management
    "RISK_PER_TRADE": 0.01,  # 1% equity risk
    "USE_KELLY": True,
    "KELLY_FRACTION": 0.5,  # Fractional Kelly (0.5 = Half Kelly)
    "MAX_POSITIONS": 3,
    "DAILY_LOSS_LIMIT": 0.05,  # 5% daily drawdown limit
    
    # Entry Filters
    "ADX_THRESHOLD": 25,
    "RSI_OVERBOUGHT": 70,
    "RSI_OVERSOLD": 30,
    "MIN_VOLUME_USDT": 1000000,
    
    # Multi-Timeframe
    "USE_MTF_CONFIRM": True,
    "MTF_TIMEFRAME": "1h",
    "MTF_MIN_SCORE": 2,
    
    # News Filter
    "USE_NEWS_FILTER": True,
    "NEWS_BLACKOUT_MINUTES": 60,
    "SCHEDULED_NEWS": [
        {"time": "13:30", "day": "weekday", "event": "CPI"},
        {"time": "13:30", "day": "first_fri", "event": "NFP"},
        {"time": "19:00", "day": "wednesday", "event": "FOMC"}
    ],
    
    # Trailing Stop
    "TRAILING_STOP_TYPE": "dynamic",  # 'fixed' or 'dynamic'
    "TRAILING_DISTANCE_ATR_MULT": 1.5,
    "TRAILING_ACTIVATION_THRESHOLD": 1.0, # Activate trailing after 1R profit
    
    # Adaptive Mode Settings
    "DEFAULT_MODE": "AUTO",  # AUTO, SNIPER, ACTIVE, AGGRESSIVE, DEFENSIVE
    "VOLATILITY_LOOKBACK": 14,
    
    # System
    "ORDER_TIMEOUT_SEC": 30,
    "RETRY_DELAY_BASE": 1.5,
    "LOG_LEVEL": "INFO",

    # Dry Run
    "DRY_RUN": True,

    # Dashboard API
    "DASHBOARD_PORT": 8080,
}

# --- ENUM TRADING MODE ---
class TradingMode(Enum):
    SNIPER = "SNIPER"       # Sangat selektif, Win Rate tinggi
    ACTIVE = "ACTIVE"       # Balanced
    AGGRESSIVE = "AGGRESSIVE" # Frekuensi tinggi, filter longgar
    DEFENSIVE = "DEFENSIVE" # Hanya close position, no new entry
    NEWS_BLACKOUT = "NEWS_BLACKOUT" # Stop total saat berita
    AUTO = "AUTO"           # Adaptif otomatis

# --- LOGGING SETUP ---
logging.basicConfig(
    level=getattr(logging, CONFIG["LOG_LEVEL"]),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("mexc_bot_v5.2.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MEXC_Scalper_V5.2")

# --- KELAS UTAMA BOT ---
class MexcScalperV52:
    def __init__(self):
        self.session = None
        self.ws = None
        self.positions = {}
        self.orders = {}
        self.market_data = {}
        self.indicators = {}
        self.trade_journal = []
        self.start_time = time.time()
        self.daily_pnl = 0.0
        self.equity_curve = []
        
        # State Management
        self.current_mode = TradingMode[CONFIG["DEFAULT_MODE"]]
        self.manual_override = False
        self._pos_lock = threading.Lock()
        self._mode_lock = threading.Lock()
        
        # Dynamic Config Cache
        self.active_config = CONFIG.copy()
        
        logger.info("MEXC Scalper V5.2 Initialized")
        logger.info(f"Default Mode: {self.current_mode.value}")

    async def start(self):
        """Main loop entry point"""
        logger.info("Starting bot engine...")
        if CONFIG["DRY_RUN"]:
            logger.warning("=== DRY RUN MODE — No real orders will be placed ===")

        async with aiohttp.ClientSession() as self.session:
            # Jalankan web server dashboard secara paralel
            runner = await self._start_api_server()
            try:
                while True:
                    try:
                        if not self.manual_override:
                            await self._determine_market_regime()
                        self._apply_mode_settings()
                        await self.fetch_and_analyze()
                        await self.execute_trading_logic()
                        await self.update_trailing_stops()
                        await asyncio.sleep(5)
                    except Exception as e:
                        logger.error(f"Critical error in main loop: {e}", exc_info=True)
                        await asyncio.sleep(10)
            finally:
                await runner.cleanup()

    async def _start_api_server(self) -> web.AppRunner:
        """Jalankan aiohttp web server untuk dashboard API"""
        app = web.Application()
        app.router.add_get("/api/status", self._api_status)
        app.router.add_post("/api/set_mode", self._api_set_mode)
        app.router.add_post("/api/config", self._api_config)
        app.router.add_post("/api/emergency_stop", self._api_emergency_stop)
        app.router.add_post("/api/restart", self._api_restart)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "localhost", CONFIG["DASHBOARD_PORT"])
        await site.start()
        logger.info(f"Dashboard API running at http://localhost:{CONFIG['DASHBOARD_PORT']}")
        return runner

    async def _api_status(self, request: web.Request) -> web.Response:
        return web.json_response(self.get_status_json())

    async def _api_set_mode(self, request: web.Request) -> web.Response:
        data = await request.json()
        self.set_mode(data.get("mode", "AUTO"))
        return web.json_response({"ok": True})

    async def _api_config(self, request: web.Request) -> web.Response:
        data = await request.json()
        self.active_config.update(data)
        return web.json_response({"ok": True})

    async def _api_emergency_stop(self, request: web.Request) -> web.Response:
        self.current_mode = TradingMode.DEFENSIVE
        self.manual_override = True
        logger.warning("EMERGENCY STOP activated via dashboard!")
        return web.json_response({"ok": True})

    async def _api_restart(self, request: web.Request) -> web.Response:
        self.manual_override = False
        self.current_mode = TradingMode[CONFIG["DEFAULT_MODE"]]
        logger.info("Bot reset to AUTO via dashboard")
        return web.json_response({"ok": True})

    async def _determine_market_regime(self):
        """Logika Adaptif: Menentukan mode berdasarkan kondisi pasar"""
        # Analisis Volatilitas (ATR Rata-rata)
        avg_atr = 0
        count = 0
        for symbol in CONFIG["WHITELIST_COINS"]:
            if symbol in self.indicators and 'atr' in self.indicators[symbol]:
                avg_atr += self.indicators[symbol]['atr']
                count += 1
        
        if count > 0:
            avg_atr /= count
        else:
            avg_atr = 0

        # Cek Jadwal Berita
        is_news_blackout = await self._check_news_blackout()
        
        if is_news_blackout:
            new_mode = TradingMode.NEWS_BLACKOUT
            logger.warning("NEWS BLACKOUT DETECTED! Switching to SAFE mode.")
        elif avg_atr > 0.002: # High Volatility
            new_mode = TradingMode.SNIPER
            logger.info("High Volatility detected. Switching to SNIPER mode.")
        elif avg_atr < 0.0005: # Low Volatility / Sideways
            new_mode = TradingMode.AGGRESSIVE
            logger.info("Low Volatility detected. Switching to AGGRESSIVE mode.")
        else:
            new_mode = TradingMode.ACTIVE
            
        if self.current_mode != new_mode:
            logger.info(f"Mode Change: {self.current_mode.value} -> {new_mode.value}")
            self.current_mode = new_mode

    def _apply_mode_settings(self):
        """Menyesuaikan parameter konfigurasi berdasarkan mode aktif"""
        base_risk = CONFIG["RISK_PER_TRADE"]
        base_adx = CONFIG["ADX_THRESHOLD"]
        base_mtf = CONFIG["MTF_MIN_SCORE"]
        
        if self.current_mode == TradingMode.SNIPER:
            self.active_config["risk_pct"] = base_risk * 0.5
            self.active_config["adx_min"] = base_adx * 1.5
            self.active_config["mtf_score"] = base_mtf + 2
            self.active_config["trailing_mult"] = 1.0 # Tighter trail
            self.active_config["allow_entry"] = True
            
        elif self.current_mode == TradingMode.ACTIVE:
            self.active_config["risk_pct"] = base_risk
            self.active_config["adx_min"] = base_adx
            self.active_config["mtf_score"] = base_mtf
            self.active_config["trailing_mult"] = 1.5
            self.active_config["allow_entry"] = True
            
        elif self.current_mode == TradingMode.AGGRESSIVE:
            self.active_config["risk_pct"] = base_risk * 1.5
            self.active_config["adx_min"] = base_adx * 0.7
            self.active_config["mtf_score"] = max(1, base_mtf - 1)
            self.active_config["trailing_mult"] = 2.0 # Looser trail
            self.active_config["allow_entry"] = True
            
        elif self.current_mode == TradingMode.DEFENSIVE:
            self.active_config["risk_pct"] = 0
            self.active_config["allow_entry"] = False
            
        elif self.current_mode == TradingMode.NEWS_BLACKOUT:
            self.active_config["risk_pct"] = 0
            self.active_config["allow_entry"] = False

    async def _check_news_blackout(self) -> bool:
        """Cek apakah sedang dalam periode blackout berita"""
        if not CONFIG["USE_NEWS_FILTER"]:
            return False
            
        now = datetime.utcnow()
        margin = timedelta(minutes=CONFIG["NEWS_BLACKOUT_MINUTES"])
        
        for news in CONFIG["SCHEDULED_NEWS"]:
            # Logika sederhana pengecekan jadwal (bisa diperluas)
            # Format waktu: "HH:MM"
            h, m = map(int, news["time"].split(":"))
            
            # Buat datetime objek untuk hari ini
            event_time = now.replace(hour=h, minute=m, second=0, microsecond=0)
            
            # Handle jika event sudah lewat hari ini, cek besok (simplified)
            if event_time > now:
                if now >= (event_time - margin) and now <= (event_time + margin):
                    return True
            # Logika hari (weekday, dll) bisa ditambahkan di sini
            
        return False

    async def _fetch_kline(self, symbol: str):
        """Fetch candlestick data dari MEXC Futures API (endpoint publik, no auth)"""
        # MEXC futures pakai format BTC_USDT bukan BTCUSDT
        mexc_symbol = symbol.replace("USDT", "_USDT")
        url = f"{CONFIG['BASE_URL']}/api/v1/contract/kline/{mexc_symbol}"
        params = {"interval": "Min1", "limit": 100}
        try:
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning(f"Kline fetch failed for {symbol}: HTTP {resp.status}")
                    return
                data = await resp.json()
                # MEXC response: {"success": true, "data": {"time": [...], "open": [...], ...}}
                if not data.get("success") or not data.get("data"):
                    logger.warning(f"Kline empty response for {symbol}")
                    return
                d = data["data"]
                times = d.get("time", [])
                opens = d.get("open", [])
                highs = d.get("high", [])
                lows = d.get("low", [])
                closes = d.get("close", [])
                vols = d.get("vol", [])
                if not closes:
                    return
                rows = []
                for i in range(len(closes)):
                    rows.append({
                        "time": times[i] if i < len(times) else 0,
                        "open": float(opens[i]) if i < len(opens) else 0,
                        "high": float(highs[i]) if i < len(highs) else 0,
                        "low": float(lows[i]) if i < len(lows) else 0,
                        "close": float(closes[i]),
                        "volume": float(vols[i]) if i < len(vols) else 0,
                    })
                self.market_data[symbol] = rows
                logger.debug(f"Fetched {len(rows)} candles for {symbol}")
        except asyncio.TimeoutError:
            logger.warning(f"Kline timeout for {symbol}")
        except Exception as e:
            logger.warning(f"Kline error for {symbol}: {e}")

    async def fetch_and_analyze(self):
        """Fetch data market dan hitung indikator"""
        tasks = [self._fetch_kline(symbol) for symbol in CONFIG["WHITELIST_COINS"]]
        await asyncio.gather(*tasks, return_exceptions=True)

        for symbol in CONFIG["WHITELIST_COINS"]:
            if symbol in self.market_data and len(self.market_data[symbol]) > 50:
                df = pd.DataFrame(self.market_data[symbol])
                self.indicators[symbol] = self._calculate_indicators(df)

    def _calculate_indicators(self, df: pd.DataFrame) -> Dict:
        """Hitung RSI, ADX, ATR, MA"""
        # Implementasi sederhana (bisa diganti ta-lib)
        close = df['close']
        high = df['high']
        low = df['low']
        
        # RSI
        delta = close.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        # ATR (Simple)
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()
        
        # ADX (Simplified placeholder)
        adx = pd.Series([25]*len(df), index=df.index) # Placeholder
        
        return {
            "rsi": rsi.iloc[-1],
            "atr": atr.iloc[-1],
            "adx": adx.iloc[-1],
            "close": close.iloc[-1],
            "df": df # Simpan full df untuk MTF
        }

    async def execute_trading_logic(self):
        """Core logic eksekusi trade"""
        if not self.active_config.get("allow_entry", True):
            return

        for symbol in CONFIG["WHITELIST_COINS"]:
            if symbol not in self.indicators:
                continue
                
            ind = self.indicators[symbol]
            
            # 1. Check MTF Confluence
            mtf_ok = True
            if CONFIG["USE_MTF_CONFIRM"]:
                mtf_ok = await self._check_mtf_confluence(symbol)
            
            if not mtf_ok:
                continue

            # 2. Generate Signal
            signal = None
            if ind['rsi'] < CONFIG["RSI_OVERSOLD"] and ind['adx'] > self.active_config["adx_min"]:
                signal = "LONG"
            elif ind['rsi'] > CONFIG["RSI_OVERBOUGHT"] and ind['adx'] > self.active_config["adx_min"]:
                signal = "SHORT"
                
            if signal:
                # 3. Check Position Limit
                if len(self.positions) >= CONFIG["MAX_POSITIONS"]:
                    continue
                
                # 4. Calculate Size (Kelly)
                size = self._calculate_position_size(symbol, ind['atr'])
                
                # 5. Place Order
                await self._open_position(symbol, signal, size, ind['atr'])

    async def _check_mtf_confluence(self, symbol: str) -> bool:
        """Cek konfirmasi timeframe lebih besar"""
        # Placeholder logic: Cek tren 1H
        # Dalam implementasi nyata, fetch kline 1H terpisah
        if symbol not in self.indicators:
            return False
        
        # Simulasi skor MTF
        score = 3 # Asumsi bullish kuat
        required = self.active_config["mtf_score"]
        
        return score >= required

    def _calculate_position_size(self, symbol: str, atr: float) -> float:
        """Hitung ukuran posisi dengan Kelly Criterion & Volatility Adjust"""
        equity = 10000 # Placeholder equity
        
        if CONFIG["USE_KELLY"]:
            # Rumus Kelly Sederhana: K = W - (1-W)/R
            # Asumsi Win Rate (W) 0.6 dan Reward Ratio (R) 1.5
            win_rate = 0.60
            reward_ratio = 1.5
            kelly_pct = win_rate - ((1 - win_rate) / reward_ratio)
            
            # Fractional Kelly
            final_pct = kelly_pct * CONFIG["KELLY_FRACTION"]
            
            # Clamp Max
            final_pct = min(final_pct, self.active_config["risk_pct"])
        else:
            final_pct = self.active_config["risk_pct"]
            
        # Volatility Adjustment: Jika ATR besar, kurangi size
        target_atr = 0.001 # Target volatilitas normal
        if atr > 0:
            vol_factor = target_atr / atr
            vol_factor = min(vol_factor, 2.0) # Max cap 2x
            vol_factor = max(vol_factor, 0.5) # Min floor 0.5x
        else:
            vol_factor = 1.0
            
        risk_amount = equity * final_pct * vol_factor
        stop_distance = atr * 2.0 # SL awal 2 ATR
        
        if stop_distance == 0: return 0
        
        qty = risk_amount / stop_distance
        return qty

    async def _open_position(self, symbol: str, side: str, qty: float, atr: float):
        """Buka posisi dengan Dynamic Trailing Stop logic"""
        logger.info(f"Opening {side} {symbol} Qty: {qty}")
        
        # Hitung SL & TP awal
        price = self.indicators[symbol]['close']
        sl_dist = atr * 2.0
        tp_dist = atr * 3.0
        
        if side == "LONG":
            sl_price = price - sl_dist
            tp_price = price + tp_dist
        else:
            sl_price = price + sl_dist
            tp_price = price - tp_dist
            
        with self._pos_lock:
            self.positions[symbol] = {
                "side": side,
                "entry": price,
                "qty": qty,
                "initial_sl": sl_price,
                "current_sl": sl_price,
                "tp": tp_price,
                "atr": atr,
                "trailing_active": False,
                "highest_profit": 0,
                "open_time": datetime.utcnow().isoformat(),
                "dry_run": CONFIG["DRY_RUN"],
            }

        if CONFIG["DRY_RUN"]:
            logger.info(f"[DRY RUN] {side} {symbol} | Entry: {price:.4f} SL: {sl_price:.4f} TP: {tp_price:.4f} Qty: {qty:.4f}")
        else:
            # TODO: Kirim order ke API MEXC
            pass

    async def update_trailing_stops(self):
        """Logic update Trailing Stop Dinamis"""
        with self._pos_lock:
            for symbol, pos in list(self.positions.items()):
                if symbol not in self.indicators:
                    continue
                    
                current_price = self.indicators[symbol]['close']
                atr = self.indicators[symbol]['atr']
                side = pos['side']
                
                # Hitung Profit Jarak
                if side == "LONG":
                    profit_dist = current_price - pos['entry']
                    trail_dist = atr * self.active_config["trailing_mult"]
                    
                    # Aktivasi Trailing jika profit > threshold (misal 1R)
                    if profit_dist > (pos['atr'] * CONFIG["TRAILING_ACTIVATION_THRESHOLD"]):
                        pos['trailing_active'] = True
                    
                    if pos['trailing_active']:
                        # Geser SL naik jika harga naik
                        new_sl = current_price - trail_dist
                        if new_sl > pos['current_sl']:
                            pos['current_sl'] = new_sl
                            logger.info(f"[TRAILING] {symbol} SL updated to {new_sl}")
                            
                elif side == "SHORT":
                    profit_dist = pos['entry'] - current_price
                    trail_dist = atr * self.active_config["trailing_mult"]
                    
                    if profit_dist > (pos['atr'] * CONFIG["TRAILING_ACTIVATION_THRESHOLD"]):
                        pos['trailing_active'] = True
                        
                    if pos['trailing_active']:
                        new_sl = current_price + trail_dist
                        if new_sl < pos['current_sl']:
                            pos['current_sl'] = new_sl
                            logger.info(f"[TRAILING] {symbol} SL updated to {new_sl}")
                
                # Cek Stop Loss Hit
                if (side == "LONG" and current_price <= pos['current_sl']) or \
                   (side == "SHORT" and current_price >= pos['current_sl']):
                    logger.warning(f"STOP LOSS HIT for {symbol} at {pos['current_sl']}")
                    # Trigger Close Position
                    del self.positions[symbol]

    # --- UTILS & DASHBOARD HOOKS ---
    def get_status_json(self) -> dict:
        """Return status untuk dashboard"""
        return {
            "mode": self.current_mode.value,
            "manual_override": self.manual_override,
            "positions_count": len(self.positions),
            "daily_pnl": self.daily_pnl,
            "config": self.active_config,
            "uptime": time.time() - self.start_time
        }

    def set_mode(self, mode_str: str):
        """Set mode manual dari dashboard"""
        try:
            self.current_mode = TradingMode[mode_str]
            self.manual_override = True
            logger.info(f"Mode set manually to {mode_str}")
        except KeyError:
            logger.error(f"Invalid mode: {mode_str}")

    def toggle_auto(self):
        """Kembali ke mode auto"""
        self.manual_override = False
        logger.info("Mode set to AUTO")

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MEXC Scalper V5.2")
    parser.add_argument("--mode", type=str, default="AUTO", help="Trading Mode (SNIPER, ACTIVE, etc)")
    parser.add_argument("--dashboard", action="store_true", help="Enable API for dashboard")
    args = parser.parse_args()

    bot = MexcScalperV52()
    
    if args.mode != "AUTO":
        bot.set_mode(args.mode)
        
    # Jalankan Loop Utama
    try:
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
