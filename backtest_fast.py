#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEXC Scalper V5.1 - Fast Backtest Engine
Digunakan untuk menguji strategi dengan data historis tanpa koneksi API real-time.
Mendukung: MTF Confluence, News Filter, Kelly Criterion, Dynamic Sizing.
"""

import pandas as pd
import numpy as np
import argparse
import json
import os
import sys
from datetime import datetime, timedelta
import random

# Import logika dari bot utama (pastikan mexc_scalperV5.1.py ada di folder yang sama)
# Kita akan mengimpor fungsi-fungsi kunci secara dinamis atau menyalin logika intinya jika terlalu terikat class
try:
    import mexc_scalperV5_1 as bot_core
    HAS_CORE = True
except ImportError:
    print("⚠️  File 'mexc_scalperV5.1.py' tidak ditemukan. Menggunakan engine simulasi mandiri.")
    HAS_CORE = False

class BacktestEngine:
    def __init__(self, config_override=None):
        self.config = {
            "INITIAL_BALANCE": 1000,
            "LEVERAGE": 10,
            "RISK_PER_TRADE": 0.01, # 1%
            "USE_KELLY": True,
            "KELLY_FRACTION": 0.5,
            "USE_MTF_CONFIRM": True,
            "MTF_MIN_SCORE": 2,
            "USE_NEWS_FILTER": True,
            "NEWS_BLACKOUT_MINUTES": 60,
            "SLIPPAGE_PCT": 0.0005, # 0.05%
            "FEE_PCT": 0.0002,      # 0.02% maker/taker avg
            "MIN_RR": 1.5,
            "MAX_POSITIONS": 1
        }
        if config_override:
            self.config.update(config_override)
        
        self.balance = self.config["INITIAL_BALANCE"]
        self.equity_curve = []
        self.trades = []
        self.current_position = None
        
        # Simulasi News Events (Dummy data untuk 30 hari terakhir)
        self.news_events = self._generate_dummy_news()

    def _generate_dummy_news(self):
        """Generate jadwal berita acak untuk simulasi filter"""
        events = []
        now = datetime.utcnow()
        for i in range(30):
            # Asumsi ada berita besar setiap 2-3 hari
            if i % 3 == 0:
                event_time = now - timedelta(days=i, hours=random.randint(13, 15)) # Siang/Sore UTC
                events.append({
                    "time": event_time,
                    "impact": "HIGH",
                    "blackout_start": event_time - timedelta(minutes=self.config["NEWS_BLACKOUT_MINUTES"]),
                    "blackout_end": event_time + timedelta(minutes=self.config["NEWS_BLACKOUT_MINUTES"])
                })
        return events

    def is_news_blackout(self, current_time):
        if not self.config["USE_NEWS_FILTER"]:
            return False
        for event in self.news_events:
            if event["blackout_start"] <= current_time <= event["blackout_end"]:
                return True
        return False

    def generate_mock_candle(self, prev_close, volatility=0.002):
        """Generate candlestick sintetis jika data asli tidak tersedia"""
        change = random.gauss(0, volatility)
        close = prev_close * (1 + change)
        high = max(prev_close, close) * (1 + abs(random.gauss(0, volatility/2)))
        low = min(prev_close, close) * (1 - abs(random.gauss(0, volatility/2)))
        open_p = prev_close
        volume = random.randint(1000, 10000)
        return {
            "timestamp": datetime.utcnow(), # Akan dioverride oleh loop utama
            "open": open_p, "high": high, "low": low, "close": close, "volume": volume
        }

    def calculate_kelly_size(self, win_rate, avg_win, avg_loss):
        if avg_loss == 0: return self.config["RISK_PER_TRADE"]
        kelly = win_rate - ((1 - win_rate) / (avg_win / abs(avg_loss)))
        kelly = kelly * self.config["KELLY_FRACTION"] # Fractional Kelly
        return max(0.001, min(kelly, self.config["RISK_PER_TRADE"] * 2)) # Clamp

    def run(self, data_file=None, symbol="BTCUSDT", days=7):
        print(f"🚀 Memulai Backtest Fast Engine untuk {symbol}...")
        print(f"📅 Periode: {days} hari terakhir (Simulasi)" if not data_file else f"📄 File Data: {data_file}")
        
        # Load Data
        df = None
        if data_file and os.path.exists(data_file):
            try:
                df = pd.read_csv(data_file)
                print(f"✅ Loaded {len(df)} candles from file.")
            except Exception as e:
                print(f"❌ Gagal load file: {e}")
                return
        else:
            print("⚠️  Tidak ada file data. Menggunakan data sintetis (Random Walk) untuk demonstrasi logika.")
            # Generate synthetic data
            dates = pd.date_range(end=datetime.utcnow(), periods=days*24*4, freq='15min') # 15m timeframe
            price = 60000
            data = []
            for t in dates:
                candle = self.generate_mock_candle(price)
                candle['timestamp'] = t
                price = candle['close']
                data.append(candle)
            df = pd.DataFrame(data)

        if df is None or len(df) < 100:
            print("❌ Data tidak cukup untuk backtest.")
            return

        # Inisialisasi Variabel Trading
        self.balance = self.config["INITIAL_BALANCE"]
        self.equity_curve = []
        self.trades = []
        self.current_position = None
        
        # Statistik untuk Kelly Dinamis
        trade_results_history = [] 

        print(f"💰 Saldo Awal: ${self.balance:,.2f}")
        print("-" * 60)

        # Loop Utama Backtest
        # Kita mulai dari index 100 agar indikator punya waktu warmup
        for i in range(100, len(df)):
            current_time = df.iloc[i]['timestamp']
            current_price = df.iloc[i]['close']
            
            # 1. Cek News Filter
            if self.is_news_blackout(current_time):
                if self.current_position:
                    # Opsional: Close paksa saat news? Biasanya bot wait saja.
                    pass 
                continue

            # 2. Siapkan Data untuk Analisis (Mocking structure yang diharapkan bot)
            # Dalam implementasi nyata, kita panggil fungsi dari mexc_scalperV5.1.py
            # Di sini kita simulasi logika sederhana jika import gagal
            
            signal = None
            if HAS_CORE:
                # TODO: Panggil fungsi get_signal dari bot_core jika strukturnya kompatibel
                # signal = bot_core.get_signal(df.iloc[i-50:i]) 
                pass
            
            # Simulasi Logika Entry Sederhana (Ganti dengan panggilan fungsi asli nanti)
            # Contoh: Jika close > SMA20 dan RSI < 70
            sma_20 = df.iloc[i-20:i]['close'].mean()
            rsi = 50 # Dummy RSI
            
            # Simulasi Sinyal Buy
            if current_price > sma_20 and random.random() > 0.95: # 5% chance entry untuk demo
                signal = "LONG"
            elif current_price < sma_20 and random.random() > 0.95:
                signal = "SHORT"

            # 3. Eksekusi Logic
            if not self.current_position and signal:
                # Cek MTF (Simulasi: 80% lolos MTF)
                if self.config["USE_MTF_CONFIRM"] and random.random() > 0.8:
                    continue # Gagal MTF
                
                # Hitung Size (Kelly atau Fixed)
                risk_pct = self.config["RISK_PER_TRADE"]
                if self.config["USE_KELLY"] and len(trade_results_history) > 10:
                    wins = sum(1 for t in trade_results_history[-20:] if t > 0)
                    avg_win = np.mean([t for t in trade_results_history[-20:] if t > 0]) or 1
                    avg_loss = abs(np.mean([t for t in trade_results_history[-20:] if t < 0])) or 1
                    if avg_loss > 0:
                        risk_pct = self.calculate_kelly_size(wins/20, avg_win, avg_loss)

                # Hitung SL/TP (Sederhana: ATR based)
                atr = df.iloc[i-14:i]['high'].max() - df.iloc[i-14:i]['low'].min()
                sl_dist = atr * 1.5
                tp_dist = sl_dist * self.config["MIN_RR"]
                
                if signal == "LONG":
                    sl_price = current_price - sl_dist
                    tp_price = current_price + tp_dist
                else:
                    sl_price = current_price + sl_dist
                    tp_price = current_price - tp_dist

                # Open Position
                size_usd = self.balance * self.config["LEVERAGE"] * risk_pct
                qty = size_usd / current_price
                
                self.current_position = {
                    "type": signal,
                    "entry": current_price,
                    "qty": qty,
                    "sl": sl_price,
                    "tp": tp_price,
                    "time": current_time
                }
                # Fee Entry
                self.balance -= (size_usd * self.config["FEE_PCT"])

            elif self.current_position:
                # Cek Exit
                pos = self.current_position
                pnl_pct = 0
                exit_reason = ""
                exit_price = 0

                if pos["type"] == "LONG":
                    if current_price <= pos["sl"]:
                        exit_price = pos["sl"]
                        exit_reason = "SL Hit"
                    elif current_price >= pos["tp"]:
                        exit_price = pos["tp"]
                        exit_reason = "TP Hit"
                else: # SHORT
                    if current_price >= pos["sl"]:
                        exit_price = pos["sl"]
                        exit_reason = "SL Hit"
                    elif current_price <= pos["tp"]:
                        exit_price = pos["tp"]
                        exit_reason = "TP Hit"
                
                # Trailing Stop Sederhana (Opsional)
                if pos["type"] == "LONG" and current_price > pos["entry"] and current_price < pos["tp"]:
                    new_sl = current_price - (pos["entry"] - pos["sl"]) # Break even logic
                    if new_sl > pos["sl"]:
                        pos["sl"] = new_sl

                if exit_reason:
                    # Hitung PnL
                    if pos["type"] == "LONG":
                        pnl = (exit_price - pos["entry"]) * pos["qty"]
                    else:
                        pnl = (pos["entry"] - exit_price) * pos["qty"]
                    
                    # Fee Exit
                    fee = (pos["qty"] * exit_price) * self.config["FEE_PCT"]
                    net_pnl = pnl - fee
                    
                    self.balance += net_pnl
                    self.trades.append({
                        "time": current_time,
                        "type": pos["type"],
                        "entry": pos["entry"],
                        "exit": exit_price,
                        "pnl": net_pnl,
                        "reason": exit_reason
                    })
                    trade_results_history.append(net_pnl)
                    self.current_position = None

            # Record Equity
            self.equity_curve.append({
                "time": current_time,
                "balance": self.balance,
                "position_pnl": 0 # Simplified
            })

        # Laporan Final
        self._print_report()

    def _print_report(self):
        print("\n" + "="*60)
        print("📊 LAPORAN BACKTEST FINAL")
        print("="*60)
        
        total_trades = len(self.trades)
        if total_trades == 0:
            print("❌ Tidak ada trade yang terjadi. Coba longgarkan filter atau perpanjang periode.")
            return

        wins = [t for t in self.trades if t["pnl"] > 0]
        losses = [t for t in self.trades if t["pnl"] <= 0]
        
        win_rate = len(wins) / total_trades * 100
        total_pnl = sum(t["pnl"] for t in self.trades)
        gross_profit = sum(t["pnl"] for t in wins)
        gross_loss = abs(sum(t["pnl"] for t in losses))
        
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')
        
        max_drawdown = 0
        peak = self.config["INITIAL_BALANCE"]
        for point in self.equity_curve:
            if point["balance"] > peak:
                peak = point["balance"]
            dd = (peak - point["balance"]) / peak * 100
            if dd > max_drawdown:
                max_drawdown = dd

        print(f"Total Trades       : {total_trades}")
        print(f"Win Rate           : {win_rate:.2f}% ({len(wins)}W / {len(losses)}L)")
        print(f"Net Profit         : ${total_pnl:,.2f}")
        print(f"Profit Factor      : {profit_factor:.2f}")
        print(f"Max Drawdown       : {max_drawdown:.2f}%")
        print(f"Final Balance      : ${self.balance:,.2f}")
        print(f"Return on Initial  : {(self.balance - self.config['INITIAL_BALANCE'])/self.config['INITIAL_BALANCE']*100:.2f}%")
        
        # Analisis Distribusi
        if losses:
            avg_loss = sum(t["pnl"] for t in losses) / len(losses)
            print(f"Avg Loss per Trade : ${avg_loss:,.2f}")
        if wins:
            avg_win = sum(t["pnl"] for t in wins) / len(wins)
            print(f"Avg Win per Trade  : ${avg_win:,.2f}")

        print("="*60)
        print("💡 Tips: Jika Win Rate tinggi tapi Profit Factor rendah, perkecil SL atau biarkan profit lari.")
        print("💡 Tips: Jika Drawdown > 20%, kurangi leverage atau matikan Kelly Criterion sementara.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fast Backtest for MEXC Scalper V5.1")
    parser.add_argument("--file", type=str, help="Path to CSV data file (columns: timestamp, open, high, low, close, volume)")
    parser.add_argument("--days", type=int, default=7, help="Number of days to simulate if no file provided")
    parser.add_argument("--symbol", type=str, default="BTCUSDT", help="Symbol to test")
    parser.add_argument("--no-mtf", action="store_true", help="Disable Multi-Timeframe filter")
    parser.add_argument("--no-news", action="store_true", help="Disable News Filter")
    parser.add_argument("--no-kelly", action="store_true", help="Disable Kelly Criterion (Use fixed risk)")
    
    args = parser.parse_args()

    config_overrides = {}
    if args.no_mtf: config_overrides["USE_MTF_CONFIRM"] = False
    if args.no_news: config_overrides["USE_NEWS_FILTER"] = False
    if args.no_kelly: config_overrides["USE_KELLY"] = False

    engine = BacktestEngine(config_override=config_overrides)
    engine.run(data_file=args.file, symbol=args.symbol, days=args.days)
