# 🥇 XAUT/USDT Pro Trading Bot v2.0 — MEXC

Bot trading emas profesional dengan fitur lengkap untuk MEXC.

## ✨ Fitur Lengkap

### Core Engine
- **WebSocket real-time price feed** — harga live dari MEXC tanpa delay
- **Multi-timeframe analysis (MTF)** — konfirmasi sinyal dari TF lebih tinggi
- **8 Indikator teknikal** — RSI, MACD, BB, EMA (9/21/50/200), ATR, Stochastic, OBV, VWAP
- **Sistem skor sinyal** — skor 0–14, entry hanya jika ≥ 7 + MTF konfirmasi

### Manajemen Risiko
- **Trailing Stop otomatis** — aktif setelah profit 1%, jarak 0.5%
- **Triple TP** — TP1 (move SL to BE) + TP2 (close) + TP3 (extended)
- **Kelly Criterion sizing** — opsional, ukuran posisi berbasis win rate historis
- **Max daily loss circuit breaker** — berhenti jika rugi 6%/hari
- **Max drawdown circuit breaker** — berhenti jika drawdown 15%
- **Dynamic position sizing** — 2% risiko per trade dari balance saat ini

### Session & News Filter
- **Session filter** — hanya trading jam 07:00–22:00 UTC (London + NY)
- **London/NY Overlap priority** — 13:00–16:00 UTC prime window
- **Weekend block** — skip Sabtu + Minggu subuh
- **News blackout** — konfigurasi event CPI/NFP/FOMC secara manual

### Infrastructure
- **State persistence** — resume otomatis setelah restart
- **Trade journal CSV** — log lengkap semua trade untuk analisis
- **Reconnect otomatis** — WebSocket reconnect hingga 10x
- **Graceful shutdown** — handle SIGINT/SIGTERM, simpan state
- **Dry Run mode** — simulasi penuh tanpa modal nyata

### Dashboard & Monitoring
- **Web dashboard** — monitoring real-time via browser (Flask)
- **Telegram notifikasi** — alert setiap open/close posisi, circuit breaker
- **Console status** — display indikator lengkap setiap iterasi

### Backtesting
- **Full backtest** — simulasi di semua data historis
- **Walk-forward analysis** — validasi strategi di multiple fold
- **Monte Carlo simulation** — estimasi distribusi return & max drawdown
- **Laporan detail** — win rate, profit factor, Sharpe ratio, expectancy

---

## 📦 Instalasi

```bash
# 1. Buat folder proyek
mkdir xaut_pro && cd xaut_pro

# 2. Virtual environment
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Setup API keys
cp .env.example .env
nano .env    # isi API key MEXC & Telegram
```

---

## 🔑 Setup MEXC API

1. Login ke [mexc.com](https://www.mexc.com)
2. **Akun → API Management → Buat API Key**
3. Aktifkan hanya: **Spot Trading** (Read + Trade)
4. ❌ **JANGAN aktifkan Withdraw permission**
5. Whitelist IP server kamu (opsional tapi disarankan)
6. Salin ke `.env`

---

## 📱 Setup Telegram (Opsional)

```
1. Buka Telegram → cari @BotFather
2. /newbot → ikuti instruksi → salin token
3. Cari @userinfobot → kirim pesan → salin id
4. Isi TELEGRAM_TOKEN dan TELEGRAM_CHAT_ID di .env
```

---

## 🚀 Cara Pakai

### Dry Run (Simulasi — Default)
```bash
python mexc_xaut_bot.py
```

### Dry Run + Dashboard
```bash
python mexc_xaut_bot.py --dashboard
# Buka browser: http://localhost:5000
```

### Live Trading
```bash
python mexc_xaut_bot.py --live
```
⚠️ Pastikan sudah dry run minimal 2 minggu!

### Backtest
```bash
# Backtest standar 1h 500 candle
python backtest.py

# Timeframe & jumlah candle custom
python backtest.py --tf 4h --candles 1000

# Walk-forward 5 fold
python backtest.py --wf --folds 5

# Walk-forward + Monte Carlo 1000 simulasi
python backtest.py --wf --mc 1000
```

---

## ⚙️ Konfigurasi Penting

Edit `CONFIG` di bagian atas `mexc_xaut_bot.py`:

| Parameter | Default | Keterangan |
|-----------|---------|------------|
| `PRIMARY_TF` | 15m | Timeframe analisis utama |
| `CONFIRM_TF` | 1h | Timeframe konfirmasi MTF |
| `RISK_PER_TRADE` | 0.02 | 2% balance per trade |
| `USE_TRAILING_STOP` | True | Trailing stop otomatis |
| `TRAIL_ACTIVATION_PCT` | 0.01 | Aktif setelah profit 1% |
| `TRAIL_DISTANCE_PCT` | 0.005 | Jarak trailing 0.5% |
| `USE_KELLY` | False | Kelly sizing (True = adaptif) |
| `MIN_BULL_SCORE` | 7 | Skor minimum LONG (max 14) |
| `MAX_DAILY_LOSS_PCT` | 0.06 | Circuit breaker harian 6% |
| `MAX_DRAWDOWN_PCT` | 0.15 | Circuit breaker total 15% |
| `REQUIRE_MTF_CONFIRM` | True | Wajib konfirmasi HTF |
| `USE_SESSION_FILTER` | True | Filter jam trading |
| `NEWS_BLACKOUT` | [] | Event yang diblokir |

### Contoh News Blackout
```python
"NEWS_BLACKOUT": [
    "04-10 18:30",   # CPI release
    "05-07 18:30",   # NFP
    "06-12 20:00",   # FOMC
]
```

---

## 📊 Sistem Skor Sinyal (Total Maks 14)

| Indikator | Bullish Max | Bearish Max |
|-----------|-------------|-------------|
| RSI | +2 | +2 |
| MACD Crossover | +2 | +2 |
| Bollinger Bands | +2 | +2 |
| EMA Alignment | +2 | +2 |
| Stochastic | +2 | +2 |
| OBV | +1 | +1 |
| VWAP | +1 | +1 |
| Volume Spike | +1 | +1 |

**Entry LONG:** Bull ≥ 7 + HTF BULLISH/NEUTRAL  
**Entry SHORT:** Bear ≥ 7 + HTF BEARISH/NEUTRAL

---

## 📁 Struktur File

```
xaut_pro/
├── mexc_xaut_bot.py     # Bot utama (semua fitur)
├── backtest.py          # Backtesting + walk-forward + Monte Carlo
├── requirements.txt     # Dependencies
├── .env.example         # Template konfigurasi
├── .env                 # Konfigurasi aktual (jangan di-commit!)
├── bot_state.json       # State persistence (auto-generated)
├── trade_journal.csv    # Log trade (auto-generated)
└── xaut_bot.log         # Log aktivitas (auto-generated)
```

---

## ⚠️ Disclaimer

Bot ini dibuat untuk tujuan edukasi dan otomasi trading pribadi. Trading aset kripto dan emas tokenized mengandung risiko tinggi termasuk risiko kehilangan seluruh modal. Selalu gunakan dana yang siap hilang, lakukan riset sendiri, dan konsultasikan dengan financial advisor sebelum trading. Performa backtest tidak menjamin hasil trading nyata di masa depan.
