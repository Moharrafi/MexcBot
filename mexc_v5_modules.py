import time
import logging
import threading
import pandas as pd
from typing import Optional

class BtcRegimeManager:
    """
    Pawang Cuaca BTC (Market Regime)
    Menyesuaikan konfigurasi bot berdasarkan tren BTC 1H.
    """
    def __init__(self, api_client, ta_engine, config_dict):
        self.api = api_client
        self.ta = ta_engine
        self.c = config_dict
        self.log = logging.getLogger("BTCRegime")
        self.last_check = 0
        self.check_interval_sec = 300  # 5 Menit
        
    def _fetch_btc_weather(self) -> str:
        try:
            df = self.api.get_klines("BTC_USDT", "1h", limit=100)
            if df is None or len(df) < 20: return "UNKNOWN"
            df = self.ta.compute(df)
            signal = self.ta.get_signal(df)
            adx = signal.get("adx", 0)
            roc = signal.get("roc", 0)
            if adx > 25 and roc > 0: return "BULL_TREND"
            elif adx > 25 and roc < 0: return "BEAR_TREND"
            else: return "SIDEWAYS"
        except Exception as e:
            self.log.error(f"Gagal membaca BTC: {e}")
            return "UNKNOWN"
            
    def apply_regime_to_config(self):
        now = time.time()
        if now - self.last_check < self.check_interval_sec:
            return
            
        weather = self._fetch_btc_weather()
        self.last_check = now
        
        if weather == "BULL_TREND":
            self.log.info("[REGIME] Cuaca BTC: BULL_TREND (Mode Trend-Following)")
            self.c["ATR_TP1_MULT"] = max(3.0, self.c.get("ATR_TP1_MULT", 2.5))
            self.c["TRAIL_ACTIVATION_PCT"] = 0.020
            self.c["EXIT_ON_SIGNAL_FLIP"] = False
        elif weather == "BEAR_TREND":
            self.log.info("[REGIME] Cuaca BTC: BEAR_TREND (Mode Bertahan)")
            self.c["ATR_TP1_MULT"] = min(2.0, self.c.get("ATR_TP1_MULT", 2.5))
            self.c["TRAIL_ACTIVATION_PCT"] = 0.010
            self.c["EXIT_ON_SIGNAL_FLIP"] = True
        elif weather == "SIDEWAYS":
            self.log.info("[REGIME] Cuaca BTC: SIDEWAYS (Mode Range Scalping)")
            self.c["ATR_TP1_MULT"] = 1.5
            self.c["TRAIL_ACTIVATION_PCT"] = 0.008
            self.c["EXIT_ON_SIGNAL_FLIP"] = True


class WsKlineManager:
    """
    Menyimpan K-Line secara real-time dari data WS (Anti-Lag).
    """
    def __init__(self, symbol: str, timeframe: str = "Min5"):
        self.symbol = symbol
        self.timeframe = timeframe
        self.df: Optional[pd.DataFrame] = None
        self._lock = threading.Lock()
        self.log = logging.getLogger("WsKline")
        
    def init_with_rest(self, rest_client):
        self.log.info(f"[{self.symbol}] Mengunduh fondasi K-Line via REST...")
        tf_str = "5m" if "5" in self.timeframe else "15m"
        self.df = rest_client.get_klines(self.symbol, interval=tf_str, limit=200)
        
    def on_kline_msg(self, msg: dict):
        if self.df is None or self.df.empty: return
        kline = msg.get("data", {})
        if not kline: return
        t_sec = int(kline.get("t", 0))
        if t_sec == 0: return
        open_time = pd.to_datetime(t_sec, unit="s")
        
        with self._lock:
            if open_time in self.df.index:
                self.df.loc[open_time, "close"] = float(kline.get("c", 0))
                self.df.loc[open_time, "high"] = float(kline.get("h", 0))
                self.df.loc[open_time, "low"] = float(kline.get("l", 0))
                self.df.loc[open_time, "volume"] = float(kline.get("q", 0))
            else:
                new_row = pd.DataFrame([{
                    "open": float(kline.get("o", 0)),
                    "close": float(kline.get("c", 0)),
                    "high": float(kline.get("h", 0)),
                    "low": float(kline.get("l", 0)),
                    "volume": float(kline.get("q", 0)),
                    "amount": float(kline.get("a", 0))
                }], index=[open_time])
                self.df = pd.concat([self.df, new_row])
                if len(self.df) > 300:
                    self.df = self.df.iloc[-300:]
                    
    def get_df(self) -> pd.DataFrame:
        with self._lock:
            return self.df.copy() if self.df is not None else pd.DataFrame()
