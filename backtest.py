"""
Backtesting Engine — XAUT/USDT Pro Bot v2.0
============================================
Simulasi historis lengkap dengan walk-forward analysis,
Monte Carlo simulation, dan laporan performa detail.

Jalankan:
    python backtest.py
    python backtest.py --tf 4h --candles 1000
    python backtest.py --wf       # walk-forward analysis
    python backtest.py --mc 500   # Monte Carlo 500 simulasi
"""

import json
import random
import argparse
import statistics
from datetime import datetime
from typing import List, Dict
from dataclasses import dataclass, field

import pandas as pd

from mexc_xaut_bot import (
    MEXCClient, TechnicalAnalysis, RiskManager, CONFIG, log
)

# ─────────────────────────────────────────────────────────────

@dataclass
class BTTrade:
    id: int
    side: str
    entry: float
    exit: float
    qty: float
    pnl: float
    reason: str
    duration_bars: int = 0
    trail_activated: bool = False
    date: str = ""

@dataclass
class BTResult:
    trades: List[BTTrade] = field(default_factory=list)
    balance: float = 10_000.0
    peak: float = 10_000.0
    start_bal: float = 10_000.0

    @property
    def wins(self): return [t for t in self.trades if t.pnl > 0]
    @property
    def losses(self): return [t for t in self.trades if t.pnl <= 0]
    @property
    def total_pnl(self): return sum(t.pnl for t in self.trades)
    @property
    def win_rate(self): return len(self.wins) / len(self.trades) * 100 if self.trades else 0
    @property
    def roi(self): return (self.balance - self.start_bal) / self.start_bal * 100
    @property
    def max_drawdown(self):
        bal = self.start_bal
        peak = bal
        max_dd = 0
        for t in self.trades:
            bal += t.pnl
            if bal > peak: peak = bal
            dd = (peak - bal) / peak * 100
            if dd > max_dd: max_dd = dd
        return max_dd
    @property
    def profit_factor(self):
        gross_profit = sum(t.pnl for t in self.wins)
        gross_loss   = abs(sum(t.pnl for t in self.losses))
        return round(gross_profit / gross_loss, 3) if gross_loss else float("inf")
    @property
    def avg_win(self): return statistics.mean([t.pnl for t in self.wins]) if self.wins else 0
    @property
    def avg_loss(self): return statistics.mean([t.pnl for t in self.losses]) if self.losses else 0
    @property
    def expectancy(self):
        if not self.trades: return 0
        wr = self.win_rate / 100
        return (wr * self.avg_win) + ((1 - wr) * self.avg_loss)
    @property
    def sharpe(self):
        if len(self.trades) < 2: return 0
        returns = [t.pnl / self.start_bal for t in self.trades]
        mean_r  = statistics.mean(returns)
        std_r   = statistics.stdev(returns)
        return round(mean_r / std_r * (252 ** 0.5), 3) if std_r else 0
    @property
    def max_consec_losses(self):
        max_c = cur = 0
        for t in self.trades:
            if t.pnl <= 0: cur += 1; max_c = max(max_c, cur)
            else: cur = 0
        return max_c


class Backtester:
    def __init__(self, initial_balance: float = 10_000.0):
        self.start_bal = initial_balance
        self.client    = MEXCClient("", "")
        self.ta        = TechnicalAnalysis(CONFIG)
        self.risk      = RiskManager(CONFIG)
        self._trade_id = 0

    def _fetch(self, symbol: str, tf: str, limit: int) -> pd.DataFrame:
        print(f"  Mengambil {limit} candle {symbol} {tf}...")
        df = self.client.get_klines(symbol, tf, limit)
        if df is None or len(df) < 60:
            raise ValueError("Data tidak cukup")
        df = self.ta.compute(df)
        df.dropna(inplace=True)
        print(f"  {len(df)} candle siap untuk backtest")
        return df

    def _simulate(self, df: pd.DataFrame, start_bal: float) -> BTResult:
        result   = BTResult(balance=start_bal, peak=start_bal, start_bal=start_bal)
        open_pos = None
        trail_high = trail_low = 0.0
        trail_sl = 0.0
        trail_active = False
        bar_in = 0

        for i in range(50, len(df)):
            window = df.iloc[:i+1]
            row    = window.iloc[-1]
            price  = row["close"]
            bar_in += 1

            if open_pos:
                side = open_pos["side"]
                sl   = trail_sl if trail_active else open_pos["sl"]

                # Update trailing
                if CONFIG["USE_TRAILING_STOP"] and not trail_active:
                    pct = (price - open_pos["entry"]) / open_pos["entry"] if side == "LONG" else \
                          (open_pos["entry"] - price) / open_pos["entry"]
                    if pct >= CONFIG["TRAIL_ACTIVATION_PCT"]:
                        trail_active = True
                        trail_high = trail_low = price
                        trail_sl = price * (1 - CONFIG["TRAIL_DISTANCE_PCT"]) if side == "LONG" else \
                                   price * (1 + CONFIG["TRAIL_DISTANCE_PCT"])

                if trail_active:
                    if side == "LONG" and price > trail_high:
                        trail_high = price
                        trail_sl   = max(trail_sl, price * (1 - CONFIG["TRAIL_DISTANCE_PCT"]))
                    elif side == "SHORT" and price < trail_low:
                        trail_low = price
                        trail_sl  = min(trail_sl, price * (1 + CONFIG["TRAIL_DISTANCE_PCT"]))
                    sl = trail_sl

                # Cek exit
                close_reason = None
                close_price  = price

                if side == "LONG":
                    if price <= sl:
                        close_reason = "Trail SL" if trail_active else "SL"
                    elif price >= open_pos["tp2"]:
                        close_reason = "TP2"
                    elif price >= open_pos["tp3"]:
                        close_reason = "TP3"
                    elif not open_pos.get("tp1_hit") and price >= open_pos["tp1"]:
                        open_pos["tp1_hit"] = True
                        open_pos["sl"]      = open_pos["entry"]  # move to BE
                else:
                    if price >= sl:
                        close_reason = "Trail SL" if trail_active else "SL"
                    elif price <= open_pos["tp2"]:
                        close_reason = "TP2"
                    elif price <= open_pos["tp3"]:
                        close_reason = "TP3"
                    elif not open_pos.get("tp1_hit") and price <= open_pos["tp1"]:
                        open_pos["tp1_hit"] = True
                        open_pos["sl"]      = open_pos["entry"]

                if close_reason:
                    pnl = (close_price - open_pos["entry"]) * open_pos["qty"] if side == "LONG" else \
                          (open_pos["entry"] - close_price) * open_pos["qty"]
                    result.balance += pnl
                    result.peak     = max(result.peak, result.balance)
                    self._trade_id += 1
                    result.trades.append(BTTrade(
                        id=self._trade_id, side=side,
                        entry=open_pos["entry"], exit=close_price,
                        qty=open_pos["qty"], pnl=round(pnl, 4),
                        reason=close_reason, duration_bars=bar_in,
                        trail_activated=trail_active,
                        date=str(row.name),
                    ))
                    open_pos = None; trail_active = False; bar_in = 0

            # Entry baru
            if open_pos is None:
                sig = self.ta.get_signal(window)
                if sig["signal"] in ("LONG", "SHORT"):
                    side   = sig["signal"]
                    levels = self.risk.calculate_levels(side, price, sig["atr"])
                    if self.risk.check_rr(levels):
                        qty = self.risk.position_size(result.balance, levels["sl_distance"])
                        if qty > 0 and qty * price < result.balance * 0.95:
                            open_pos = {
                                "side":  side,
                                "entry": price,
                                "qty":   qty,
                                "sl":    levels["stop_loss"],
                                "tp1":   levels["take_profit1"],
                                "tp2":   levels["take_profit2"],
                                "tp3":   levels["take_profit3"],
                            }

        # Tutup posisi yang masih terbuka
        if open_pos:
            last_price = df.iloc[-1]["close"]
            pnl = (last_price - open_pos["entry"]) * open_pos["qty"] if open_pos["side"] == "LONG" else \
                  (open_pos["entry"] - last_price) * open_pos["qty"]
            result.balance += pnl
            self._trade_id += 1
            result.trades.append(BTTrade(
                id=self._trade_id, side=open_pos["side"],
                entry=open_pos["entry"], exit=last_price,
                qty=open_pos["qty"], pnl=round(pnl, 4),
                reason="END", duration_bars=bar_in,
                date=str(df.iloc[-1].name),
            ))

        return result

    def run(self, symbol: str = "XAUTUSDT", tf: str = "1h", candles: int = 500) -> BTResult:
        print(f"\n{'═'*60}")
        print(f"  BACKTEST XAUT/USDT — {tf} | {candles} candle")
        print(f"{'═'*60}")
        df = self._fetch(symbol, tf, candles)
        result = self._simulate(df, self.start_bal)
        self._print_result(result, f"Full Backtest {tf}")
        self._save_json(result, "backtest_results.json")
        return result

    def walk_forward(self, symbol: str = "XAUTUSDT", tf: str = "1h",
                     candles: int = 1000, folds: int = 5) -> List[BTResult]:
        print(f"\n{'═'*60}")
        print(f"  WALK-FORWARD ANALYSIS — {folds} fold")
        print(f"{'═'*60}")
        df = self._fetch(symbol, tf, candles)
        fold_size = len(df) // folds
        results = []
        for i in range(folds):
            start = i * fold_size
            end   = start + fold_size if i < folds - 1 else len(df)
            fold_df = df.iloc[start:end]
            if len(fold_df) < 60:
                continue
            print(f"\n  Fold {i+1}/{folds} [{start}:{end}] ({len(fold_df)} candle)")
            r = self._simulate(fold_df.copy(), self.start_bal)
            self._print_result(r, f"Fold {i+1}")
            results.append(r)

        # Summary
        if results:
            avg_roi  = statistics.mean([r.roi for r in results])
            avg_wr   = statistics.mean([r.win_rate for r in results])
            avg_pf   = statistics.mean([r.profit_factor for r in results])
            avg_dd   = statistics.mean([r.max_drawdown for r in results])
            print(f"\n  {'─'*50}")
            print(f"  Walk-Forward Summary ({len(results)} fold):")
            print(f"  Avg ROI     : {avg_roi:+.2f}%")
            print(f"  Avg Win Rate: {avg_wr:.1f}%")
            print(f"  Avg PF      : {avg_pf:.3f}")
            print(f"  Avg Max DD  : {avg_dd:.2f}%")
            print(f"  {'─'*50}")

        return results

    def monte_carlo(self, result: BTResult, simulations: int = 1000) -> dict:
        """Simulasi Monte Carlo untuk estimasi distribusi hasil."""
        print(f"\n  Running Monte Carlo ({simulations} simulasi)...")
        trades = [t.pnl for t in result.trades]
        if not trades:
            print("  Tidak ada trade untuk simulasi")
            return {}

        final_balances = []
        max_drawdowns  = []

        for _ in range(simulations):
            shuffled = random.sample(trades, len(trades))
            bal  = self.start_bal
            peak = bal
            max_dd = 0
            for pnl in shuffled:
                bal  += pnl
                peak  = max(peak, bal)
                dd    = (peak - bal) / peak * 100 if peak > 0 else 0
                max_dd = max(max_dd, dd)
            final_balances.append(bal)
            max_drawdowns.append(max_dd)

        final_balances.sort()
        p5  = final_balances[int(simulations * 0.05)]
        p25 = final_balances[int(simulations * 0.25)]
        p50 = final_balances[int(simulations * 0.50)]
        p75 = final_balances[int(simulations * 0.75)]
        p95 = final_balances[int(simulations * 0.95)]
        prob_profit = sum(1 for b in final_balances if b > self.start_bal) / simulations * 100

        print(f"\n  Monte Carlo Results ({simulations} simulasi):")
        print(f"  Prob Profit : {prob_profit:.1f}%")
        print(f"  P5  Balance : ${p5:,.2f}  (worst case)")
        print(f"  P25 Balance : ${p25:,.2f}")
        print(f"  P50 Balance : ${p50:,.2f}  (median)")
        print(f"  P75 Balance : ${p75:,.2f}")
        print(f"  P95 Balance : ${p95:,.2f}  (best case)")
        print(f"  Avg Max DD  : {statistics.mean(max_drawdowns):.2f}%")

        return {
            "simulations": simulations,
            "prob_profit_pct": round(prob_profit, 2),
            "p5": round(p5, 2), "p25": round(p25, 2),
            "p50": round(p50, 2), "p75": round(p75, 2), "p95": round(p95, 2),
            "avg_max_drawdown_pct": round(statistics.mean(max_drawdowns), 2),
        }

    def _print_result(self, r: BTResult, label: str = ""):
        prefix = f"[{label}] " if label else ""
        print(f"\n  {prefix}{'─'*40}")
        print(f"  Total Trade      : {len(r.trades)}")
        print(f"  Menang / Kalah   : {len(r.wins)} / {len(r.losses)}")
        print(f"  Win Rate         : {r.win_rate:.1f}%")
        print(f"  Profit Factor    : {r.profit_factor}")
        print(f"  Expectancy       : ${r.expectancy:+.4f} / trade")
        print(f"  Sharpe Ratio     : {r.sharpe}")
        print(f"  Total PnL        : ${r.total_pnl:+,.4f}")
        print(f"  ROI              : {r.roi:+.2f}%")
        print(f"  Balance Akhir    : ${r.balance:,.2f}")
        print(f"  Max Drawdown     : {r.max_drawdown:.2f}%")
        print(f"  Max Consec Loss  : {r.max_consec_losses}")
        print(f"  Avg Win          : ${r.avg_win:+.4f}")
        print(f"  Avg Loss         : ${r.avg_loss:+.4f}")

    def _save_json(self, r: BTResult, filename: str):
        data = {
            "generated_at": datetime.now().isoformat(),
            "summary": {
                "total_trades":      len(r.trades),
                "wins":              len(r.wins),
                "losses":            len(r.losses),
                "win_rate_pct":      round(r.win_rate, 2),
                "profit_factor":     r.profit_factor,
                "expectancy":        round(r.expectancy, 4),
                "sharpe":            r.sharpe,
                "total_pnl":         round(r.total_pnl, 4),
                "roi_pct":           round(r.roi, 2),
                "final_balance":     round(r.balance, 2),
                "max_drawdown_pct":  round(r.max_drawdown, 2),
                "max_consec_losses": r.max_consec_losses,
            },
            "trades": [
                {
                    "id": t.id, "side": t.side,
                    "entry": t.entry, "exit": t.exit,
                    "qty": t.qty, "pnl": t.pnl,
                    "reason": t.reason,
                    "duration_bars": t.duration_bars,
                    "trail_activated": t.trail_activated,
                    "date": t.date,
                }
                for t in r.trades
            ],
        }
        with open(filename, "w") as f:
            json.dump(data, f, indent=2)
        print(f"\n  ✅ Hasil disimpan ke {filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="XAUT/USDT Backtester v2.0")
    parser.add_argument("--tf",      default="1h",    help="Timeframe (1m/5m/15m/1h/4h)")
    parser.add_argument("--candles", type=int, default=500, help="Jumlah candle")
    parser.add_argument("--bal",     type=float, default=10_000, help="Balance awal")
    parser.add_argument("--wf",      action="store_true", help="Walk-forward analysis")
    parser.add_argument("--folds",   type=int, default=5, help="Jumlah fold walk-forward")
    parser.add_argument("--mc",      type=int, default=0, help="Jumlah simulasi Monte Carlo")
    args = parser.parse_args()

    bt = Backtester(initial_balance=args.bal)

    if args.wf:
        results = bt.walk_forward("XAUTUSDT", args.tf, args.candles, args.folds)
    else:
        result = bt.run("XAUTUSDT", args.tf, args.candles)
        if args.mc > 0:
            mc_result = bt.monte_carlo(result, args.mc)
