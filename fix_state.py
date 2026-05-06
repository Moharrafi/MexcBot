"""
Reset bot state to match actual MEXC balance.
PnL history was corrupted by wrong price sync bug.
"""
import json, os

state_file = "bot_state.json"
if not os.path.exists(state_file):
    print("State file not found!")
    exit(1)

with open(state_file, "r") as f:
    data = json.load(f)

print(f"=== BEFORE RESET ===")
print(f"  Balance:     ${data.get('balance', 0):.4f}")
print(f"  Total PnL:   ${data.get('total_pnl', 0):.4f}")
print(f"  Daily PnL:   ${data.get('daily_pnl', 0):.4f}")
print(f"  Peak:        ${data.get('peak_balance', 0):.4f}")
print(f"  Total Trades: {data.get('total_trades', 0)}")
print(f"  Winning:     {data.get('winning_trades', 0)}")
print(f"  Circuit:     {data.get('circuit_breaker', False)}")

# Reset semua counter PnL ke 0 (mulai bersih)
# Balance akan di-sync dari API MEXC saat bot start
data["total_pnl"] = 0.0
data["daily_pnl"] = 0.0
data["peak_balance"] = 0.0  # Will be re-synced
data["balance"] = 0.0       # Will be re-synced from MEXC API
data["circuit_breaker"] = False
data["circuit_reason"] = ""
data["circuit_type"] = ""
data["total_trades"] = 0
data["winning_trades"] = 0

# Hapus semua posisi lama yang sudah corrupt
data["positions"] = []

with open(state_file, "w") as f:
    json.dump(data, f, indent=4)

print(f"\n=== AFTER RESET ===")
print(f"  Balance:     ${data['balance']:.4f} (akan di-sync dari MEXC)")
print(f"  Total PnL:   ${data['total_pnl']:.4f}")
print(f"  Positions:   {len(data['positions'])} (bersih)")
print(f"  Circuit:     {data['circuit_breaker']}")
print(f"\n✅ State berhasil di-reset. Bot akan sync saldo dari MEXC saat start.")
