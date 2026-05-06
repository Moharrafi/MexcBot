"""
MEXC Listing Sniper — Spot + Dashboard
Auto-detect pre-market price dari order book, lalu beli + pasang TP otomatis.

Cara pakai:
1. Isi API_KEY, API_SECRET
2. Set SYMBOL, LISTING_TIME, USDT_AMOUNT, TP targets
3. Jalankan: python listing_sniper.py
4. Buka browser: http://localhost:5050
"""

import hmac
import hashlib
import time
import requests
import urllib3
import ssl
import json
import threading
from datetime import datetime
from flask import Flask, jsonify
from requests.adapters import HTTPAdapter

urllib3.disable_warnings()

class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *a, **kw):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kw["ssl_context"] = ctx
        super().init_poolmanager(*a, **kw)

_session = requests.Session()
_session.mount("https://", TLSAdapter())
import pytz

# ══════════════════════════════════════════════
#  KONFIGURASI — Edit bagian ini
# ══════════════════════════════════════════════

API_KEY    = "mx0vgljxzGaPdRDewp"
API_SECRET = "ce2d5d8e47ab4a5eaed6f00cd436218b"

SYMBOL       = "XNTUSDT"               # Tanpa underscore, tanpa slash
LISTING_TIME = "2026-04-22 19:00:00"   # Waktu listing (WIB / UTC+7)
USDT_AMOUNT  = 5.0                     # Modal beli dalam USDT

LIMIT_BUFFER_PCT = 0.30

TP_LEVELS = [
    (0.5,  0.30),   # TP1: +50%  jual 30% (aman)
    (2.0,  0.30),   # TP2: +200% jual 30%
    (5.0,  0.40),   # TP3: +500% jual 40% sisa
]

DASHBOARD_PORT = 5002
BASE_URL = "https://api.mexc.com"
WIB      = pytz.timezone("Asia/Jakarta")

# ══════════════════════════════════════════════
#  STATE (shared antara sniper thread & dashboard)
# ══════════════════════════════════════════════

state = {
    "phase":           "STANDBY",       # STANDBY / MONITORING / ORDERING / WAITING_FILL / TP_SET / DONE / ERROR
    "symbol":          SYMBOL,
    "listing_time":    LISTING_TIME,
    "usdt_amount":     USDT_AMOUNT,
    "detected_price":  None,
    "buy_price":       None,
    "order_id":        None,
    "order_status":    None,
    "filled_qty":      0.0,
    "filled_price":    None,
    "tp_orders":       [],              # [{level, price, qty, order_id, pct}]
    "log":             [],              # list of log strings
    "error":           None,
    "countdown_sec":   None,
}

def log(msg: str):
    ts = datetime.now(WIB).strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    print(entry)
    state["log"].append(entry)
    if len(state["log"]) > 100:
        state["log"].pop(0)

# ══════════════════════════════════════════════
#  HELPER FUNCTIONS
# ══════════════════════════════════════════════

def sign(params: dict) -> str:
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()

def get_headers():
    return {"X-MEXC-APIKEY": API_KEY}

def get_orderbook():
    try:
        r = _session.get(f"{BASE_URL}/api/v3/depth",
            params={"symbol": SYMBOL, "limit": 5}, timeout=5, verify=False)
        return r.json()
    except Exception as e:
        log(f"[!] Orderbook error: {e}")
        return None

def get_indicative_price():
    book = get_orderbook()
    if not book:
        return None
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    if bids and asks:
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        mid = (best_bid + best_ask) / 2
        log(f"Order book: bid={best_bid}, ask={best_ask}, mid={mid:.6f}")
        return mid
    elif asks:
        p = float(asks[0][0])
        log(f"Order book (ask only): {p:.6f}")
        return p
    elif bids:
        p = float(bids[0][0])
        log(f"Order book (bid only): {p:.6f}")
        return p
    return None

def get_live_price():
    try:
        r = _session.get(f"{BASE_URL}/api/v3/ticker/price",
            params={"symbol": SYMBOL}, timeout=5, verify=False)
        d = r.json()
        p = float(d.get("price", 0))
        return p if p > 0 else None
    except:
        return None

def place_limit_order(side: str, price: float, quantity: float) -> dict:
    ts = str(int(time.time() * 1000))
    params = {
        "symbol":    SYMBOL,
        "side":      side,
        "type":      "LIMIT",
        "price":     f"{price:.8f}".rstrip("0").rstrip("."),
        "quantity":  f"{quantity:.4f}".rstrip("0").rstrip("."),
        "timestamp": ts,
    }
    params["signature"] = sign(params)
    r = _session.post(f"{BASE_URL}/api/v3/order",
        params=params, headers=get_headers(), timeout=10, verify=False)
    return r.json()

def get_order_status(order_id: str):
    ts = str(int(time.time() * 1000))
    params = {"symbol": SYMBOL, "orderId": order_id, "timestamp": ts}
    params["signature"] = sign(params)
    try:
        r = _session.get(f"{BASE_URL}/api/v3/order",
            params=params, headers=get_headers(), timeout=10, verify=False)
        return r.json()
    except:
        return None

def get_symbol_info():
    try:
        r = _session.get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=10, verify=False)
        for s in r.json().get("symbols", []):
            if s["symbol"] == SYMBOL:
                return s
    except:
        pass
    return None

def round_price(price: float, tick_size: float) -> float:
    if tick_size <= 0:
        return price
    import math
    decimals = max(0, -int(math.floor(math.log10(tick_size))))
    return round(round(price / tick_size) * tick_size, decimals)

def round_qty(qty: float, step_size: float) -> float:
    if step_size <= 0:
        return qty
    import math
    decimals = max(0, -int(math.floor(math.log10(step_size))))
    return round(int(qty / step_size) * step_size, decimals)

# ══════════════════════════════════════════════
#  SNIPER LOGIC (berjalan di thread terpisah)
# ══════════════════════════════════════════════

def sniper_thread():
    listing_dt = WIB.localize(datetime.strptime(LISTING_TIME, "%Y-%m-%d %H:%M:%S"))
    listing_ts = listing_dt.timestamp()

    log(f"=== MEXC Listing Sniper ===")
    log(f"Symbol  : {SYMBOL}")
    log(f"Listing : {LISTING_TIME} WIB")
    log(f"Modal   : {USDT_AMOUNT} USDT")
    log(f"Buffer  : +{LIMIT_BUFFER_PCT*100:.0f}%")
    log(f"Dashboard: http://localhost:{DASHBOARD_PORT}")

    # ── 1. Standby sampai T-5 menit ──────────────────────────
    monitor_start = listing_ts - 5 * 60
    now = time.time()
    if now < monitor_start:
        log(f"Standby... mulai monitor T-5 menit sebelum listing ({int(monitor_start - now)}s lagi)")
        state["phase"] = "STANDBY"
        while time.time() < monitor_start:
            state["countdown_sec"] = int(listing_ts - time.time())
            time.sleep(1)

    # ── 2. Monitor order book T-5 menit sampai T-5 detik ────
    ORDER_EARLY_SEC = 5
    state["phase"] = "MONITORING"
    log(f"Memantau order book {SYMBOL}...")

    while time.time() < (listing_ts - ORDER_EARLY_SEC):
        state["countdown_sec"] = int(listing_ts - time.time())
        price = get_indicative_price()
        if price and price > 0:
            state["detected_price"] = price
            log(f"Pre-market terdeteksi: ${price:.6f}")
        else:
            log(f"Order book belum ada data...")
        remaining = listing_ts - time.time()
        time.sleep(3 if remaining > 30 else 1)

    state["countdown_sec"] = ORDER_EARLY_SEC
    log(f"T-{ORDER_EARLY_SEC}s — siap kirim order!")

    # ── 3. Tentukan harga beli ────────────────────────────────
    detected = state["detected_price"]
    if detected and detected > 0:
        buy_price = detected * (1 + LIMIT_BUFFER_PCT)
        log(f"Pre-market: ${detected:.6f} | Limit BUY: ${buy_price:.6f} (+{LIMIT_BUFFER_PCT*100:.0f}%)")
    else:
        log("GAGAL: pre-market price tidak terdeteksi!")
        state["phase"] = "ERROR"
        state["error"] = "Pre-market price tidak terdeteksi dari order book"
        return

    # ── 4. Precision info ─────────────────────────────────────
    tick_size = step_size = 0.0
    sym_info = get_symbol_info()
    if sym_info:
        for f in sym_info.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                tick_size = float(f.get("tickSize", 0))
            if f.get("filterType") == "LOT_SIZE":
                step_size = float(f.get("stepSize", 0))
        log(f"Precision: tick={tick_size}, step={step_size}")

    if tick_size > 0:
        buy_price = round_price(buy_price, tick_size)

    quantity = USDT_AMOUNT / buy_price
    if step_size > 0:
        quantity = round_qty(quantity, step_size)

    state["buy_price"] = buy_price
    log(f"Order: BUY {quantity} {SYMBOL.replace('USDT','')} @ ${buy_price:.6f} (~{quantity*buy_price:.2f} USDT)")

    # ── 5. Kirim order ────────────────────────────────────────
    state["phase"] = "ORDERING"
    log(f"KIRIM ORDER BUY!")
    result = place_limit_order("BUY", buy_price, quantity)
    log(f"Response: {json.dumps(result)}")

    order_id = result.get("orderId")
    if not order_id:
        log(f"ORDER GAGAL!")
        state["phase"] = "ERROR"
        state["error"] = f"Order gagal: {result}"
        return

    state["order_id"] = str(order_id)
    log(f"Order ID: {order_id}")

    # ── 6. Tunggu fill ────────────────────────────────────────
    state["phase"] = "WAITING_FILL"
    filled_qty = 0.0
    filled_price = buy_price
    timeout = time.time() + 120

    while time.time() < timeout:
        status = get_order_status(str(order_id))
        if not status:
            time.sleep(2)
            continue

        s = status.get("status", "")
        filled_qty = float(status.get("executedQty", 0))
        avg_price = float(status.get("price", buy_price))
        if float(status.get("cummulativeQuoteQty", 0)) > 0 and filled_qty > 0:
            avg_price = float(status.get("cummulativeQuoteQty", 0)) / filled_qty

        state["order_status"] = s
        state["filled_qty"] = filled_qty
        state["filled_price"] = avg_price
        log(f"Status: {s} | Filled: {filled_qty} @ ${avg_price:.6f}")

        if s == "FILLED":
            filled_price = avg_price
            log(f"ORDER TERISI PENUH! {filled_qty} @ ${filled_price:.6f}")
            break
        elif s == "PARTIALLY_FILLED":
            filled_price = avg_price
        elif s in ("CANCELED", "REJECTED", "EXPIRED"):
            log(f"Order {s}")
            state["phase"] = "ERROR"
            state["error"] = f"Order {s}"
            return

        time.sleep(2)
    else:
        log(f"Timeout fill, lanjut dengan qty terisi: {filled_qty}")

    if filled_qty <= 0:
        log("Tidak ada qty terisi!")
        state["phase"] = "ERROR"
        state["error"] = "Tidak ada qty yang terisi"
        return

    # ── 7. Pasang TP ──────────────────────────────────────────
    state["phase"] = "TP_SET"
    log(f"Pasang {len(TP_LEVELS)} TP sell...")
    remaining_qty = filled_qty

    for i, (profit_mult, pct_sell) in enumerate(TP_LEVELS, 1):
        tp_price = filled_price * (1 + profit_mult)
        sell_qty = filled_qty * pct_sell
        if i == len(TP_LEVELS):
            sell_qty = remaining_qty

        if tick_size > 0:
            tp_price = round_price(tp_price, tick_size)
        if step_size > 0:
            sell_qty = round_qty(sell_qty, step_size)
        if sell_qty <= 0:
            continue

        remaining_qty -= sell_qty
        tp_result = place_limit_order("SELL", tp_price, sell_qty)
        tp_oid = tp_result.get("orderId", "ERROR")
        state["tp_orders"].append({
            "level":    i,
            "pct":      int(profit_mult * 100),
            "price":    tp_price,
            "qty":      sell_qty,
            "order_id": str(tp_oid),
        })
        log(f"TP{i}: SELL {sell_qty} @ ${tp_price:.6f} (+{profit_mult*100:.0f}%) id={tp_oid}")
        time.sleep(0.3)

    state["phase"] = "DONE"
    log(f"SELESAI! Entry ${filled_price:.6f}")
    for i, (mult, pct) in enumerate(TP_LEVELS, 1):
        log(f"  TP{i}: ${filled_price*(1+mult):.6f} (+{mult*100:.0f}%)")

# ══════════════════════════════════════════════
#  DASHBOARD
# ══════════════════════════════════════════════

app = Flask(__name__)

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Listing Sniper — {symbol}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0d1117; color: #e6edf3; font-family: 'Segoe UI', sans-serif; padding: 16px; }}
  h1 {{ font-size: 1.3rem; color: #58a6ff; margin-bottom: 4px; }}
  .sub {{ color: #8b949e; font-size: 0.85rem; margin-bottom: 16px; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; }}
  .card {{ background: #161b22; border-radius: 10px; padding: 14px; border: 1px solid #30363d; }}
  .card label {{ font-size: 0.7rem; color: #8b949e; text-transform: uppercase; letter-spacing: 1px; }}
  .card .val {{ font-size: 1.5rem; font-weight: 700; margin-top: 4px; }}
  .phase-STANDBY   {{ color: #8b949e; }}
  .phase-MONITORING{{ color: #e3b341; }}
  .phase-ORDERING  {{ color: #58a6ff; }}
  .phase-WAITING_FILL {{ color: #58a6ff; }}
  .phase-TP_SET    {{ color: #3fb950; }}
  .phase-DONE      {{ color: #3fb950; }}
  .phase-ERROR     {{ color: #f85149; }}
  .green {{ color: #3fb950; }}
  .yellow {{ color: #e3b341; }}
  .blue  {{ color: #58a6ff; }}
  .red   {{ color: #f85149; }}
  .tp-table {{ width: 100%; border-collapse: collapse; margin-top: 8px; }}
  .tp-table th, .tp-table td {{ padding: 8px 10px; text-align: left; border-bottom: 1px solid #21262d; font-size: 0.85rem; }}
  .tp-table th {{ color: #8b949e; font-size: 0.72rem; text-transform: uppercase; }}
  .log-box {{ background: #0d1117; border: 1px solid #21262d; border-radius: 8px; padding: 10px;
              font-family: monospace; font-size: 0.75rem; color: #8b949e; height: 180px;
              overflow-y: auto; margin-top: 4px; }}
  .log-box .entry {{ padding: 1px 0; border-bottom: 1px solid #161b22; }}
  .countdown {{ font-size: 2.2rem; font-weight: 800; color: #e3b341; }}
  .pnl-pos {{ color: #3fb950; }}
  .pnl-neg {{ color: #f85149; }}
  @media(max-width:480px) {{ .grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>
<h1>🚀 Listing Sniper</h1>
<div class="sub" id="subtitle">Loading...</div>

<div class="grid">
  <div class="card">
    <label>Status</label>
    <div class="val" id="phase">—</div>
  </div>
  <div class="card">
    <label>Countdown</label>
    <div class="countdown" id="countdown">—</div>
  </div>
</div>

<div class="grid">
  <div class="card">
    <label>Pre-market Price</label>
    <div class="val blue" id="premarket">—</div>
  </div>
  <div class="card">
    <label>Limit Buy Price</label>
    <div class="val yellow" id="buyprice">—</div>
  </div>
</div>

<div class="grid">
  <div class="card">
    <label>Live Price</label>
    <div class="val" id="liveprice">—</div>
  </div>
  <div class="card">
    <label>Live PnL</label>
    <div class="val" id="livepnl">—</div>
  </div>
</div>

<div class="grid">
  <div class="card">
    <label>Order Status</label>
    <div class="val" id="orderstatus">—</div>
    <div style="font-size:0.75rem;color:#8b949e;margin-top:4px" id="orderid">—</div>
  </div>
  <div class="card">
    <label>Filled</label>
    <div class="val green" id="filled">—</div>
  </div>
</div>

<div class="card" style="margin-bottom:12px">
  <label>Take Profit Orders</label>
  <table class="tp-table">
    <thead><tr><th>#</th><th>Target</th><th>Harga</th><th>Qty</th><th>Order ID</th></tr></thead>
    <tbody id="tp-body"><tr><td colspan="5" style="color:#8b949e">Belum ada TP</td></tr></tbody>
  </table>
</div>

<div class="card">
  <label>Log</label>
  <div class="log-box" id="logbox"></div>
</div>

<script>
const PHASES = {
  STANDBY:'⏳ Standby', MONITORING:'🔍 Monitoring', ORDERING:'📤 Kirim Order',
  WAITING_FILL:'⌛ Tunggu Fill', TP_SET:'🎯 TP Terpasang', DONE:'✅ Selesai', ERROR:'❌ Error'
};

function fmt(v, dec=6) { return v != null ? '$' + parseFloat(v).toFixed(dec) : '—'; }
function fmtSec(s) {
  if (s == null) return '—';
  const h = Math.floor(s/3600), m = Math.floor((s%3600)/60), sec = s%60;
  if (h > 0) return h+'h '+m+'m '+sec+'s';
  if (m > 0) return m+'m '+sec+'s';
  return sec+'s';
}

async function refresh() {
  try {
    const d = await fetch('/api/state').then(r => r.json());

    document.getElementById('subtitle').textContent =
      d.symbol + ' — Listing: ' + d.listing_time + ' WIB | Modal: $' + d.usdt_amount;

    const ph = d.phase;
    const phEl = document.getElementById('phase');
    phEl.textContent = PHASES[ph] || ph;
    phEl.className = 'val phase-' + ph;

    document.getElementById('countdown').textContent =
      d.countdown_sec != null ? fmtSec(d.countdown_sec) : (ph === 'DONE' ? 'Listed!' : '—');

    document.getElementById('premarket').textContent =
      d.detected_price ? fmt(d.detected_price) : '⏳ Deteksi...';
    document.getElementById('buyprice').textContent =
      d.buy_price ? fmt(d.buy_price) : '—';

    // Live price & PnL
    const lp = d.live_price;
    const fp = d.filled_price;
    const lpEl = document.getElementById('liveprice');
    const pnlEl = document.getElementById('livepnl');
    if (lp) {
      lpEl.textContent = fmt(lp);
      if (fp && d.filled_qty > 0) {
        const pnlPct = (lp - fp) / fp * 100;
        const pnlUsd = (lp - fp) * d.filled_qty;
        pnlEl.textContent = (pnlPct >= 0 ? '+' : '') + pnlPct.toFixed(2) + '% (' +
          (pnlUsd >= 0 ? '+$' : '-$') + Math.abs(pnlUsd).toFixed(4) + ')';
        pnlEl.className = 'val ' + (pnlPct >= 0 ? 'pnl-pos' : 'pnl-neg');
      } else { pnlEl.textContent = '—'; pnlEl.className = 'val'; }
    } else { lpEl.textContent = '—'; pnlEl.textContent = '—'; }

    const os = d.order_status || (d.order_id ? 'PENDING' : '—');
    document.getElementById('orderstatus').textContent = os;
    document.getElementById('orderid').textContent = d.order_id ? 'ID: ' + d.order_id : '—';

    document.getElementById('filled').textContent =
      d.filled_qty > 0
        ? d.filled_qty + ' @ ' + fmt(d.filled_price)
        : '—';

    const tbody = document.getElementById('tp-body');
    if (d.tp_orders && d.tp_orders.length > 0) {
      tbody.innerHTML = d.tp_orders.map(tp =>
        '<tr><td>TP' + tp.level + '</td><td class="green">+' + tp.pct + '%</td>' +
        '<td>' + fmt(tp.price) + '</td><td>' + tp.qty + '</td>' +
        '<td style="font-size:0.7rem;color:#8b949e">' + tp.order_id + '</td></tr>'
      ).join('');
    }

    const lb = document.getElementById('logbox');
    lb.innerHTML = (d.log || []).slice().reverse()
      .map(e => '<div class="entry">' + e + '</div>').join('');

    if (d.error) {
      document.getElementById('phase').textContent = '❌ ' + d.error;
    }
  } catch(e) { console.error(e); }
}

refresh();
setInterval(refresh, 1500);
</script>
</body>
</html>""".replace("{symbol}", SYMBOL)

@app.route("/")
def index():
    return DASHBOARD_HTML

@app.route("/api/state")
def api_state():
    d = dict(state)
    # Tambah live price untuk PnL
    if state["phase"] in ("WAITING_FILL", "TP_SET", "DONE") and state["filled_qty"] > 0:
        lp = get_live_price()
        d["live_price"] = lp
    else:
        d["live_price"] = None
    return jsonify(d)

# ══════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════

if __name__ == "__main__":
    t = threading.Thread(target=sniper_thread, daemon=True)
    t.start()

    print(f"\n{'='*50}")
    print(f"  Dashboard: http://localhost:{DASHBOARD_PORT}")
    print(f"  Symbol   : {SYMBOL}")
    print(f"  Listing  : {LISTING_TIME} WIB")
    print(f"{'='*50}\n")

    app.run(host="0.0.0.0", port=DASHBOARD_PORT, debug=False, use_reloader=False)
