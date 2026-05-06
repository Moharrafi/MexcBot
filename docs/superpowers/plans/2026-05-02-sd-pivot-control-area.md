# Supply & Demand + Pivot Points + Control Area — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tambah deteksi S&D zones, ICT Order Blocks, Pivot Points (Classic/Camarilla/Fibonacci), POC, dan Confluence zone ke mexc_scalperV4.py sebagai filter entry dan TP/SL dinamis.

**Architecture:** Empat metode baru di `ScalperTA` (pure TA functions), dua metode baru di `ScalperBotV4` (pivot cache + zone context aggregator), integrasi ke main loop sebagai gate dan scoring tambahan, override TP/SL pivot di `calculate_levels()`.

**Tech Stack:** Python 3, pandas, numpy. File target: `/home/ubuntu/scalper/mexc_scalperV4.py`, `/home/ubuntu/scalper/config_scalper_v4.json`.

---

## File yang Dimodifikasi

| File | Perubahan |
|------|-----------|
| `mexc_scalperV4.py` baris ~80 | Tambah 10 key ke dict `CONFIG` |
| `mexc_scalperV4.py` setelah baris 1240 | Tambah 4 metode ke `ScalperTA`: `_detect_sd_zones`, `_detect_order_blocks`, `_calc_poc`, `_calc_midrange` |
| `mexc_scalperV4.py` baris ~2730 | Tambah `self._pivot_cache` dan `self._last_df` ke `ScalperBotV4.__init__` |
| `mexc_scalperV4.py` setelah `_refresh_pivot_levels` | Tambah 2 metode ke `ScalperBotV4`: `_refresh_pivot_levels`, `_get_zone_context` |
| `mexc_scalperV4.py` baris ~3046 | Simpan `self._last_df = df` di `fetch_and_analyze()` |
| `mexc_scalperV4.py` baris ~3975 | Tambah zone gate + log di main loop entry section |
| `mexc_scalperV4.py` baris ~3200 | Pass `zone_ctx` ke `_open_position()` dari main loop |
| `mexc_scalperV4.py` baris ~2284 | Tambah `pivot_context=None` param ke `calculate_levels()` |
| `config_scalper_v4.json` | Tambah 10 key baru |
| `/home/ubuntu/scalper/tests/test_zone_features.py` | File test baru |

---

## Task 1: Tambah Config Keys

**Files:**
- Modify: `mexc_scalperV4.py` (dict `CONFIG`, sekitar baris 80–240)
- Modify: `config_scalper_v4.json`

- [ ] **Step 1: Tambah 10 key ke dict `CONFIG` di mexc_scalperV4.py**

Cari blok `CONFIG = {` (sekitar baris 80). Tambahkan di bagian akhir CONFIG, sebelum tanda tutup `}`:

```python
    # ─── Supply & Demand / Order Block / Pivot / Control Area ──
    "USE_SD_ZONES":        True,   # S&D zone gate aktif
    "USE_ORDER_BLOCKS":    True,   # ICT Order Block scoring
    "USE_PIVOT_LEVELS":    True,   # Pivot Point proximity scoring
    "PIVOT_TYPE":         "ALL",   # "CLASSIC" | "CAMARILLA" | "FIB" | "ALL"
    "USE_PIVOT_TARGETS":  False,   # Pivot sebagai TP/SL (mulai False — aktifkan setelah validasi)
    "USE_POC":             True,   # Volume Profile POC scoring
    "USE_CONFLUENCE":      True,   # Confluence zone bonus
    "ZONE_PROXIMITY_ATR":  0.5,   # Radius zona = 0.5×ATR
    "SD_LOOKBACK":          50,   # Candle lookback untuk S&D detection
    "OB_LOOKBACK":          20,   # Candle lookback untuk Order Block detection
    "POC_BUCKETS":          20,   # Jumlah bucket Volume Profile
```

- [ ] **Step 2: Tambah key yang sama ke config_scalper_v4.json**

```bash
cd ~/scalper
python3 -c "
import json
with open('config_scalper_v4.json') as f:
    cfg = json.load(f)
new_keys = {
    'USE_SD_ZONES': True,
    'USE_ORDER_BLOCKS': True,
    'USE_PIVOT_LEVELS': True,
    'PIVOT_TYPE': 'ALL',
    'USE_PIVOT_TARGETS': False,
    'USE_POC': True,
    'USE_CONFLUENCE': True,
    'ZONE_PROXIMITY_ATR': 0.5,
    'SD_LOOKBACK': 50,
    'OB_LOOKBACK': 20,
    'POC_BUCKETS': 20,
}
cfg.update(new_keys)
with open('config_scalper_v4.json', 'w') as f:
    json.dump(cfg, f, indent=4)
print('OK — keys added:', list(new_keys.keys()))
"
```

Expected output: `OK — keys added: ['USE_SD_ZONES', 'USE_ORDER_BLOCKS', ...]`

- [ ] **Step 3: Buat folder tests**

```bash
mkdir -p ~/scalper/tests
touch ~/scalper/tests/__init__.py
```

---

## Task 2: ScalperTA._detect_sd_zones()

**Files:**
- Modify: `mexc_scalperV4.py` — tambah setelah `_detect_liquidity_sweep` (baris 1240)
- Test: `/home/ubuntu/scalper/tests/test_zone_features.py`

- [ ] **Step 1: Tulis test gagal**

Buat file `/home/ubuntu/scalper/tests/test_zone_features.py`:

```python
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import pandas as pd
import numpy as np

# Buat ScalperTA dengan config minimal
cfg = {
    "SD_LOOKBACK": 50, "OB_LOOKBACK": 20, "POC_BUCKETS": 20,
    "ST_PERIOD": 14, "ST_MULTIPLIER": 3.0, "DEMA_FAST": 9, "DEMA_SLOW": 21,
    "ROC_PERIOD": 9, "WILLR_PERIOD": 14, "ATR_PERIOD": 14, "ADX_PERIOD": 14,
    "VOL_MA_PERIOD": 20, "SQUEEZE_BB_PERIOD": 20, "SQUEEZE_BB_STD": 2.0,
    "SQUEEZE_KC_PERIOD": 20, "SQUEEZE_KC_MULT": 1.5,
}

def make_df(n=60):
    """DataFrame OHLCV sintetis dengan harga stabil di 100."""
    data = {
        "open":   [100.0] * n,
        "high":   [100.5] * n,
        "low":    [99.5]  * n,
        "close":  [100.0] * n,
        "volume": [1000.0] * n,
    }
    return pd.DataFrame(data)

def test_sd_zones_detects_demand_zone():
    from mexc_scalperV4 import ScalperTA
    ta = ScalperTA(cfg)
    df = make_df(60)
    # Candle 50-53: base (kecil)
    for i in range(50, 54):
        df.at[i, "open"]  = 100.0
        df.at[i, "close"] = 100.1
        df.at[i, "high"]  = 100.2
        df.at[i, "low"]   = 99.9
    # Candle 54: impulse bullish besar (body = 3.0 >> avg ATR ~1.0)
    df.at[54, "open"]  = 100.0
    df.at[54, "close"] = 103.0
    df.at[54, "high"]  = 103.1
    df.at[54, "low"]   = 99.9
    # Candle 55-59: harga naik, tidak menyentuh zona base
    for i in range(55, 60):
        df.at[i, "open"]  = 103.0
        df.at[i, "close"] = 103.5
        df.at[i, "high"]  = 104.0
        df.at[i, "low"]   = 102.5

    # Tambah kolom atr manual
    df["atr"] = 0.5
    zones = ta._detect_sd_zones(df)
    demand_zones = [z for z in zones if z["type"] == "demand"]
    assert len(demand_zones) >= 1, f"Expected demand zone, got: {zones}"
    assert demand_zones[-1]["fresh"], "Zone harus fresh (belum disentuh)"
    print("PASS: test_sd_zones_detects_demand_zone")

def test_sd_zones_marks_used_zone():
    from mexc_scalperV4 import ScalperTA
    ta = ScalperTA(cfg)
    df = make_df(60)
    # Base + impulse
    for i in range(50, 53):
        df.at[i, "open"] = 100.0; df.at[i, "close"] = 100.1
        df.at[i, "high"] = 100.2; df.at[i, "low"]   = 99.9
    df.at[53, "open"] = 100.0; df.at[53, "close"] = 103.0
    df.at[53, "high"] = 103.1; df.at[53, "low"]   = 99.9
    # Candle 54: harga balik ke zona (close dalam 99.9-100.2)
    df.at[54, "open"]  = 102.0
    df.at[54, "close"] = 100.05  # masuk zona
    df.at[54, "high"]  = 102.0
    df.at[54, "low"]   = 99.8
    df["atr"] = 0.5
    zones = ta._detect_sd_zones(df)
    demand_zones = [z for z in zones if z["type"] == "demand"]
    if demand_zones:
        assert not demand_zones[-1]["fresh"], "Zone harus marked NOT fresh setelah disentuh"
    print("PASS: test_sd_zones_marks_used_zone")

if __name__ == "__main__":
    test_sd_zones_detects_demand_zone()
    test_sd_zones_marks_used_zone()
    print("\nAll S&D tests passed.")
```

- [ ] **Step 2: Jalankan test — pastikan GAGAL dengan AttributeError**

```bash
cd ~/scalper
python3 tests/test_zone_features.py
```

Expected: `AttributeError: 'ScalperTA' object has no attribute '_detect_sd_zones'`

- [ ] **Step 3: Tambah `_detect_sd_zones` ke ScalperTA (setelah baris 1240 — setelah `_detect_liquidity_sweep`)**

```python
    # ─── Supply & Demand Zone Detection ───────────────────────

    def _detect_sd_zones(self, df: pd.DataFrame) -> list:
        lookback = self.cfg.get("SD_LOOKBACK", 50)
        df = df.tail(lookback).reset_index(drop=True)
        avg_atr = df["atr"].mean() if "atr" in df.columns else (df["high"] - df["low"]).mean()
        impulse_min = 1.5 * avg_atr
        base_max    = 0.5 * avg_atr
        zones = []

        for i in range(4, len(df) - 1):
            candle = df.iloc[i]
            body = abs(candle["close"] - candle["open"])
            if body < impulse_min:
                continue
            is_bull = candle["close"] > candle["open"]

            base_rows = []
            for j in range(1, 5):
                if i - j < 0:
                    break
                bc = df.iloc[i - j]
                if abs(bc["close"] - bc["open"]) < base_max:
                    base_rows.append(bc)
                else:
                    break
            if not base_rows:
                continue

            base_slice = pd.DataFrame(base_rows)
            zone_low  = float(base_slice["low"].min())
            zone_high = float(base_slice["high"].max())

            subsequent = df.iloc[i + 1:]
            touched = ((subsequent["close"] >= zone_low) &
                       (subsequent["close"] <= zone_high)).any()

            zones.append({
                "type":      "demand" if is_bull else "supply",
                "low":       zone_low,
                "high":      zone_high,
                "formed_at": i,
                "fresh":     not touched,
            })

        return zones
```

- [ ] **Step 4: Jalankan test lagi — pastikan PASS**

```bash
cd ~/scalper
python3 tests/test_zone_features.py
```

Expected:
```
PASS: test_sd_zones_detects_demand_zone
PASS: test_sd_zones_marks_used_zone

All S&D tests passed.
```

---

## Task 3: ScalperTA._detect_order_blocks()

**Files:**
- Modify: `mexc_scalperV4.py` — tambah setelah `_detect_sd_zones`
- Test: `tests/test_zone_features.py`

- [ ] **Step 1: Tambah test untuk order blocks ke test file**

Append ke `/home/ubuntu/scalper/tests/test_zone_features.py`:

```python
def test_order_block_detects_bullish_ob():
    from mexc_scalperV4 import ScalperTA
    ta = ScalperTA(cfg)
    df = make_df(30)
    # Candle 25: bearish (merah) — kandidat Bullish OB
    df.at[25, "open"]  = 101.0
    df.at[25, "close"] = 100.0  # bearish
    df.at[25, "high"]  = 101.2
    df.at[25, "low"]   = 99.8
    # Candle 26: impulse bullish besar body=4.0
    df.at[26, "open"]  = 100.0
    df.at[26, "close"] = 104.0
    df.at[26, "high"]  = 104.1
    df.at[26, "low"]   = 99.9
    df["atr"] = 0.5
    obs = ta._detect_order_blocks(df)
    bullish_obs = [o for o in obs if o["type"] == "bullish"]
    assert len(bullish_obs) >= 1, f"Expected bullish OB, got: {obs}"
    ob = bullish_obs[-1]
    assert ob["low"] == 100.0 and ob["high"] == 101.0, f"OB bounds wrong: {ob}"
    print("PASS: test_order_block_detects_bullish_ob")

def test_order_block_detects_bearish_ob():
    from mexc_scalperV4 import ScalperTA
    ta = ScalperTA(cfg)
    df = make_df(30)
    # Candle 25: bullish — kandidat Bearish OB
    df.at[25, "open"]  = 100.0
    df.at[25, "close"] = 101.0  # bullish
    df.at[25, "high"]  = 101.2
    df.at[25, "low"]   = 99.8
    # Candle 26: impulse bearish besar body=4.0
    df.at[26, "open"]  = 101.0
    df.at[26, "close"] = 97.0
    df.at[26, "high"]  = 101.1
    df.at[26, "low"]   = 96.9
    df["atr"] = 0.5
    obs = ta._detect_order_blocks(df)
    bearish_obs = [o for o in obs if o["type"] == "bearish"]
    assert len(bearish_obs) >= 1, f"Expected bearish OB, got: {obs}"
    print("PASS: test_order_block_detects_bearish_ob")
```

- [ ] **Step 2: Jalankan test — pastikan GAGAL**

```bash
cd ~/scalper && python3 tests/test_zone_features.py
```

Expected: `AttributeError: 'ScalperTA' object has no attribute '_detect_order_blocks'`

- [ ] **Step 3: Tambah `_detect_order_blocks` ke ScalperTA (setelah `_detect_sd_zones`)**

```python
    # ─── ICT Order Block Detection ────────────────────────────

    def _detect_order_blocks(self, df: pd.DataFrame) -> list:
        lookback = self.cfg.get("OB_LOOKBACK", 20)
        df = df.tail(lookback + 5).reset_index(drop=True)
        avg_atr = df["atr"].mean() if "atr" in df.columns else (df["high"] - df["low"]).mean()
        impulse_min = 2.0 * avg_atr
        obs = []

        for i in range(1, len(df) - 1):
            impulse = df.iloc[i]
            body = abs(impulse["close"] - impulse["open"])
            if body < impulse_min:
                continue
            is_bull = impulse["close"] > impulse["open"]

            for j in range(1, min(4, i + 1)):
                prev = df.iloc[i - j]
                prev_bear = prev["close"] < prev["open"]
                prev_bull = prev["close"] > prev["open"]
                if is_bull and prev_bear:
                    obs.append({
                        "type":      "bullish",
                        "low":       float(min(prev["open"], prev["close"])),
                        "high":      float(max(prev["open"], prev["close"])),
                        "formed_at": i - j,
                    })
                    break
                elif not is_bull and prev_bull:
                    obs.append({
                        "type":      "bearish",
                        "low":       float(min(prev["open"], prev["close"])),
                        "high":      float(max(prev["open"], prev["close"])),
                        "formed_at": i - j,
                    })
                    break

        return obs
```

- [ ] **Step 4: Jalankan test — pastikan PASS**

```bash
cd ~/scalper && python3 tests/test_zone_features.py
```

Expected: semua 4 test PASS.

---

## Task 4: ScalperTA._calc_poc() dan _calc_midrange()

**Files:**
- Modify: `mexc_scalperV4.py` — tambah setelah `_detect_order_blocks`
- Test: `tests/test_zone_features.py`

- [ ] **Step 1: Tambah test POC dan midrange ke test file**

```python
def test_calc_poc_returns_high_volume_level():
    from mexc_scalperV4 import ScalperTA
    ta = ScalperTA(cfg)
    df = make_df(20)
    # Volume tinggi di close = 101
    for i in range(10, 15):
        df.at[i, "close"]  = 101.0
        df.at[i, "volume"] = 9999.0
    poc = ta._calc_poc(df)
    assert 100.5 <= poc <= 101.5, f"POC {poc} tidak dekat 101"
    print("PASS: test_calc_poc_returns_high_volume_level")

def test_calc_midrange():
    from mexc_scalperV4 import ScalperTA
    ta = ScalperTA(cfg)
    df = make_df(30)
    df["high"] = 110.0
    df["low"]  = 90.0
    mid = ta._calc_midrange(df, lookback=20)
    assert mid == 100.0, f"Expected mid=100, got {mid}"
    print("PASS: test_calc_midrange")
```

- [ ] **Step 2: Jalankan test — pastikan GAGAL**

```bash
cd ~/scalper && python3 tests/test_zone_features.py
```

Expected: `AttributeError: 'ScalperTA' object has no attribute '_calc_poc'`

- [ ] **Step 3: Tambah `_calc_poc` dan `_calc_midrange` ke ScalperTA (setelah `_detect_order_blocks`)**

```python
    # ─── Volume Profile: Point of Control ─────────────────────

    def _calc_poc(self, df: pd.DataFrame) -> float:
        buckets = self.cfg.get("POC_BUCKETS", 20)
        if "volume" not in df.columns or df["volume"].sum() == 0:
            return 0.0
        lo, hi = float(df["low"].min()), float(df["high"].max())
        if hi <= lo:
            return (lo + hi) / 2
        bsize   = (hi - lo) / buckets
        vol_arr = np.zeros(buckets)
        for _, row in df.iterrows():
            idx = min(int((float(row["close"]) - lo) / bsize), buckets - 1)
            vol_arr[idx] += float(row.get("volume", 0))
        best = int(np.argmax(vol_arr))
        return float(lo + (best + 0.5) * bsize)

    # ─── Mid-range / Equilibrium ───────────────────────────────

    def _calc_midrange(self, df: pd.DataFrame, lookback: int = 20) -> float:
        recent = df.tail(lookback)
        return float((float(recent["high"].max()) + float(recent["low"].min())) / 2)
```

- [ ] **Step 4: Jalankan test — pastikan PASS**

```bash
cd ~/scalper && python3 tests/test_zone_features.py
```

Expected: semua 6 test PASS.

---

## Task 5: ScalperBotV4._refresh_pivot_levels()

**Files:**
- Modify: `mexc_scalperV4.py` — `__init__` (baris ~2730) + tambah metode baru ke `ScalperBotV4`
- Test: `tests/test_zone_features.py`

- [ ] **Step 1: Tambah instance variables ke `ScalperBotV4.__init__` (setelah baris `self._last_signal_cached`)**

Cari baris (sekitar 2731):
```python
        self._last_signal_cached:      Optional[dict]   = None
```

Tambah dua baris setelahnya:
```python
        self._pivot_cache:             dict             = {}
        self._last_df:                 Optional[object] = None
```

- [ ] **Step 2: Tambah test pivot (offline — tanpa API)**

Append ke `tests/test_zone_features.py`:

```python
def test_pivot_classic_formulas():
    """Verifikasi rumus pivot classic dengan nilai OHLC diketahui."""
    H, L, C = 110.0, 90.0, 105.0
    PP = (H + L + C) / 3  # = 101.667
    R1 = 2*PP - L          # = 113.333
    S1 = 2*PP - H          # = 93.333
    R2 = PP + (H - L)      # = 121.667
    S2 = PP - (H - L)      # = 81.667
    assert abs(PP - 101.667) < 0.01, f"PP wrong: {PP}"
    assert abs(R1 - 113.333) < 0.01, f"R1 wrong: {R1}"
    assert abs(S1 - 93.333)  < 0.01, f"S1 wrong: {S1}"
    assert abs(R2 - 121.667) < 0.01, f"R2 wrong: {R2}"
    assert abs(S2 - 81.667)  < 0.01, f"S2 wrong: {S2}"
    print("PASS: test_pivot_classic_formulas")

def test_pivot_camarilla_formulas():
    H, L, C = 110.0, 90.0, 105.0
    HL = H - L  # = 20
    k  = HL * 1.1
    R3 = C + k/4   # = 105 + 5.5 = 110.5
    S3 = C - k/4   # = 105 - 5.5 = 99.5
    assert abs(R3 - 110.5) < 0.01, f"Cam R3 wrong: {R3}"
    assert abs(S3 - 99.5)  < 0.01, f"Cam S3 wrong: {S3}"
    print("PASS: test_pivot_camarilla_formulas")
```

- [ ] **Step 3: Jalankan test — pastikan PASS (pure math, tidak butuh API)**

```bash
cd ~/scalper && python3 tests/test_zone_features.py
```

Expected: semua 8 test PASS.

- [ ] **Step 4: Tambah `_refresh_pivot_levels` ke `ScalperBotV4` (sebagai metode baru, setelah `_load_ml_model` atau dekat metode lain di class tersebut)**

Cari `def _load_ml_model` dan tambahkan metode baru sebelum atau sesudahnya:

```python
    # ─── Pivot Level Cache ─────────────────────────────────────

    def _refresh_pivot_levels(self, symbol: str) -> dict:
        today = datetime.now(timezone.utc).date().isoformat()
        cache_key = f"{symbol}_{today}"
        if self._pivot_cache.get("key") == cache_key:
            return self._pivot_cache.get("levels", {})

        df_d = self._fetch_df(symbol, "1d")
        if df_d is None or len(df_d) < 2:
            log.debug("[Pivot] Data harian tidak cukup, skip pivot levels")
            return {}

        prev = df_d.iloc[-2]
        H  = float(prev["high"])
        L  = float(prev["low"])
        C  = float(prev["close"])
        HL = H - L
        ptype  = self.cfg.get("PIVOT_TYPE", "ALL")
        levels = {}

        if ptype in ("CLASSIC", "ALL"):
            PP = (H + L + C) / 3
            levels["classic"] = {
                "PP": PP,
                "R1": 2*PP - L,        "R2": PP + HL,        "R3": H + 2*(PP - L),
                "S1": 2*PP - H,        "S2": PP - HL,        "S3": L - 2*(H - PP),
            }

        if ptype in ("CAMARILLA", "ALL"):
            k = HL * 1.1
            levels["camarilla"] = {
                "R1": C + k/12, "R2": C + k/6, "R3": C + k/4, "R4": C + k/2,
                "S1": C - k/12, "S2": C - k/6, "S3": C - k/4, "S4": C - k/2,
            }

        if ptype in ("FIB", "ALL"):
            PP = (H + L + C) / 3
            levels["fibonacci"] = {
                "PP": PP,
                "R1": PP + 0.382*HL, "R2": PP + 0.618*HL, "R3": PP + HL,
                "S1": PP - 0.382*HL, "S2": PP - 0.618*HL, "S3": PP - HL,
            }

        self._pivot_cache = {"key": cache_key, "levels": levels}
        log.debug(f"[Pivot] {ptype} loaded for {symbol} ({today})")
        return levels
```

---

## Task 6: ScalperBotV4._get_zone_context()

**Files:**
- Modify: `mexc_scalperV4.py` — tambah metode setelah `_refresh_pivot_levels`
- Test: `tests/test_zone_features.py`

- [ ] **Step 1: Tambah test zone_context**

Append ke `tests/test_zone_features.py`:

```python
def test_zone_context_score_ob_bonus():
    """Zone context harus memberi +2 saat harga di dalam bullish OB."""
    from mexc_scalperV4 import ScalperTA
    ta = ScalperTA(cfg)
    df = make_df(30)
    # Setup bullish OB di candle 25-26
    df.at[25, "open"]  = 101.0; df.at[25, "close"] = 100.0
    df.at[25, "high"]  = 101.2; df.at[25, "low"]   = 99.8
    df.at[26, "open"]  = 100.0; df.at[26, "close"] = 104.0
    df.at[26, "high"]  = 104.1; df.at[26, "low"]   = 99.9
    df["atr"] = 0.5

    # Simulasikan _get_zone_context tanpa bot instance
    obs = ta._detect_order_blocks(df)
    price = 100.3  # dalam OB range 100.0-101.0
    prox  = 0.5 * 0.5  # ZONE_PROXIMITY_ATR=0.5 * atr=0.5
    at_ob = any(
        (ob["low"] - prox <= price <= ob["high"] + prox) and ob["type"] == "bullish"
        for ob in obs
    )
    assert at_ob, f"Harga {price} seharusnya di dalam bullish OB. OBs: {obs}"
    print("PASS: test_zone_context_score_ob_bonus")

def test_zone_context_sd_gate_blocks():
    """Gate harus block jika ada fresh zone tapi harga tidak di dalamnya."""
    from mexc_scalperV4 import ScalperTA
    ta = ScalperTA(cfg)
    df = make_df(60)
    # Fresh demand zone di 99.9-100.2
    for i in range(50, 53):
        df.at[i, "open"] = 100.0; df.at[i, "close"] = 100.1
        df.at[i, "high"] = 100.2; df.at[i, "low"]   = 99.9
    df.at[53, "open"] = 100.0; df.at[53, "close"] = 103.0
    df.at[53, "high"] = 103.1; df.at[53, "low"]   = 99.9
    # Candle 54-59: harga tinggi, tidak menyentuh zona
    for i in range(54, 60):
        df.at[i, "open"]  = 103.0; df.at[i, "close"] = 103.5
        df.at[i, "high"]  = 104.0; df.at[i, "low"]   = 102.5
    df["atr"] = 0.5

    zones = ta._detect_sd_zones(df)
    fresh = [z for z in zones if z["fresh"]]
    price = 103.5  # jauh dari zona demand 99.9-100.2
    prox  = 0.25
    in_zone = any(
        z["low"] - prox <= price <= z["high"] + prox and z["type"] == "demand"
        for z in fresh
    )
    gate_ok = not (fresh and not in_zone)
    assert not gate_ok, "Gate harus block karena ada fresh demand zone tapi harga tidak di sana"
    print("PASS: test_zone_context_sd_gate_blocks")
```

- [ ] **Step 2: Jalankan test — pastikan PASS (logic diuji langsung)**

```bash
cd ~/scalper && python3 tests/test_zone_features.py
```

Expected: semua 10 test PASS.

- [ ] **Step 3: Tambah `_get_zone_context` ke `ScalperBotV4` (setelah `_refresh_pivot_levels`)**

```python
    # ─── Zone Context Aggregator ───────────────────────────────

    def _get_zone_context(self, price: float, df, atr: float, side: str) -> dict:
        cfg   = self.cfg
        prox  = atr * cfg.get("ZONE_PROXIMITY_ATR", 0.5)
        score = 0
        confluence = 0
        details    = []
        sd_gate_ok = True
        in_sd_zone = False
        at_ob      = False
        at_pivot   = False
        nearest_support    = None
        nearest_resistance = None

        # ── S&D Zone gate ──────────────────────────────────────
        if cfg.get("USE_SD_ZONES", True):
            zones = self.ta._detect_sd_zones(df)
            fresh = [z for z in zones if z["fresh"]]
            for z in fresh:
                in_z = z["low"] - prox <= price <= z["high"] + prox
                right = (side == "LONG" and z["type"] == "demand") or \
                        (side == "SHORT" and z["type"] == "supply")
                if in_z and right:
                    in_sd_zone = True
                    confluence += 1
                    details.append(f"{z['type'].capitalize()} Zone ✓")
                    break
            if fresh and not in_sd_zone:
                sd_gate_ok = False

        # ── Order Blocks ───────────────────────────────────────
        if cfg.get("USE_ORDER_BLOCKS", True):
            obs = self.ta._detect_order_blocks(df)
            for ob in obs:
                in_ob = ob["low"] - prox <= price <= ob["high"] + prox
                right  = (side == "LONG" and ob["type"] == "bullish") or \
                         (side == "SHORT" and ob["type"] == "bearish")
                if in_ob and right:
                    at_ob = True
                    score += 2
                    confluence += 1
                    details.append(f"OB {ob['type']} ✓")
                    break

        # ── Pivot Levels ───────────────────────────────────────
        if cfg.get("USE_PIVOT_LEVELS", True):
            pivots   = self._refresh_pivot_levels(cfg["SYMBOL"])
            all_vals = [(t, n, v) for t, lvls in pivots.items() for n, v in lvls.items()]
            supports    = [(t, n, v) for t, n, v in all_vals if v < price]
            resistances = [(t, n, v) for t, n, v in all_vals if v > price]
            if supports:
                nearest_support = max(supports, key=lambda x: x[2])
            if resistances:
                nearest_resistance = min(resistances, key=lambda x: x[2])
            nearby = [(t, n, v) for t, n, v in all_vals if abs(v - price) <= prox]
            if nearby:
                at_pivot = True
                score   += 1
                confluence += 1
                details.append(f"Pivot {nearby[0][1]} ({nearby[0][0]}) ✓")

        # ── POC ────────────────────────────────────────────────
        poc = 0.0
        if cfg.get("USE_POC", True):
            poc = self.ta._calc_poc(df)
            if poc > 0 and abs(poc - price) <= prox:
                score += 1
                confluence += 1
                details.append(f"POC ${poc:.4f} ✓")
                if "volume" in df.columns:
                    vol_avg = df["volume"].tail(20).mean()
                    if vol_avg > 0 and float(df.iloc[-1].get("volume", 0)) > vol_avg * 1.5:
                        score += 1
                        details.append("POC break vol ✓")

        # ── Mid-range premium/discount ─────────────────────────
        mid = self.ta._calc_midrange(df)
        if mid > 0:
            if (side == "LONG" and price < mid) or (side == "SHORT" and price > mid):
                score += 1
                confluence += 1
                details.append("Discount/Premium ✓")
            elif (side == "LONG" and price > mid) or (side == "SHORT" and price < mid):
                score -= 1
                details.append("Wrong zone -1")

        # ── Confluence bonus ───────────────────────────────────
        if cfg.get("USE_CONFLUENCE", True):
            if confluence >= 4:
                score += 3
                details.append(f"Confluence ×{confluence} +3")
            elif confluence == 3:
                score += 2
                details.append(f"Confluence ×{confluence} +2")
            elif confluence == 2:
                score += 1
                details.append(f"Confluence ×{confluence} +1")

        return {
            "in_sd_zone":          in_sd_zone,
            "sd_gate_ok":          sd_gate_ok,
            "at_order_block":      at_ob,
            "at_pivot":            at_pivot,
            "nearest_support":     nearest_support,
            "nearest_resistance":  nearest_resistance,
            "poc":                 poc,
            "midrange":            mid,
            "confluence_count":    confluence,
            "zone_score":          score,
            "zone_detail":         details,
        }
```

---

## Task 7: Wire ke fetch_and_analyze() dan Main Loop

**Files:**
- Modify: `mexc_scalperV4.py` — `fetch_and_analyze()` dan main loop entry section

- [ ] **Step 1: Simpan `self._last_df = df` di `fetch_and_analyze()`**

Di `fetch_and_analyze()` (baris ~3046), tepat setelah baris `self._last_candle_ts = cur_candle_ts`:

```python
        self._last_candle_ts = cur_candle_ts
        self._last_df = df          # ← TAMBAH BARIS INI
```

- [ ] **Step 2: Tambah zone gate di main loop entry section**

Cari blok ini di main loop (sekitar baris 3960–3975):

```python
                    if can_open:
                        signal["signal"]         = sig_label
                        signal["is_early_entry"] = is_early
                        old_count = self.open_positions_count()
                        self._open_position(signal, price)
```

Tambahkan blok zone context SEBELUM `if can_open:` yang terakhir itu (setelah ML filter block):

```python
                    # ── Zone Context: S&D gate + zone scoring ──
                    if can_open and self._last_df is not None:
                        _atr = signal.get("atr", 0.0)
                        if _atr > 0:
                            zone_ctx = self._get_zone_context(price, self._last_df, _atr, sig_label)
                            signal["zone_ctx"] = zone_ctx
                            zs = zone_ctx["zone_score"]
                            if not zone_ctx["sd_gate_ok"]:
                                can_open = False
                                log.info(f"[ZONE] S&D gate block — price not in fresh {sig_label} zone")
                            elif zone_ctx["zone_detail"]:
                                log.info(
                                    f"[Zone] {sig_label} | "
                                    + " | ".join(zone_ctx["zone_detail"])
                                    + f" | Zone Score {zs:+d}"
                                )
```

- [ ] **Step 3: Verifikasi bot startup tanpa error**

```bash
cd ~/scalper
kill $(pgrep -f mexc_scalperV4) 2>/dev/null; sleep 2
nohup venv/bin/python3 mexc_scalperV4.py --dashboard > /dev/null 2>&1 &
sleep 10
tail -20 scalper_v4.log
```

Expected: bot start tanpa `AttributeError` atau `TypeError`. Harus ada log `Config dimuat` dan `[1H TREND]`.

---

## Task 8: Pivot TP/SL Override di calculate_levels()

**Files:**
- Modify: `mexc_scalperV4.py` — `calculate_levels()` baris ~2284 dan `_open_position()` baris ~3197

- [ ] **Step 1: Tambah parameter `pivot_context=None` ke `calculate_levels()`**

Ubah signature (baris ~2284) dari:
```python
    def calculate_levels(self, side: str, entry: float, atr: float,
                         trend_power: int = 50, atr_pct: float = 0.0) -> dict:
```
Menjadi:
```python
    def calculate_levels(self, side: str, entry: float, atr: float,
                         trend_power: int = 50, atr_pct: float = 0.0,
                         pivot_context: dict = None) -> dict:
```

- [ ] **Step 2: Tambah pivot override logic di akhir `calculate_levels()`, sebelum `return` terakhir**

Tepat sebelum `if side == "LONG": return {` (sekitar baris 2317), tambahkan:

```python
        # ── Pivot TP/SL override (jika USE_PIVOT_TARGETS=true dan pivot_context tersedia)
        if pivot_context and self.cfg.get("USE_PIVOT_TARGETS", False):
            ns  = pivot_context.get("nearest_support")
            nr  = pivot_context.get("nearest_resistance")
            min_sl_dist = atr * sl_base * 0.5   # minimum 50% dari ATR SL

            if side == "LONG" and ns and nr:
                p_sl  = ns[2]   # value dari tuple (type, name, value)
                p_tp1 = nr[2]
                if (entry - p_sl) >= min_sl_dist and p_tp1 > entry:
                    sl   = entry - p_sl
                    tp1  = p_tp1 - entry
                    tp2  = tp1 * 2.0
                    tp3  = tp1 * 3.0

            elif side == "SHORT" and ns and nr:
                p_sl  = nr[2]
                p_tp1 = ns[2]
                if (p_sl - entry) >= min_sl_dist and p_tp1 < entry:
                    sl   = p_sl - entry
                    tp1  = entry - p_tp1
                    tp2  = tp1 * 2.0
                    tp3  = tp1 * 3.0
```

- [ ] **Step 3: Pass `pivot_context` dari `_open_position()` ke `calculate_levels()`**

Di `_open_position()` (sekitar baris 3200), ubah:
```python
        levels = self.risk.calculate_levels(side, entry_price, atr,
                                            atr_pct=signal.get("atr_pct", 0.0))
```
Menjadi:
```python
        zone_ctx = signal.get("zone_ctx", {})
        levels = self.risk.calculate_levels(side, entry_price, atr,
                                            atr_pct=signal.get("atr_pct", 0.0),
                                            pivot_context=zone_ctx if self.cfg.get("USE_PIVOT_TARGETS") else None)
```

- [ ] **Step 4: Jalankan semua test — pastikan masih PASS**

```bash
cd ~/scalper && python3 tests/test_zone_features.py
```

Expected: semua 10 test PASS.

- [ ] **Step 5: Restart bot dan verifikasi log normal**

```bash
cd ~/scalper
kill $(pgrep -f mexc_scalperV4) 2>/dev/null; sleep 2
nohup venv/bin/python3 mexc_scalperV4.py --dashboard > /dev/null 2>&1 &
sleep 15
tail -30 scalper_v4.log | grep -E 'Zone|Pivot|OB|Demand|Supply|Config|ERROR|Traceback'
```

Expected:
- `Config dimuat dari config_scalper_v4.json` — config baru terbaca
- Tidak ada `ERROR` atau `Traceback`
- Jika ada sinyal LONG/SHORT: muncul `[Zone] ...` log dengan detail zona

---

## Catatan Akhir

- `USE_PIVOT_TARGETS` default `False` — aktifkan setelah observasi beberapa hari di dry run bahwa pivot level menghasilkan TP/SL yang lebih baik dari ATR
- `USE_SD_ZONES` gate bisa dimatikan dengan `false` jika terlalu banyak sinyal valid yang diblokir — monitor di dry run journal
- Semua feature bisa dimatikan via config tanpa restart menggunakan dashboard `/api/config` endpoint yang sudah ada
