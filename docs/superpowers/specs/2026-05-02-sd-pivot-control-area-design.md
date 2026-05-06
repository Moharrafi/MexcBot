# Design: Supply & Demand + Pivot Points + Control Area
**Date:** 2026-05-02  
**Bot:** mexc_scalperV4.py  
**Status:** Approved

---

## 1. Overview

Menambahkan tiga sistem analisis teknikal ke MEXC Scalper V4:
1. **Supply & Demand Zones** (klasik) + **ICT Order Blocks**
2. **Pivot Points** (Classic, Camarilla, Fibonacci) — juga sebagai TP/SL dinamis
3. **Control Area**: POC (volume profile), Mid-range/equilibrium, dan Confluence zone

Semua fitur baru **fully optional** via config — jika semua di-set `false`, bot berjalan identik dengan versi sekarang.

---

## 2. Arsitektur

Tiga modul baru ditambah ke `ScalperTA` dan di-wire ke pipeline entry/exit:

```
ScalperTA.compute()
    ├── [sudah ada] SuperTrend, DEMA, ROC, ADX, Squeeze, OBI...
    ├── [BARU] _calc_pivot_levels()    → PP, S1-S3, R1-R3 (Classic/Cam/Fib)
    ├── [BARU] _detect_sd_zones()      → Supply/Demand boxes dari base candles
    └── [BARU] _detect_order_blocks()  → ICT Order Block dari impulse moves

ScalperBot._evaluate_entry()
    ├── [sudah ada] bull_score / bear_score (max 22)
    ├── [BARU] zone_context = _get_zone_context(price)
    │     ├── at_pivot_level?     → +1 score + override TP/SL
    │     ├── inside_sd_zone?     → gate (wajib jika USE_SD_ZONES=true)
    │     ├── at_order_block?     → +2 score
    │     ├── at_poc?             → +1 score
    │     ├── premium_discount?   → +1 / -1
    │     └── confluence_count    → +0 s/d +3
    └── Final entry: (base_score + zone_score) ≥ MIN_SCORE AND gate valid

ScalperBot._calc_exit_levels()
    ├── [sudah ada] ATR-based SL/TP1/TP2
    └── [BARU] jika USE_PIVOT_TARGETS=true → TP1=nearest R/S, TP2=next R/S
               fallback ke ATR jika pivot SL < minimum ATR
```

### Config Baru

```json
"USE_SD_ZONES":        true,
"USE_ORDER_BLOCKS":    true,
"USE_PIVOT_LEVELS":    true,
"PIVOT_TYPE":          "ALL",
"USE_PIVOT_TARGETS":   true,
"USE_POC":             true,
"USE_CONFLUENCE":      true,
"ZONE_PROXIMITY_ATR":  0.5,
"SD_LOOKBACK":         50,
"OB_LOOKBACK":         20,
"POC_BUCKETS":         20
```

---

## 3. Supply & Demand Zones (Klasik)

### Deteksi

Zona terbentuk dari pola **base → impulse**:

- **Impulse candle**: body ≥ 1.5×ATR rata-rata
- **Base**: 1–4 candle sebelum impulse dengan body < 0.5×ATR
- **Demand zone**: base sebelum impulse bullish → zona = `[low_base, high_base]`
- **Supply zone**: base sebelum impulse bearish → zona = `[low_base, high_base]`
- **Fresh zone**: belum pernah disentuh harga setelah terbentuk — satu-satunya yang valid untuk entry

### Integrasi

| Kondisi | Efek |
|---------|------|
| Harga dalam fresh Demand zone (LONG signal) | Gate — wajib jika `USE_SD_ZONES=true` |
| Harga dalam fresh Supply zone (SHORT signal) | Gate — wajib jika `USE_SD_ZONES=true` |
| Tidak ada zona fresh | Gate dilewati (tidak memblokir entry) |

---

## 4. ICT Order Blocks

### Deteksi

- **Bullish OB**: candle bearish (merah) terakhir sebelum impulse naik ≥ 2×ATR → zona = body candle tersebut
- **Bearish OB**: candle bullish (hijau) terakhir sebelum impulse turun ≥ 2×ATR → zona = body candle tersebut
- Lookback: `OB_LOOKBACK` candle terakhir

### Integrasi ke Scoring

| Kondisi | Poin |
|---------|------|
| Harga di dalam Order Block arah sesuai | +2 |

---

## 5. Pivot Points

### Kalkulasi (dari OHLC hari sebelumnya, cache reset harian)

**Classic:**
```
PP = (H + L + C) / 3
R1 = 2×PP - L      S1 = 2×PP - H
R2 = PP + (H - L)  S2 = PP - (H - L)
R3 = H + 2×(PP-L)  S3 = L - 2×(H-PP)
```

**Camarilla:**
```
R1 = C + (H-L)×1.1/12    S1 = C - (H-L)×1.1/12
R2 = C + (H-L)×1.1/6     S2 = C - (H-L)×1.1/6
R3 = C + (H-L)×1.1/4     S3 = C - (H-L)×1.1/4
R4 = C + (H-L)×1.1/2     S4 = C - (H-L)×1.1/2
```

**Fibonacci:**
```
PP = (H + L + C) / 3
R1 = PP + 0.382×(H-L)    S1 = PP - 0.382×(H-L)
R2 = PP + 0.618×(H-L)    S2 = PP - 0.618×(H-L)
R3 = PP + 1.000×(H-L)    S3 = PP - 1.000×(H-L)
```

### Integrasi ke Entry

| Kondisi | Efek |
|---------|------|
| Harga dalam `ZONE_PROXIMITY_ATR` dari pivot mana pun | +1 score |

### Integrasi ke TP/SL (jika `USE_PIVOT_TARGETS=true`)

```
LONG:  SL = S level terdekat di bawah entry  |  TP1 = R1  |  TP2 = R2
SHORT: SL = R level terdekat di atas entry   |  TP1 = S1  |  TP2 = S2
```

Jika jarak pivot SL < `ATR_SL_MULT × ATR` → fallback ke ATR-based SL.

---

## 6. Control Area

### A) POC — Point of Control

Range hari ini dibagi `POC_BUCKETS` (default 20) bucket harga. Volume setiap candle dimasukkan ke bucket sesuai close. POC = harga tengah bucket volume tertinggi.

| Kondisi | Efek |
|---------|------|
| Harga dalam 0.5×ATR dari POC + sinyal sejalan | +1 score |
| Harga break POC dengan volume spike | +1 score tambahan |

### B) Mid-range / Equilibrium

```
swing_high = max(high, 20 candle terakhir)
swing_low  = min(low,  20 candle terakhir)
mid        = (swing_high + swing_low) / 2
```

- Harga di bawah mid (discount) → LONG lebih valid: +1
- Harga di atas mid (premium) → SHORT lebih valid: +1
- Berlawanan arah: -1

### C) Confluence Zone

Hitung berapa banyak level berbeda yang berkumpul dalam radius `ZONE_PROXIMITY_ATR` dari harga saat ini:

| Level yang dihitung | |
|--------------------|-|
| Pivot PP/R/S | ✓ |
| S&D zone boundary | ✓ |
| Order Block boundary | ✓ |
| POC | ✓ |
| Mid-range | ✓ |

**Bonus score:**
| Confluence count | Score tambahan |
|-----------------|---------------|
| 1 | +0 |
| 2 | +1 |
| 3 | +2 |
| ≥4 | +3 |

---

## 7. Alur Entry Lengkap

```
evaluate_entry()
 1. Hitung zone_context (S&D, OB, Pivot, POC, mid, confluence)
 2. Gate check: jika USE_SD_ZONES=true dan ada fresh zone → harga harus di dalamnya
 3. Base score (sudah ada, max 22)
 4. Zone score (max +8):
      OB arah sesuai        → +2
      Pivot proximity       → +1
      POC proximity+vol     → +1 (+1)
      Premium/Discount      → +1 / -1
      Confluence ×2/×3/≥×4 → +1/+2/+3
 5. Total score ≥ MIN_BULL/BEAR_SCORE → entry
 6. Set TP/SL: pivot targets jika USE_PIVOT_TARGETS=true, else ATR
```

---

## 8. Log Output Baru

```
[Zone] LONG @ $4582 | Demand Zone ✓ | OB ✓ | S1 Classic $4571 | Confluence ×3 | Zone Score +5
[Entry] LONG XAUT_USDT | Score 17+5=22/30 | TP1=$4601(R1) TP2=$4622(R2) SL=$4571(S1)
```

---

## 9. Error Handling

| Kasus | Penanganan |
|-------|-----------|
| Data candle < `SD_LOOKBACK` | Skip S&D deteksi, gate dilewati |
| Belum ada data hari sebelumnya untuk pivot | Skip pivot score & targets, fallback ATR |
| Volume = 0, POC tidak bisa dihitung | Skip POC score |
| Semua `USE_*` = false | Bot identik dengan versi sekarang |

---

## 10. File yang Dimodifikasi

| File | Perubahan |
|------|-----------|
| `mexc_scalperV4.py` | Tambah `_calc_pivot_levels()`, `_detect_sd_zones()`, `_detect_order_blocks()`, `_get_zone_context()`, update `_evaluate_entry()`, update `_calc_exit_levels()` |
| `config_scalper_v4.json` | Tambah 10 config key baru |
