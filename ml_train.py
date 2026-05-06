"""
ML Model Trainer v2 — MEXC Scalper V4
Trains XGBoost classifier on collected trade data.

Run after ml_collector.py:
    python ml_train.py

Output: ml_model.pkl  (loaded automatically by the bot if present)
"""

import os
import numpy as np
import pandas as pd
import joblib
import xgboost as xgb
from sklearn.model_selection import StratifiedKFold, cross_val_score

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_PATH  = os.path.join(BASE_DIR, "ml_training_data.csv")
MODEL_PATH = os.path.join(BASE_DIR, "ml_model.pkl")

FEATURES = [
    # Core 5m indicators
    "side", "st_dir", "adx", "roc", "roc_accel", "willr", "atr_pct",
    "squeeze_on", "squeeze_mom", "sq_mom_aligned", "cvd_trend",
    "vol_ratio", "vol_spike", "body_ratio", "consec",
    "dema_cross", "vwap_dist", "hour_utc", "dow",
    # Multi-timeframe
    "st_dir_1h", "adx_1h", "roc_1h", "dema_cross_1h",
    "st_dir_15m", "adx_15m", "trend_aligned",
    # Pattern & structure
    "pattern_align", "struct_align", "sweep_align",
]

MIN_SAMPLES = 200


def load_data():
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(
            f"Training data not found: {DATA_PATH}\nRun ml_collector.py first."
        )
    df = pd.read_csv(DATA_PATH)
    print(f"Loaded {len(df):,} samples | Win rate: {df['label'].mean()*100:.1f}%")

    # Use only features that exist in this dataset (v1 data may lack some v2 cols)
    available = [f for f in FEATURES if f in df.columns]
    missing   = [f for f in FEATURES if f not in df.columns]
    if missing:
        print(f"  Note: {len(missing)} new features not in data yet (run collector again): {missing}")

    X = df[available].fillna(0).astype(float)
    y = df["label"].astype(int)
    return X, y, df, available


def train(X, y):
    if len(X) < MIN_SAMPLES:
        raise ValueError(
            f"Only {len(X)} samples — need {MIN_SAMPLES}+. Run ml_collector.py first."
        )

    pos_weight = (y == 0).sum() / max((y == 1).sum(), 1)

    model = xgb.XGBClassifier(
        n_estimators=500,
        max_depth=4,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.75,
        min_child_weight=20,
        gamma=0.15,
        reg_alpha=0.1,
        reg_lambda=1.0,
        scale_pos_weight=pos_weight,
        eval_metric="auc",
        random_state=42,
        n_jobs=-1,
    )

    print("\nRunning 5-fold cross-validation ...")
    cv     = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_val_score(model, X, y, cv=cv, scoring="roc_auc", n_jobs=-1)
    print(f"CV AUC: {scores.mean():.4f} ± {scores.std():.4f}  "
          f"({', '.join(f'{s:.3f}' for s in scores)})")

    if   scores.mean() < 0.52: print("\n⚠️  AUC < 0.52 — little predictive power. Collect more data.")
    elif scores.mean() < 0.55: print("\n⚠️  AUC < 0.55 — weak model. Use ML_MIN_SCORE ≤ 0.52.")
    elif scores.mean() < 0.60: print(f"\n✅ AUC {scores.mean():.3f} — model useful.")
    else:                       print(f"\n🎯 AUC {scores.mean():.3f} — strong model!")

    print("Fitting final model ...")
    model.fit(X, y)
    return model


def report(model, X, y, df_orig, features):
    probs = model.predict_proba(X)[:, 1]

    print("\n── Feature importance ──────────────────────────────")
    imp = pd.Series(model.feature_importances_, index=features).sort_values(ascending=False)
    for feat, val in imp.items():
        bar = "█" * int(val * 50)
        print(f"  {feat:<20} {val:.4f}  {bar}")

    print("\n── Threshold analysis ──────────────────────────────")
    print(f"  {'Thresh':>7}  {'Trades':>8}  {'Coverage':>9}  {'Win Rate':>9}  {'Verdict':}")
    total = len(y)
    for thresh in [0.48, 0.50, 0.52, 0.55, 0.58, 0.60, 0.63, 0.65, 0.68, 0.70]:
        mask = probs >= thresh
        if mask.sum() < 20:
            continue
        wr  = y[mask].mean() * 100
        cov = mask.sum() / total * 100
        verdict = ("✅ recommended" if 50 <= wr < 60 and cov >= 3
                   else "🎯 aggressive"  if wr >= 60 and cov >= 1
                   else "⚠️ too few"    if cov < 1
                   else "")
        print(f"  {thresh:>7.2f}  {mask.sum():>8,}  {cov:>8.1f}%  {wr:>8.1f}%  {verdict}")

    # MTF aligned subset
    if "trend_aligned" in df_orig.columns:
        aligned = df_orig["trend_aligned"] == 1
        if aligned.sum() > 100:
            probs_s = pd.Series(probs)
            mask_a  = aligned & (probs_s >= 0.60)
            if mask_a.sum() > 0:
                wr_a = y[mask_a].mean() * 100
                print(f"\n  MTF-aligned + score≥0.60: {mask_a.sum():,} trades | WR {wr_a:.1f}%")

    if "symbol" in df_orig.columns:
        print("\n── Per-symbol @ threshold 0.60 ─────────────────────")
        mask60 = probs >= 0.60
        grp = df_orig[mask60].copy()
        grp["label_"] = y[mask60].values
        if not grp.empty:
            s = grp.groupby("symbol")["label_"].agg(trades="count", win_rate="mean").round(3)
            print(s.sort_values("win_rate", ascending=False).to_string())


def main():
    X, y, df_orig, features = load_data()
    model = train(X, y)
    report(model, X, y, df_orig, features)

    joblib.dump({"model": model, "features": features}, MODEL_PATH)
    print(f"\n✅ Model saved → {MODEL_PATH}")
    print("   Restart the bot to load the new model.")


if __name__ == "__main__":
    main()
