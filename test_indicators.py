
import pandas as pd
import pandas_ta as ta
import numpy as np

# Create dummy data
df = pd.DataFrame({
    'open': np.random.randn(100),
    'high': np.random.randn(100) + 1,
    'low': np.random.randn(100) - 1,
    'close': np.random.randn(100),
    'volume': np.random.randn(100) * 100
})

print("Testing RSI...")
rsi = df.ta.rsi(length=14)
print(f"RSI type: {type(rsi)}")
print(f"RSI last value: {rsi.iloc[-1]}")

print("\nTesting ATR...")
atr = df.ta.atr(length=14)
print(f"ATR type: {type(atr)}")
print(f"ATR last value: {atr.iloc[-1]}")

print("\nTesting STOCH...")
stoch = df.ta.stoch(k=14, d=3, smooth_k=3)
print(f"STOCH type: {type(stoch)}")
print(f"STOCH columns: {stoch.columns.tolist() if hasattr(stoch, 'columns') else 'N/A'}")
