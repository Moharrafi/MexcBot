import json
with open('bot_state.json', 'r+') as f:
  data = json.load(f)
  data['positions'] = []
  data['total_pnl'] = 0.0
  data['daily_pnl'] = 0.0
  data['circuit_breaker'] = False
  data['circuit_reason'] = ''
  data['peak_balance'] = data.get('balance', 0.0)
  f.seek(0)
  json.dump(data, f)
  f.truncate()
