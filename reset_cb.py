import json

f = 'scalper/scalper_state_v4.json'
with open(f, 'r') as file:
    d = json.load(file)

d['peak_balance'] = d['balance']
d['circuit_breaker'] = False

with open(f, 'w') as file:
    json.dump(d, file, indent=4)
