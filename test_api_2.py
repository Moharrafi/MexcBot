import requests
import urllib3
urllib3.disable_warnings()
def check():
    for sym in ['BTC_USDT', 'XAUT_USDT']:
        r = requests.get(f'https://contract.mexc.com/api/v1/contract/kline/{sym}', params={'interval': 'Min15', 'limit': 2000}, verify=False)
        d = r.json().get('data', {})
        t = d.get('time', [])
        print(f"ROWS {sym}: {len(t)}")
if __name__ == '__main__':
    check()
