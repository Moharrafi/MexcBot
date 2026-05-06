import requests
import urllib3
urllib3.disable_warnings()
def check():
    r = requests.get('https://contract.mexc.com/api/v1/contract/kline/BTC_USDT', params={'interval': 'Min15', 'limit': 2000}, verify=False)
    data = r.json()
    print(f"RESPONSE JSON: {data}")
    print(f"FETCHED: {len(data.get('data', []))}")
if __name__ == '__main__':
    check()
