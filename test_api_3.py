import requests
import urllib3
urllib3.disable_warnings()
def check():
    r = requests.get('https://contract.mexc.com/api/v1/contract/kline/BTC_USDT?interval=Min15&limit=1', verify=False)
    print(r.json().get('data', {}).keys())
if __name__ == '__main__':
    check()
