import requests, ssl
class TLSAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        ctx.check_hostname = False
        kwargs['ssl_context'] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)

s = requests.Session()
s.mount("https://", TLSAdapter())
r = s.get('https://contract.mexc.com/api/v1/contract/ticker')
print([x for x in r.json().get('data', []) if x.get('symbol') == 'BTC_USDT'][0])
