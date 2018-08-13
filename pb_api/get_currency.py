from requests import get
from xml.etree import ElementTree as ET

main_cur = ('USD', 'EUR', 'BTC', 'RUB')
class ExchangeRate:
    def __init__(self, currency):
        self.currency = currency

    def currency_extractor(self):
        url = 'https://api.privatbank.ua/p24api/pubinfo?exchange&coursid=4'
        if self.currency.upper() in main_cur:
            url =  'https://api.privatbank.ua/p24api/pubinfo?exchange&coursid=5'
        page = get(url)
        root = ET.fromstring(page.content)

        for ex_rate in root.findall('row/exchangerate'):
            dict_val = dict(ex_rate.attrib)
            if dict_val['ccy'].lower() == self.currency:
                return dict_val['buy'], dict_val['sale']
        raise KeyError('Wrong currency chosen. USD, EUR, PLN, GBP and BTC are available')

    def buy(self):
        return self.currency_extractor()[0]

    def sell(self):
        return self.currency_extractor()[1]
