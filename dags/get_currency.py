from requests import get
from xml.etree import ElementTree as ET
from datetime import datetime

main_cur = ('USD', 'EUR', 'BTC')
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
            if dict_val['ccy'].lower() == self.currency.lower():
                return dict_val['buy'], dict_val['sale'], dict_val['ccy'], dict_val['base_ccy']
        raise KeyError(f'Wrong currency "{self.currency}" chosen. USD, EUR, PLZ, GBP and CZK are available')

    def extractor(self):
        today = datetime.now().strftime('%d.%m.%y')
        url ='https://api.privatbank.ua/p24api/exchange_rates?date={}'.format(today)
        page = get(url)
        root = ET.fromstring(page.content)
        attrib_dict = dict(root.attrib)
        return attrib_dict['date'], attrib_dict['bank']

    def bank(self):
        return self.extractor()[1]

    def day(self):
        return self.extractor()[0]

    def buy(self):
        return self.currency_extractor()[0]

    def sell(self):
        return self.currency_extractor()[1]

    def ccy(self):
        return self.currency_extractor()[2]

    def base_ccy(self):
        return self.currency_extractor()[3]

# print(ExchangeRate('USD').base_ccy(), ExchangeRate('USD').ccy(), ExchangeRate('USD').sell())
#print(ExchangeRate('usd').extractor())
