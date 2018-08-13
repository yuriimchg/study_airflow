from get_currency import ExchangeRate

buy_gbp = ExchangeRate('gbp').buy()
print(buy_gbp)

sell_usd = ExchangeRate('usd').sell()
print(sell_usd)
