import requests
import ccxt
from datetime import datetime

class Binance():
    name = "Binance"
    transaction_fee = 0.0007 # transaction fee PERP (taker 0.03%) + 2 * option (taker 0.02%)
    def __init__(self, token, start, end, strike_range):
        self.token = token.upper()
        self.start_date = start
        self.end_date = end
        self.strike_range = strike_range
        self.future_account = ccxt.binance({'options':{'defaultType': 'future'}})

        self.options = self.filter_markets()

    def fetch_underlying_price(self):
        current_price_url = f"https://api.binance.com/api/v3/trades?symbol={self.token}BUSD&limit=1"
        self.current_price = float(requests.get(current_price_url).json()[0]["price"])

    def get_market_symbols(self):
        if self.exchange_name == "binance":
            url = "https://eapi.binance.com/eapi/v1/exchangeInfo"
            response = requests.get(url).json()
            markets = [ele["symbol"] for ele in response["optionSymbols"]]
        if self.exchange_name == "bybit":
            markets = [ele["symbol"] for ele in self.bybit_opt_client.query_symbol()["result"]["dataList"]]
        return markets

    def filter_markets(self):
        self.fetch_underlying_price()
        response = requests.get("https://eapi.binance.com/eapi/v1/exchangeInfo").json()
        markets = [ele["symbol"] for ele in response["optionSymbols"]]

        # Bybit altcoin options need to be handled separately
        markets = [markt for markt in markets if markt.split("-")[0].upper() == self.token]

        date_format = "%y%m%d"
        dates = [datetime.strptime(markt.split("-")[1], date_format) for markt in markets]
        dates_to_scan = set([dt for dt in dates if (dt >= self.start_date and dt <= self.end_date)])

        strikes = [int(markt.split("-")[2]) for markt in markets]
        strikes = [*set(strikes)]
        strikes.sort()
        strikes_to_scan = [strk for strk in strikes if strk < self.current_price][-self.strike_range:]  # get certain number of strikes above the current price
        strikes_to_scan.extend([strk for strk in strikes if strk > self.current_price][:self.strike_range]) # extend the list to certain number of strikes below the current price

        options_pairs = []
        for dt in dates_to_scan:
            for strike in strikes_to_scan:
                s = f"{self.token}-{dt.strftime(date_format)}-{strike}-".upper()
                options_pairs.append((s+"C", s+"P"))
        return options_pairs

    def get_prices(self, symbol, perp=False):
        if perp:
            response = self.future_account.fetch_order_book(symbol, limit=10)
        else:
            response = requests.get(f"https://eapi.binance.com/eapi/v1/depth?symbol={symbol}").json()
        long = float(response["asks"][0][0])
        short = float(response["bids"][0][0])
        return long, short

    def check_arb(self, pair):
        strike = int(pair[0].split("-")[2])
        long_perp, short_perp = self.get_prices(f"{self.token}USDT", True)
        long_call, short_call = self.get_prices(pair[0])
        long_put, short_put = self.get_prices(pair[1])

        long_call_profit = short_put + short_perp - long_call - strike
        short_call_profit = short_call + strike - long_put - long_perp
        if short_call_profit > self.threshold:
            print(f"Long PERP @ {long_perp}")
            print(f"Long '{pair[1]}' @ {long_put}")
            print(f"Short '{pair[0]}' @ {short_call}")
            print(f"Profit: {short_call_profit}")
            print("======================================================")
            
        if long_call_profit > self.threshold:
            print(f"Short PERP @ {short_perp}")
            print(f"Short '{pair[1]}' @ {short_put}")
            print(f"Long '{pair[0]}' @ {long_call}")
            print(f"Profit: {long_call_profit}")
            print("======================================================")

    def run(self):
        self.fetch_underlying_price()
        self.threshold = self.transaction_fee * self.current_price
        print(f"Arbitrage Threshold: ${round(self.threshold,2)}", end="\r")
        captured_errors = (KeyError, IndexError)
        for pair in self.options:
            try:
                self.check_arb(pair)
            except captured_errors:
                pass
