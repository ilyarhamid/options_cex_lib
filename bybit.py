import requests
from datetime import datetime
from pybit.unified_trading import HTTP
import time
from configparser import ConfigParser
import sys
import os  

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

config = ConfigParser()
config.read("./config.ini")

price_step_dict = {"ETH":25, "SOL":1}

def round_to(num, step):
    return step * (num//step)

class Bybit():
    name = "Bybit"
    transaction_fee = 0.0012 * 2 # transaction fee: PERP (taker 0.06%) + 2 * option (taker 0.03%). transaction fee * 2 for open and close.
    def __init__(self):
        self.token = None
        self.start_date = None
        self.end_date = None
        self.base_size = 0
        self.strike_range = 3
        self.dollar_threshold = 0
        self.place_order = 1
        self.stop = False
        self.best_opp = 0
        self.refetch_markets = False
        self.last_price = 0
        self.client = HTTP(
                            testnet=False,
                            api_key=config.get('credentials', 'api_key'),
                            api_secret=config.get('credentials', 'secret')
                            )
        self.status_update()

    def status_update(self):
        config.read("./config.ini")
        msg = ""
        temp = str(config.get('arb_configs', 'token'))
        if self.token != temp:
            msg += f"token: {self.token} -> {temp}\n"
            self.token = temp
            self.refetch_markets = True

        temp = self.str_to_datetime(config.get('arb_configs', 'start'))
        if self.start_date != temp:
            msg += f"start: {self.start_date} -> {temp.date()}\n"
            self.start_date = temp
            self.refetch_markets = True

        temp = self.str_to_datetime(config.get('arb_configs', 'end'))
        if self.end_date != temp:
            msg += f"end: {self.end_date} -> {temp.date()}\n"
            self.end_date = temp
            self.refetch_markets = True

        temp = float(config.get('arb_configs', 'base_size'))
        if self.base_size != temp:
            msg += f"base_size: {self.base_size} -> {temp}\n"
            self.base_size = temp

        temp = int(config.get('arb_configs', 'strike_range'))
        if self.strike_range != temp:
            msg += f"strike_range: {self.strike_range} -> {temp}\n"
            self.strike_range = temp
            self.refetch_markets = True

        temp = float(config.get('arb_configs', 'net_profit'))
        if self.dollar_threshold * self.base_size != temp:
            msg += f"net_profit: {self.dollar_threshold * self.base_size} -> {temp}\n"
            self.dollar_threshold = temp / self.base_size

        temp = int(config.get('arb_configs', 'place_order'))
        if self.place_order != temp:
            msg += f"place_order: {self.place_order} -> {temp}\n"
            self.place_order = temp
        
        self.fetch_underlying_price()

        if self.refetch_markets: 
            self.options = self.filter_markets()
            self.refetch_markets = False
            msg = "Fetched New Markets\n" + msg
        if len(msg): print(msg)

    def fetch_underlying_price(self):
        current_price_url = f"https://api.binance.com/api/v3/trades?symbol={self.token}BUSD&limit=1"
        self.current_price = float(requests.get(current_price_url).json()[0]["price"])
        self.refetch_markets = abs(self.current_price - self.last_price) / self.current_price >= 0.05
        if self.refetch_markets: self.last_price = self.current_price

    def filter_markets(self):
        markets = [ele["symbol"] for ele in self.client.get_tickers(category="option", baseCoin=self.token)["result"]["list"]]

        date_format = "%d%b%y"
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
                s = f"{self.token}-{dt.strftime('%-d%b%y')}-{strike}-".upper()
                options_pairs.append((s+"C", s+"P"))
        return options_pairs
    
    def get_prices(self, symbol, perp=False):
        if perp:
            response = self.client.get_orderbook(symbol=symbol, category="linear")["result"]
        else:
            response = self.client.get_orderbook(symbol=symbol, category="option")["result"]
        long = float(response["b"][0][0])
        short = float(response["a"][0][0])
        return long, short
    
    def sendmessage(self, message):
        bot_token = "2077936209:AAEIKNeABZxQ7Xpwec9BZZ8xmIePbbQpEnk"
        bot_chatID = '-660370327'
        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + message
        requests.get(send_text)

    def place_option_pair_orders(self, symbol, side, size, limit_price, put_price):
        side = side.capitalize()
        put_symbol = symbol[:-1] + "P"
        put_side = "Sell" if side == "Buy" else "Buy"

        order_list = [
                        {"symbol": symbol, "side": side, "size": str(size), "price": str(limit_price)},
                        {"symbol": put_symbol, "side": put_side, "size": str(size), "price": str(put_price)}
                    ]
        return self.create_option_batch_order(order_list=order_list)
    
    def create_option_batch_order(self, order_list):
        order_info = [{"symbol": order["symbol"], "side": order["side"], "orderType": "Limit", "qty": order["size"], "price": order["price"], "orderLinkId": str(time.time()), "timeInForce": "GTC"} 
                      for order in order_list
                      ]
        return self.client.place_batch_order(category="option", request=order_info)
    
    def create_order(self, symbol, side, size, price):
        side = side.capitalize()
        if symbol[-2:] in ["-P", "-C"]:
            category = "option"
        else:
            category = "linear"
        order_info = {"category":category, "symbol": symbol, "side": str(side), "orderType": "Limit", "qty": str(size), "price": str(price), "orderLinkId": str(time.time()), "timeInForce": "GTC"}
        if "PERP" in symbol:
        # USDC PERP are not available on API V5. Have to use V3 api
            order_info["timeInForce"] = "GoodTillCancel"
            return self.client._submit_request(
                method="POST",
                path=f"{self.client.endpoint}/contract/v3/private/order/create",
                query=order_info,
                auth=True,
        )
        return self.client.place_order(**order_info)

    def check_arb(self, pair):
        msg = ""
        strike = int(pair[0].split("-")[2])
        long_call, short_call = self.get_prices(pair[0])
        long_put, short_put = self.get_prices(pair[1])
        long_perp, short_perp = self.get_prices(f"{self.token}PERP", True)

        long_call_profit = short_put + short_perp - long_call - strike
        short_call_profit = short_call + strike - long_put - long_perp
        self.best_opp = max(long_call_profit, short_call_profit)
        if short_call_profit > self.threshold:
            if self.place_order:
                self.place_perp_order("Buy", self.base_size, long_perp)
                self.place_option_pair_orders(pair[0], "Sell", self.base_size, short_call, long_put)
                self.stop = True
            msg+=f"Long PERP @ {long_perp}\n"
            msg+=f"Long '{pair[1]}' @ {long_put}\n"
            msg+=f"Short '{pair[0]}' @ {short_call}\n"
            msg+=f"Profit: {short_call_profit}"
            
        if long_call_profit > self.threshold:
            if self.place_order:
                self.place_perp_order("Sell", self.base_size, short_perp)
                self.place_option_pair_orders(pair[0], "Buy", self.base_size, long_call, short_put)
                self.stop = True
            msg+=f"Short PERP @ {short_perp}\n"
            msg+=f"Short '{pair[1]}' @ {short_put}\n"
            msg+=f"Long '{pair[0]}' @ {long_call}\n"
            msg+=f"Profit: {long_call_profit}"
        if len(msg):
            print(msg + "\n")
            # if self.place_order: self.sendmessage(msg)
    
    def str_to_datetime(self, str):
        y, m, d = str.split("/")
        return datetime(int(y), int(m), int(d))

    def run(self):
        self.stop = False
        self.status_update()
        self.threshold = self.transaction_fee * self.current_price + self.dollar_threshold
        captured_errors = (KeyError, IndexError)
        for pair in self.options:
            try:
                if not self.stop: self.check_arb(pair)
            except captured_errors:
                pass
        print(f"Arbitrage Threshold: ${round(self.threshold,2)} | Current Best: {self.best_opp}", end="\r")
