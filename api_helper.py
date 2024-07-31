from NorenRestApiPy.NorenApi import NorenApi
from threading import Timer
import pandas as pd
import time
import concurrent.futures
import pyotp
from datetime import datetime,timedelta

api = None


class Order:
    def __init__(self, buy_or_sell: str = None, product_type: str = None,
                 exchange: str = None, tradingsymbol: str = None,
                 price_type: str = None, quantity: int = None,
                 price: float = None, trigger_price: float = None, discloseqty: int = 0,
                 retention: str = 'DAY', remarks: str = "tag",
                 order_id: str = None):
        self.buy_or_sell = buy_or_sell
        self.product_type = product_type
        self.exchange = exchange
        self.tradingsymbol = tradingsymbol
        self.quantity = quantity
        self.discloseqty = discloseqty
        self.price_type = price_type
        self.price = price
        self.trigger_price = trigger_price
        self.retention = retention
        self.remarks = remarks
        self.order_id = None


# print(ret)


def get_time(time_string):
    data = time.strptime(time_string, '%d-%m-%Y %H:%M:%S')

    return time.mktime(data)



class ShoonyaApiPy(NorenApi):
    def __init__(self):
        NorenApi.__init__(self, host='https://api.shoonya.com/NorenWClientTP/',
                          websocket='wss://api.shoonya.com/NorenWSTP/')
        global api
        api = self

    def generateTotp(self,token):
        totp = pyotp.TOTP(token)
        return totp.now()

    def place_basket(self, orders):

        resp_err = 0
        resp_ok = 0
        result = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:

            future_to_url = {executor.submit(self.place_order, order): order for order in orders}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
            try:
                result.append(future.result())
            except Exception as exc:
                print(exc)
                resp_err = resp_err + 1
            else:
                resp_ok = resp_ok + 1

        return result

    # def placeOrder(self, order: Order):
    #     ret = NorenApi.place_order(self, buy_or_sell=order.buy_or_sell, product_type=order.product_type,
    #                                exchange=order.exchange, tradingsymbol=order.tradingsymbol,
    #                                quantity=order.quantity, discloseqty=order.discloseqty, price_type=order.price_type,
    #                                price=order.price, trigger_price=order.trigger_price,
    #                                retention=order.retention, remarks=order.remarks)
    #     # print(ret)
    #
    #     return ret
    def placeOrder(self, tt, qty, type, price, tgPrice, tag, symbol):
        print("Placing order ------ ",tt,qty,price,tgPrice,symbol)
        order = api.place_order(buy_or_sell=tt, product_type='I',
                                exchange='NFO', tradingsymbol=symbol,
                                quantity=qty, discloseqty=0, price_type=type, price=price, trigger_price=tgPrice,
                                retention='DAY', remarks="LONG")
        return order

    def fetchHistData(self, token, tf):
        lastBusDay = datetime.today() - timedelta(days=4)
        lastBusDay = lastBusDay.replace(hour=9, minute=15, second=0, microsecond=0)
        dailyPrice = api.get_time_price_series(exchange='NSE', token=token, starttime=lastBusDay.timestamp(),
                                               interval=tf)

        df = pd.DataFrame(dailyPrice)

        columns_to_convert = ["into", "inth", "intl", "intc", "intvwap"]
        for column in columns_to_convert:
            try:
                df[column] = pd.to_numeric(df[column])
            except KeyError:
                pass
        df = df.loc[::-1]
        try:
            df['5ema'] = df.ta.ema(close=df['intc'], length=5)
            df['15ema'] = df.ta.ema(close=df['intc'], length=15)
            df['15sma'] = df.ta.sma(close=df['intc'], length=15)
            df = df.dropna()
        except:
            print("EXception occured with dataframe object from API_HELPER", df)
        return df


    def searchSymbol(self, text):
        r = api.searchscrip(exchange='NFO', searchtext=text)
        return (r)