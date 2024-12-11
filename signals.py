import math
from datetime import datetime, timedelta
import os
import urllib
import pandas as pd
import yaml
import json
from dbWrapper import *

from api_helper import ShoonyaApiPy

today = datetime.now()


def multiUserLogin():
    for cred in os.listdir('./creds'):
        print("multiUserLogin --", cred)
        path = './creds/' + cred
        with open(path) as f:
            credLoaded = yaml.load(f, Loader=yaml.FullLoader)
            print(credLoaded)

        user = ShoonyaApiPy()
        user.login(userid=credLoaded['user'], password=credLoaded['pwd'],
                   twoFA=user.generateTotp(credLoaded['factor2']), vendor_code=credLoaded['vc'],
                   api_secret=credLoaded['apikey'], imei=credLoaded['imei'])
        user_list.append(user)

    print("Login List ", user_list)

    return user_list
    # user.placeOrder()


def fetchHistData(user, token, prevDays):
    lastBusDay = today - timedelta(days=prevDays)
    lastBusDay = lastBusDay.replace(hour=9, minute=15, second=0, microsecond=0)
    dailyPrice = user.get_time_price_series(exchange='NSE', token=str(token), starttime=lastBusDay.timestamp(),
                                            interval=240)

    df = pd.DataFrame(dailyPrice)

    columns_to_convert = ["into", "inth", "intl", "intc", "intvwap"]
    for column in columns_to_convert:
        try:
            df[column] = pd.to_numeric(df[column])
        except KeyError:
            pass
    df = df.loc[::-1]
    df = df.dropna()
    return df


def searchSymbol(text):
    api = user_list[0]
    r = api.searchscrip(exchange='NSE', searchtext=text)
    print(len(r['values']))


def loadScripMaster():
    nseDf = pd.read_csv("NSE_symbols.csv")
    bseDf = pd.read_csv("BSE_symbols.csv")
    df1 = pd.read_excel("Final ETF List.xlsx")
    df = (pd.concat([nseDf, bseDf]))
    df.to_csv("merged_instruments.csv")
    # print(df1.head(10))


# class PlaceOrder implements Thread :
#     void PlaceOrder(user)
#         this.user = user
#
#     void run(){
#         //place order here
#         this.user.placeOrder()
#
# }
# }
#
# for(user in user_list):
#
#     PlaceOrder x = new Thread(user)
#     x.run()

#     cred.yml
# list<user>
# for(each cred file in cred directory){
#     creds_user = yaml.load(f, Loader=yaml.FullLoader)
#     user = ShoonyaApiPy()
#     ret = user.login(userid=creds_user1['user'], password=creds_user1['pwd'],
#             twoFA=user1.generateTotp(creds_user1['factor2']), vendor_code=creds_user1['vc'],
#          api_secret=creds_user1['apikey'], imei=creds_user1['imei'])
#     user_list.add(user)
#
# for user in user_list :
#     Thread x= new Thread(user)
#     x.placeOrder()
# }


def filterEtfs():
    print("filterEtfs")
    df = pd.read_csv("merged_instruments.csv")
    df = df[df["Symbol"].str.contains("ETF")]
    return df


def getTopEtfs(etfDf):
    api = user_list[0]
    result_list = []
    tokens = etfDf["Token"]
    print(tokens.values)
    for i in tokens.values:
        try:
            histData90 = fetchHistData(api, i, 90)
            opening90 = histData90.iloc[1]['intc']
            opening1 = histData90.iloc[-3]['intc']
            closing = histData90.iloc[-1]['intc']
            returns90 = (closing - opening90) * 100 / opening90
            returns = (closing - opening1) * 100 / opening1
            symbol = etfDf.loc[etfDf['Token'] == i, 'TradingSymbol']
            print("Fetching for ", symbol, i)
            result_list.append(
                {"symbol": symbol, "token": i, "opening": opening90, "closing": closing, "prevClose": opening1,
                 "returns90": returns90, "returns": returns})
        except:
            print("Error with --", i)
    resultDf = pd.DataFrame(result_list)
    resultDf.to_csv("final_returns-etf.csv")
    return resultDf


def getPerformingEtfs(filteredEtfs):
    newEtfs = filteredEtfs.loc[filteredEtfs["returns90"] > 10]
    newEtfs = newEtfs.sort_values('returns', ascending=True)
    newEtfs.to_csv("1111final_returns-etf.csv")


def getTopEtfsFromNse():
    return nsefetch("https://www.nseindia.com/api/etf")


def nsefetch(payload):
    curl_headers = ''' -H "authority: beta.nseindia.com" -H "cache-control: max-age=0" -H "dnt: 1" -H "upgrade-insecure-requests: 1" -H "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36" -H "sec-fetch-user: ?1" -H "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9" -H "sec-fetch-site: none" -H "sec-fetch-mode: navigate" -H "accept-encoding: gzip, deflate, br" -H "accept-language: en-US,en;q=0.9,hi;q=0.8" --compressed'''
    if (("%26" in payload) or ("%20" in payload)):
        encoded_url = payload
    else:
        encoded_url = urllib.parse.quote(payload, safe=':/?&=')
    payload_var = 'curl -b cookies.txt "' + encoded_url + '"' + curl_headers + ''
    try:
        output = os.popen(payload_var).read()
        output = json.loads(output)
    except ValueError:  # includes simplejson.decoder.JSONDecodeError:
        payload2 = "https://www.nseindia.com"
        output2 = os.popen('curl -c cookies.txt "' + payload2 + '"' + curl_headers + '').read()
        output = os.popen(payload_var).read()
        output = json.loads(output)
    return output["data"]


def flattenList(filteredEtfs):
    for item in filteredEtfs:
        meta_data = item.pop('meta')
        item.update(meta_data)
    return filteredEtfs


def findTradableEtf(df):
    # df['perChange30d'] = df['perChange30d'].replace('-', 0.0)
    # df['perChange30d'] = pd.to_numeric(df['perChange30d'])
    # df['per'] = pd.to_numeric(df['per'])
    # df['ltP'] = pd.to_numeric(df['ltP'])
    newEtfs = df.loc[df['3m_change%'] > 10]
    newEtfs = newEtfs.sort_values('CMP-20DMA_%', ascending=True)
    newEtfs.to_csv("nse-etf.csv")
    return newEtfs


def placeTrades(topFive):
    print("Top Five :- ", topFive)
    for index, row in topFive.iterrows():
        symbol = row['symbol']
        tradingSymbol = symbol + "-EQ"
        ltp = float(row['ltP'])
        qty = math.floor(investmentAmount / ltp)
        print(symbol, qty)
        api = user_list[0]
        order = api.place_order(buy_or_sell='B', product_type='C',
                                exchange='NSE', tradingsymbol=tradingSymbol,
                                quantity=qty, discloseqty=0, price_type='MKT', price=0.0,
                                retention='DAY', remarks='my_order_etf')
        print(order)
        print("Skipping Inserting to DB")
        # print(api.get)
        # insert_buy_order(symbol, ltp, qty)
        # main()
    return True


def get20DmaValueForEtf(data):
    df = pd.DataFrame(data)

    # Ensure the CH_CLOSING_PRICE column is numeric
    df['CH_CLOSING_PRICE'] = pd.to_numeric(df['CH_CLOSING_PRICE'], errors='coerce')

    # Sort the DataFrame by date (CH_TIMESTAMP) in ascending order
    df = df.sort_values(by='CH_TIMESTAMP')

    # Calculate the 20-day moving average (DMA)
    df['20DMA'] = df['CH_CLOSING_PRICE'].rolling(window=20).mean()
    # print(df.tail(5))

    # Get the latest 20 DMA value
    latest_20dma = df['20DMA'].iloc[-1]

    return latest_20dma, df['CH_CLOSING_PRICE'].iloc[0]


def addTwentyDmaData(df):
    # Loopover each item fetch historical data from nse api
    ## Calculate 20 DMA (PandasTA) ; Ratio change ; Return sorted ascending
    for index, row in df.iterrows():
        symbol = row['symbol']
        ltp = float(row['ltP'])
        # Calculate today's date
        to_date = datetime.today()

        # Calculate the date three months before today
        from_date = to_date - timedelta(days=90)  # Approximately 3 months

        to_date_str = to_date.strftime("%d-%m-%Y")
        from_date_str = from_date.strftime("%d-%m-%Y")
        histBaseUrl = "https://www.nseindia.com/api/historical/cm/equity?symbol="
        histContinueUrl = f"&from={from_date_str}&to={to_date_str}"
        finalUrl = histBaseUrl + symbol + histContinueUrl
        print(finalUrl)

        try:
            oneMonthHistoricalData = nsefetch(finalUrl)
            (latest20Dma, threeMonthClose) = get20DmaValueForEtf(oneMonthHistoricalData)

        except:
            print("Error during ->", symbol)

        print(symbol, latest20Dma, ltp, threeMonthClose)

        if latest20Dma is not None:
            df.at[index, '20DMA'] = latest20Dma
            df.at[index, 'CMP-20DMA'] = ltp - latest20Dma
            df.at[index, 'CMP-20DMA_%'] = ((ltp - latest20Dma) / ltp) * 100
            df.at[index, '3m_change%'] = ((ltp - threeMonthClose) / threeMonthClose) * 100
        else:
            df.at[index, '20DMA'] = 0.0
            df.at[index, 'CMP-20DMA'] = 0.0
            df.at[index, 'CMP-20DMA_%'] = 0.0

        # df = df.dropna(subset=['20DMA'])
        # df = df.drop(['isMunicipalBond', 'quotepreopenstatus', 'industry', 'assets','tempSuspendedSeries'], axis=1)
        # print(symbol, df)

    return df


def squareOffHoldingsBreachingCutoff(cutOff):
    print("squareOffHoldingsBreachingCutoff", cutOff)
    api = user_list[0]
    holdings = api.get_holdings()
    for holding in holdings:
        quantity = float(holding['holdqty'])
        if quantity > 0.0:
            buyAveragePrice = float(holding['upldprc'])
            filteredExchangeSymbol = holding['exch_tsym'][0]
            tsym = filteredExchangeSymbol['tsym']
            ltp = float(api.get_quotes(exchange=filteredExchangeSymbol['exch'], token=filteredExchangeSymbol['token'])['lp'])
            returns = (ltp - buyAveragePrice) / buyAveragePrice
            if returns >= cutOff / 100:
                print("Selling for ", tsym, "returns :- ", returns, "Qty :- ", quantity)
                order = api.place_order(buy_or_sell='S', product_type='C',
                                        exchange='NSE', tradingsymbol=tsym,
                                        quantity=quantity, discloseqty=0, price_type='MKT', price=0.0,
                                        retention='DAY', remarks='my_squareOff_etf')
                print(order)
            else:
                print("Not Squaring off ",tsym)


if __name__ == "__main__":
    print("Init -- Logging in users ")
    investmentAmount = 500
    user_list = []
    usersListed = multiUserLogin()

    # searchSymbol("ETF")
    # etfDf = filterEtfs()
    # filteredEtfs = getTopEtfs(etfDf)

    filteredEtfs = getTopEtfsFromNse()
    print(filteredEtfs)
    filteredEtfs = flattenList(filteredEtfs)
    df = pd.DataFrame(filteredEtfs)
    # print(df)
    newDf = addTwentyDmaData(df)
    result = findTradableEtf(newDf)

    # result = pd.read_csv('nse-etf.csv')
    try:
        placeTrades(result.head(1))
    except Exception as ex:
        print("Error while placing order",ex)
    print("Executed placeTrades ; Now squareOffHoldingsBreachingCutoff ")
    squareOffHoldingsBreachingCutoff(5)
    # filteredEtfs=pd.read_csv("final_returns-etf.csv")
    # performingEtfs = getPerformingEtfs(filteredEtfs)
    # loadScripMaster()
