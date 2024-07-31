_I='3m_change%'
_H='returns'
_G='returns90'
_F='merged_instruments.csv'
_E='CMP-20DMA_%'
_D='symbol'
_C='NSE'
_B='20DMA'
_A='intc'
import math
from datetime import datetime,timedelta
import os,urllib,pandas as pd,yaml,json
from dbWrapper import*
from api_helper import ShoonyaApiPy
today=datetime.now()
def multiUserLogin():
	for cred in os.listdir('./creds'):
		print('multiUserLogin --',cred);path='./creds/'+cred
		with open(path)as f:credLoaded=yaml.load(f,Loader=yaml.FullLoader);print(credLoaded)
		user=ShoonyaApiPy();user.login(userid=credLoaded['user'],password=credLoaded['pwd'],twoFA=user.generateTotp(credLoaded['factor2']),vendor_code=credLoaded['vc'],api_secret=credLoaded['apikey'],imei=credLoaded['imei']);user_list.append(user)
	print('Login List ',user_list);return user_list
def fetchHistData(user,token,prevDays):
	lastBusDay=today-timedelta(days=prevDays);lastBusDay=lastBusDay.replace(hour=9,minute=15,second=0,microsecond=0);dailyPrice=user.get_time_price_series(exchange=_C,token=str(token),starttime=lastBusDay.timestamp(),interval=240);df=pd.DataFrame(dailyPrice);columns_to_convert=['into','inth','intl',_A,'intvwap']
	for column in columns_to_convert:
		try:df[column]=pd.to_numeric(df[column])
		except KeyError:pass
	df=df.loc[::-1];df=df.dropna();return df
def searchSymbol(text):api=user_list[0];r=api.searchscrip(exchange=_C,searchtext=text);print(len(r['values']))
def loadScripMaster():nseDf=pd.read_csv('NSE_symbols.csv');bseDf=pd.read_csv('BSE_symbols.csv');df1=pd.read_excel('Final ETF List.xlsx');df=pd.concat([nseDf,bseDf]);df.to_csv(_F)
def filterEtfs():print('filterEtfs');df=pd.read_csv(_F);df=df[df['Symbol'].str.contains('ETF')];return df
def getTopEtfs(etfDf):
	A='Token';api=user_list[0];result_list=[];tokens=etfDf[A];print(tokens.values)
	for i in tokens.values:
		try:histData90=fetchHistData(api,i,90);opening90=histData90.iloc[1][_A];opening1=histData90.iloc[-3][_A];closing=histData90.iloc[-1][_A];returns90=(closing-opening90)*100/opening90;returns=(closing-opening1)*100/opening1;symbol=etfDf.loc[etfDf[A]==i,'TradingSymbol'];print('Fetching for ',symbol,i);result_list.append({_D:symbol,'token':i,'opening':opening90,'closing':closing,'prevClose':opening1,_G:returns90,_H:returns})
		except:print('Error with --',i)
	resultDf=pd.DataFrame(result_list);resultDf.to_csv('final_returns-etf.csv');return resultDf
def getPerformingEtfs(filteredEtfs):newEtfs=filteredEtfs.loc[filteredEtfs[_G]>10];newEtfs=newEtfs.sort_values(_H,ascending=True);newEtfs.to_csv('1111final_returns-etf.csv')
def getTopEtfsFromNse():return nsefetch('https://www.nseindia.com/api/etf')
def nsefetch(payload):
	curl_headers=' -H "authority: beta.nseindia.com" -H "cache-control: max-age=0" -H "dnt: 1" -H "upgrade-insecure-requests: 1" -H "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36" -H "sec-fetch-user: ?1" -H "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9" -H "sec-fetch-site: none" -H "sec-fetch-mode: navigate" -H "accept-encoding: gzip, deflate, br" -H "accept-language: en-US,en;q=0.9,hi;q=0.8" --compressed'
	if'%26'in payload or'%20'in payload:encoded_url=payload
	else:encoded_url=urllib.parse.quote(payload,safe=':/?&=')
	payload_var='curl -b cookies.txt "'+encoded_url+'"'+curl_headers+''
	try:output=os.popen(payload_var).read();output=json.loads(output)
	except ValueError:payload2='https://www.nseindia.com';output2=os.popen('curl -c cookies.txt "'+payload2+'"'+curl_headers+'').read();output=os.popen(payload_var).read();output=json.loads(output)
	return output['data']
def flattenList(filteredEtfs):
	for item in filteredEtfs:meta_data=item.pop('meta');item.update(meta_data)
	return filteredEtfs
def findTradableEtf(df):newEtfs=df.loc[df[_I]>10];newEtfs=newEtfs.sort_values(_E,ascending=True);newEtfs.to_csv('nse-etf.csv');return newEtfs
def placeTrades(topFive):
	print('Top Five :- ',topFive)
	for(index,row)in topFive.iterrows():symbol=row[_D];tradingSymbol=symbol+'-EQ';ltp=float(row['ltP']);qty=math.floor(investmentAmount/ltp);print(symbol,qty);api=user_list[0];order=api.place_order(buy_or_sell='B',product_type='C',exchange=_C,tradingsymbol=tradingSymbol,quantity=qty,discloseqty=0,price_type='MKT',price=.0,retention='DAY',remarks='my_order_etf');print(order);print('Inserting to DB')
	return True
def get20DmaValueForEtf(data):A='CH_CLOSING_PRICE';df=pd.DataFrame(data);df[A]=pd.to_numeric(df[A],errors='coerce');df=df.sort_values(by='CH_TIMESTAMP');df[_B]=df[A].rolling(window=20).mean();latest_20dma=df[_B].iloc[-1];return latest_20dma,df[A].iloc[0]
def addTwentyDmaData(df):
	A='CMP-20DMA'
	for(index,row)in df.iterrows():
		symbol=row[_D];ltp=float(row['ltP']);histBaseUrl='https://www.nseindia.com/api/historical/cm/equity?symbol=';histContinueUrl='&from=23-04-2024&to=23-07-2024';finalUrl=histBaseUrl+symbol+histContinueUrl;print(finalUrl)
		try:oneMonthHistoricalData=nsefetch(finalUrl);latest20Dma,threeMonthClose=get20DmaValueForEtf(oneMonthHistoricalData)
		except:print('Error during ->',symbol)
		print(symbol,latest20Dma,ltp,threeMonthClose)
		if latest20Dma is not None:df.at[index,_B]=latest20Dma;df.at[index,A]=ltp-latest20Dma;df.at[index,_E]=(ltp-latest20Dma)/ltp*100;df.at[index,_I]=(ltp-threeMonthClose)/threeMonthClose*100
		else:df.at[index,_B]=.0;df.at[index,A]=.0;df.at[index,_E]=.0
	return df
if __name__=='__main__':print('Init -- Logging in users ');investmentAmount=500;user_list=[];usersListed=multiUserLogin();filteredEtfs=getTopEtfsFromNse();print(filteredEtfs);filteredEtfs=flattenList(filteredEtfs);df=pd.DataFrame(filteredEtfs);newDf=addTwentyDmaData(df);result=findTradableEtf(newDf);placeTrades(result.head(1))