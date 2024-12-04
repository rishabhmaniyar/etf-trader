import os
import urllib.parse
import json
import pandas as pd

# List of ETF symbols
etf_symbols = [
    "GOLDBEES", "MAHKTECH", "METALIETF", "LIQUIDBEES", "SILVERBEES", "LIQUIDCASE", "NIFTYBEES",
    "ITBEES", "LTGILTBEES", "PHARMABEES", "TATAGOLD", "PVTBANIETF", "OILIETF", "CPSEETF",
    "GOLDIETF", "LOWVOLIETF", "HDFCGOLD", "SETFGOLD", "UTINIFTETF", "MOM30IETF", "MIDCAPETF",
    "ALPL30IETF", "PSUBNKBEES", "ALPHA", "AUTOIETF", "ALPHAETF", "TATSILV", "GROWWEV",
    "BANKBEES", "BANKIETF", "GOLD1", "SILVERIETF", "ICICIB22", "NV20IETF", "LIQUIDIETF",
    "PVTBANKADD", "GOLDCASE", "BSLNIFTY", "NEXT50IETF", "EVINDIA", "BFSI", "AXISGOLD",
    "MIDCAPIETF", "SMALLCAP", "MIDSMALL", "MOSMALL250", "LIQUIDADD", "HNGSNGBEES", "SETFNIF50",
    "LIQUID", "HDFCSML250", "MON100", "FMCGIETF", "NIFTYIETF", "MOM100", "MID150BEES",
    "BSE500IETF", "ITIETF", "ABSLPSE", "MAFANG", "MIDSELIETF", "ITETF", "MOREALTY",
    "MODEFENCE", "HDFCSILVER", "HDFCMID150", "NIF100IETF", "MONIFTY500", "HEALTHY",
    "HDFCMOMENT", "TOP100CASE", "GILT5YBEES", "NIFTYETF", "MID150CASE", "LIQUIDETF",
    "COMMOIETF", "AUTOBEES", "JUNIORBEES", "QUAL30IETF", "MOMENTUM50", "MOMENTUM", "FINIETF",
    "SILVER", "GOLDETF", "TOP10ADD", "MULTICAP", "CONSUMBEES", "MOMOMENTUM", "NIFTYQLITY",
    "SILVERETF", "GOLDSHARE", "HDFCNEXT50", "AXISILVER", "BSLGOLDETF", "INFRAIETF", "LOWVOL1",
    "MOVALUE", "BANKETF", "HDFCPVTBAN", "SETFNIFBK", "UTINEXT50", "UTIBANKETF", "TNIDETF",
    "PSUBNKIETF", "MASPTOP50", "AXISBPSETF", "HDFCNIFTY", "SILVER1", "GROWWLIQID", "EGOLD",
    "ESG", "SBISILVER", "HDFCNIF100", "MONQ50", "IT", "DIVOPPBEES", "GOLDETFADD", "ABSLNN50ET",
    "LIQUID1", "MOLOWVOL", "LICNETFGSC", "NIFTY1", "PSUBANK", "MNC", "QGOLDHALF",
    "CONSUMIETF", "ESILVER", "HEALTHIETF", "ITETFADD", "HDFCSENSEX", "BANKETFADD", "NIF100BEES",
    "MOHEALTH", "MAKEINDIA", "SBINEQWETF", "HDFCNIFBAN", "BANKNIFTY1", "HDFCNIFIT", "BANKBETF",
    "HDFCLOWVOL", "TECH", "HDFCBSE500", "SETFNN50", "SENSEXETF", "SILVRETF", "ABSLBANETF",
    "HDFCPSUBK", "LIQUIDSHRI", "CONS", "MIDCAP", "LICNETFN50", "NV20", "AXISNIFTY",
    "EQUAL50ADD", "UTISXN50", "AXSENSEX", "SETF10GILT", "SBIETFCON", "SBIETFIT", "HDFCQUAL",
    "INFRABEES", "EBBETF0430", "NEXT50", "NV20BEES", "EBANKNIFTY", "SILVERADD", "PSUBANKADD",
    "HDFCGROWTH", "NIFTY50ADD", "SENSEXIETF", "SBIETFQLTY", "SBIETFPB", "ABSLLIQUID",
    "EBBETF0431", "BSLSENETFG", "AXISBNKETF", "MOM50", "BBNPPGOLD", "LICNMID100", "HDFCVALUE",
    "LOWVOL", "AXISCETF", "SHARIABEES", "NPBET", "BBETF0432", "NIFTYBETF", "NETF",
    "EBBETF0425", "AXISHCETF", "MOQUALITY", "LIQUIDBETF", "LIQUIDSBI", "MIDQ50ADD",
    "GSEC5IETF"
]


# NSE Fetch function
def nsefetch(payload):
    curl_headers = ''' -H "authority: beta.nseindia.com" -H "cache-control: max-age=0" -H "dnt: 1" -H "upgrade-insecure-requests: 1" -H "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36" -H "sec-fetch-user: ?1" -H "accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9" -H "sec-fetch-site: none" -H "sec-fetch-mode: navigate" -H "accept-encoding: gzip, deflate, br" -H "accept-language: en-US,en;q=0.9,hi;q=0.8" --compressed'''

    encoded_url = urllib.parse.quote(payload, safe=':/?&=')
    payload_var = 'curl -b cookies.txt "' + encoded_url + '"' + curl_headers

    try:
        output = os.popen(payload_var).read()

        # Debugging: Print the output to inspect if it's valid
        print(f"API response for {payload}: {output}")

        # Check if the response is empty
        if not output:
            print("Empty response from API")
            return None

        # Try to load the JSON response
        output = json.loads(output)

    except json.JSONDecodeError as e:
        print(f"JSON decoding failed: {e}")
        print(f"Raw output: {output}")
        return None  # Return None or handle this case based on your requirements

    except Exception as e:
        print(f"Error during API call: {e}")
        return None

    return output


# Create an empty list to store the data
data = []

# Loop through each ETF symbol and fetch bid/ask data
for etf in etf_symbols:
    url = f"https://www.nseindia.com/api/quote-equity?symbol={etf}&section=trade_info"
    response = nsefetch(url)

    # Extract first bid and ask prices
    try:
        bid_price = response["marketDeptOrderBook"]["bid"][0]["price"]
        ask_price = response["marketDeptOrderBook"]["ask"][0]["price"]
    except (KeyError, IndexError):
        bid_price = None
        ask_price = None
        continue

    # Append the result as a dictionary
    data.append({
        "Symbol": etf,
        "Bid Price": bid_price,
        "Ask Price": ask_price
    })

# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(data)

# Export the dataframe to a CSV file
df.to_csv("etf_bid_ask_data1.csv", index=False)

print("Data fetched and exported to etf_bid_ask_data.csv")
