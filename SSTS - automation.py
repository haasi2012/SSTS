#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import warnings
import pandas as pd
import requests
from datetime import datetime, timedelta
import json
import time
import requests

warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)

fromdate = datetime.now().strftime('%d-%m-%Y')
todate = datetime.today() - timedelta(days=120)  # 4 months of timeframe is being considered
enddate = datetime.strftime(todate, '%d-%m-%Y')

# Define the headers
headers = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cookie': '_ga=GA1.1.1830975394.1712109392; _abck=559D60DF39649528037633BE6D0D26A4~0~YAAQN0YDF6QY7p6OAQAAMgo2wwsnx8gTCS2Cp8ffXl+0pJa+A592DeRCUA8W+8clyXl8HQspGwDztZj2rHCUzgxlYu22PptSes17sR5ckZ8LxOf7hwYS+q+tc1o66Wh1OdwJeYTVUBKZEuKyAQNLgz1cGx7xatFg1ZReOHbRQF9StxiPgSBsnPBme2+cL7lqcxwrRJPxcjpHTQgIpU6hPyy8SZUzhSBLznvRtPtbnlSBtrIDzeAKwc1QBrvYAW6eJKmdE/TN24ZuFM29Ft0ghHQKLc570JAGFSH3RCaw33mF0P8ZMOIv3ynqQkCcggcAKfE539O2Ij6Zc1//tnRxCN3YW+VnOTbBPmwesXndlZPVAIXKmEv+N/csF3JKYubM2MkfC6o++CeoWYI6zYKqbtVuGCFse5EOsMc=~-1~-1~-1; defaultLang=en; nsit=_Zkx56Pd8_-_m4g0CygszDRK; nseappid=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJhcGkubnNlIiwiYXVkIjoiYXBpLm5zZSIsImlhdCI6MTcxMjY3MjI2NSwiZXhwIjoxNzEyNjc5NDY1fQ.Oe6DuBmDqCkfDWHx486zLUiG4EpQyvXH6KEgygJw9eQ; bm_mi=0B2A68B74E1CEF7275797632148B758F~YAAQN0YDF3857p6OAQAAKdY4wxfLZG/RFBleagN8+iRrdOp//fn3vHqmsUfemEgm3U5FoehSVgwFTswum7NmUNRjrLWMtFMC9NIHyoNGqD4Lul60FRq8tF8CWFMQ4zd3VZtQNU3DtmebQF2s2SsX55Pg4QWYCO+qEPVoHM4GAn+HrCaU4IYyhNvT/cLmSws/EuijnO7IknZpVNXNeswWyx7RuiCy2ZNjr3RhkzwT7O+DSwFfMBBZTeuhh9bzmq5KV8QWE83f3iKTxyM2I7FNa9le0cewFnA1f+fESEnnj+kwsDaF3UNH2PDfGZCw6VqcqabYjva5xjEBj0ObiJIzyg5k4SXJBOfB~1; bm_sz=675C1E70E156D963368D76DF472D0D23~YAAQN0YDF4E57p6OAQAAsNs4wxetsCldrsTeg1YMyTZW8ssYXtP99Ub+hrfTOSrau/iAsoWYkGbbkq8FMF9Y963P0lVGao2+pPNygzQpZnuwMhXfBcspUyKPQF9ijbZSLdpbyanaiu874WAUbmO/A12BVJ7HgvSeqbwokyjBy6ycoPv8IvENCbaN3SmPlZiBD2BRojgs2hcjBSoIIuwrQmylTvImp3kpEBqYKPxqlFX+jN7bk/wb8tYmBueLeWo7ZlgRlXA10OVXRb0jQ2VcoDK9kRaeLbwlwWgcQcRMVjLU/DDAuwwukRxL2PmEPQCNQSyNJGVvK7c/LWXwWUUR6V853E+syc6JouHm0mI4Fm9AuSD1J3Te1YOHF7kLMh0klwhiO1lyyGlxw36FBk9lnYxAWopKycIiBwdWGQnhsqgwaTjHoYKZq9Fr+hqMNdDsQO8OekU/84n864g9nxO5Lh8iDjPgg0IYgLuFow2reqtLCSiBGGPpFlgiPskvI4c6iF4ecCjwOTDB; _ga_QJZ4447QD3=GS1.1.1712672265.22.0.1712674368.0.0.0; _ga_87M7PJ3R97=GS1.1.1712672083.26.1.1712674368.0.0.0; RT="z=1&dm=nseindia.com&si=b8dddc7e-4b08-4f64-8709-e309209b72ba&ss=lusgqjq2&sl=0&se=8c&tt=0&bcn=%2F%2F684d0d46.akstat.io%2F"; bm_sv=3C1FBC8A975FEED5F5013855BDB487B8~YAAQN0YDF7sd8J6OAQAA2Ddtwxfv5AAH3P4LYAS2J6D3av0iwAyU+BslUj1edt5U/DH6Nn1ZvdyqvuC1bXLmtjQ1nYifAy6M8lzHYW2jdSs97wlo2IDjHyMEzzDyf+HJICGWWQkwIJrTSeCTPIzy9+vfiX1m8ViVr2S08QzLSeexXqHj+5SOthc2bEY6WDR0z2N42f6wtmK8yw4dzLK4ttffPy/D/ZW3c4KD0kOyv5QMRTCu9I3hHRAJOMjw/dsR52ADLw==~1; ak_bmsc=05AB08F1039B3B2CAAA594285E5BACA8~000000000000000000000000000000~YAAQN0YDF7M57p6OAQAAsNs4wxetsCldrsTeg1YMyTZW8ssYXtP99Ub+hrfTOSrau/iAsoWYkGbbkq8FMF9Y963P0lVGao2+pPNygzQpZnuwMhXfBcspUyKPQF9ijbZSLdpbyanaiu874WAUbmO/A12BVJ7HgvSeqbwokyjBy6ycoPv8IvENCbaN3SmPlZiBD2BRojgs2hcjBSoIIuwrQmylTvImp3kpEBqYKPxqlFX+jN7bk/wb8tYmBueLeWo7ZlgRlXA10OVXRb0jQ2VcoDK9kRaeLbwlwWgcQcRMVjLU/DDAuwwukRxL2PmEPQCNQSyNJGVvK7c/LWXwWUUR6V853E+syc6JouHm0mI4Fm9AuSD1J3Te1YOHF7kLMh0klwhiO1lyyGlxw36FBk9lnYxAWopKycIiBwdWGQnhsqgwaTjHoYKZq9Fr+hqMNdDsQO8OekU/84n864g9nxO5Lh8iDjPgg0IYgLuFow2reqtLCSiBGGPpFlgiPskvI4c6iF4ecCjwOTDB',
    'Referer': 'https://www.nseindia.com/companies-listing/corporate-filings-insider-trading',
    'Sec-Ch-Ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
}

# Define the URL
URL = 'https://www.nseindia.com/api/corporates-pit?index=equities&from_date=' + enddate + '&to_date=' + fromdate

# Send GET request
response = requests.get(URL, headers=headers)

# Check if request was successful
if response.status_code == 200:
    data = response.json()  # Parse response JSON
    # Process data further as needed
else:
    print("Failed to fetch data. Status code:", response.status_code)


# In[ ]:


d = requests.get(URL, headers=headers).json()

#Convert to pandas dataframe
df = pd.DataFrame(d['data'])
df.head()


# In[ ]:


#Replace blank values with 0
df["secVal"].replace({"-":0}, inplace=True)

#Convert to pandas dataframe
df["secVal"] = pd.to_numeric(df["secVal"])
df["secAcq"] = pd.to_numeric(df["secAcq"])
df.tail()


# In[ ]:


#Consider only Promoters or Promoter Group for anlysis that too when they do Market Purchase
personCat = ['Promoters','Promoter Group']
df = df[df['acqMode'] == 'Market Purchase']
df = df[df['personCategory'].isin(personCat)]
df


# In[ ]:


#DATE OF ALLOTMENT/ACQUISITION - handle any errors that may occur during conversion by ignoring them.
df['acqfromDt'] = pd.to_datetime(df['acqfromDt'], format='%d-%m-%Y', errors='ignore')


# In[ ]:


#Group the data to do a Consolidation bsaed on VALUE OF SECURITY, NO. OF SECURITIES and DATE OF ALLOTMENT/ACQUISITION
df1 = df.groupby(['symbol']).agg({'secVal':'sum','secAcq':'sum','acqfromDt':'max'}).reset_index()


# In[ ]:


# Buying value is Total VALUE OF SECURITY / NO. OF SECURITIES
df1['BuyValue'] = df1['secVal']/df1['secAcq']
df1


# In[ ]:


# 50Lakhs is the VALUE OF SECURITY (ACQUIRED/DISPLOSED) to be considered
df1 = df1[df1['secVal'] > 50000000]
df1


# In[ ]:


# Sort based on DATE OF ALLOTMENT/ACQUISITION
df1.sort_values(by='acqfromDt', ascending = False)
df1


# In[ ]:


# No. of stocks identified
len(df1)


# In[ ]:


# Specify the full path where you want to save the file
file_path = r"F:\Markets\SSTS\SSTS_daily_dump.xlsx"

# Write to Excel
with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
    df1.to_excel(writer, sheet_name='SSTS list', index=False)

    # Autofit column widths
    for sheet_name in writer.sheets:
        sheet = writer.sheets[sheet_name]
        for idx, col in enumerate(df.columns):
            series = df[col]
            max_len = max((
                series.astype(str).map(len).max(),
                len(str(series.name))
            )) + 1
            sheet.set_column(idx, idx, max_len)
            
print("Data has been saved to",file_path)


# In[ ]:




