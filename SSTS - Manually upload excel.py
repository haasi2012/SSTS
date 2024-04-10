#!/usr/bin/env python
# coding: utf-8

# In[1]:


import warnings
import pandas as pd
import requests
from datetime import datetime, timedelta
import json
import time
import requests

warnings.filterwarnings("ignore")
pd.set_option('display.max_columns', None)


# In[2]:


# Read csv file

df = pd.read_csv(r"F:\Markets\SSTS\Promoters.csv", skipinitialspace = True)
df.columns = df.columns.str.strip() #remove blank from column headers
df.tail()


# In[3]:


#Replace blank values with 0
df["VALUE OF SECURITY (ACQUIRED/DISPLOSED)"].replace({"-":0}, inplace=True)

#Convert to pandas dataframe
df["VALUE OF SECURITY (ACQUIRED/DISPLOSED)"] = pd.to_numeric(df["VALUE OF SECURITY (ACQUIRED/DISPLOSED)"])
df["NO. OF SECURITIES (ACQUIRED/DISPLOSED)"] = pd.to_numeric(df["NO. OF SECURITIES (ACQUIRED/DISPLOSED)"])
df.tail()


# In[4]:


#Consider only Promoters or Promoter Group for anlysis that too when they do Market Purchase
personCat = ['Promoters','Promoter Group']
df = df[df["MODE OF ACQUISITION"] == 'Market Purchase']
df = df[df["CATEGORY OF PERSON"].isin(personCat)]
df.head()


# In[5]:


#DATE OF ALLOTMENT/ACQUISITION - handle any errors that may occur during conversion by ignoring them.
df["DATE OF ALLOTMENT/ACQUISITION FROM"] = pd.to_datetime(df["DATE OF ALLOTMENT/ACQUISITION FROM"], format='%d-%m-%Y', errors='ignore')


# In[6]:


#Group the data to do a Consolidation bsaed on VALUE OF SECURITY, NO. OF SECURITIES and DATE OF ALLOTMENT/ACQUISITION
df1 = df.groupby(["SYMBOL"]).agg({'VALUE OF SECURITY (ACQUIRED/DISPLOSED)':'sum','NO. OF SECURITIES (ACQUIRED/DISPLOSED)':'sum','DATE OF ALLOTMENT/ACQUISITION FROM':'max'}).reset_index()


# In[7]:


# Buying value is Total VALUE OF SECURITY / NO. OF SECURITIES
df1['BuyValue'] = df1["VALUE OF SECURITY (ACQUIRED/DISPLOSED)"]/df1["NO. OF SECURITIES (ACQUIRED/DISPLOSED)"]
df1


# In[8]:


# 1Cr is the VALUE OF SECURITY (ACQUIRED/DISPLOSED) to be considered
df1 = df1[df1["VALUE OF SECURITY (ACQUIRED/DISPLOSED)"] > 100000000]
df1


# In[9]:


df1.sort_values(by='DATE OF ALLOTMENT/ACQUISITION FROM', ascending = False)


# In[10]:


# No. of stocks identified
len(df1)


# In[11]:


# Specify the full path where you want to save the file
file_path = r"F:\Markets\SSTS\SSTS_manual_daily_sheet.xlsx"

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

