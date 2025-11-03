import tushare as ts
import os
from dotenv import load_dotenv

load_dotenv()
pro = ts.pro_api()

data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

print(data.head(10))

print(len(data))

data.to_csv('/home/echo/workspace/trade_data/stock_data/stock_basic_listed.csv', index=False)
