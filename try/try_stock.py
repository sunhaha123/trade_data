import tushare as ts
import os
from dotenv import load_dotenv

load_dotenv()
pro = ts.pro_api()

df = pro.stk_mins(ts_code='002602.SZ', freq='1min', start_date='2025-10-01 09:00:00', end_date='2025-10-31 19:00:00')
print(df.head(10))
df.to_csv('./output/002602.SZ_202510.csv', index=False)
