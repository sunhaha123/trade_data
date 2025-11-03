# A股复权行情
import tushare as ts
import os
from dotenv import load_dotenv
load_dotenv()


# ts_code	str	Y	证券代码
# start_date	str	N	开始日期 (格式：YYYYMMDD)
# end_date	str	N	结束日期 (格式：YYYYMMDD)
# asset	str	Y	资产类别：E股票 I沪深指数 C数字货币 FT期货 FD基金 O期权，默认E
# adj	str	N	复权类型(只针对股票)：None未复权 qfq前复权 hfq后复权 , 默认None
# freq	str	Y	数据频度 ：1MIN表示1分钟（1/5/15/30/60分钟） D日线 ，默认D
# ma	list	N	均线，支持任意周期的均价和均量，输入任意合理int数值

pro = ts.pro_api()
df = ts.pro_bar(ts_code='002602.SZ', freq='1min', start_date='20251031', end_date='20251103',
                   adj="qfq")
print(df.head(10))
df.to_csv('./output/002602.SZ_202510_2.csv', index=False)
