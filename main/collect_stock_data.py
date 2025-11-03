import tushare as ts
import os
import pandas as pd
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading
import argparse

# 从 .env 文件加载环境变量
# 指定.env文件的路径，确保总能从项目根目录加载
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

# 初始化 tushare pro api
# 确保你的 .env 文件中有 TUSHARE_TOKEN=your_token
# 显式传递 token，若缺失则及时给出提示
token = os.getenv('TUSHARE_TOKEN')
if not token:
    raise ValueError('环境变量 TUSHARE_TOKEN 未设置，请在 .env 文件或环境中配置后再运行程序。')

ts.set_token(token)
pro = ts.pro_api(token)

# 定义常量
START_DATE = '20241031'
END_DATE = '20251031'
FREQ = '1min' # 数据频率
OUTPUT_DIR = '/home/echo/workspace/trade_data/stock_data'
INPUT_FILE = '/home/echo/workspace/trade_data/output/stock_basic_listed.csv'
MAX_WORKERS = 8 # 调整并发工作线程数
API_CALLS_PER_MINUTE = 500
TRADE_CAL_EXCHANGE = 'SSE'
TRADE_CAL_CHUNK_DAYS = 20

class APIRateLimiter:
    """
    一个简单的线程安全速率限制器。
    """
    def __init__(self, max_calls, period_seconds):
        self.max_calls = max_calls
        self.period_seconds = period_seconds
        self.lock = threading.Lock()
        self.call_timestamps = []

    def __enter__(self):
        with self.lock:
            while True:
                now = time.time()
                # 移除超出时间窗口的旧时间戳
                self.call_timestamps = [t for t in self.call_timestamps if t > now - self.period_seconds]
                
                if len(self.call_timestamps) < self.max_calls:
                    self.call_timestamps.append(now)
                    break
                
                # 计算需要等待的时间
                oldest_call_time = self.call_timestamps[0]
                wait_time = (oldest_call_time + self.period_seconds) - now
                if wait_time > 0:
                    # print(f"线程 {threading.get_ident()} 速率超限，等待 {wait_time:.2f} 秒...")
                    time.sleep(wait_time)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

# 创建一个全局的速率限制器实例
rate_limiter = APIRateLimiter(API_CALLS_PER_MINUTE, 60)

def compute_trade_date_chunks():
    """
    计算交易日区间列表，用于分批请求分钟级数据。
    """
    with rate_limiter:
        trade_cal_df = pro.trade_cal(
            exchange=TRADE_CAL_EXCHANGE,
            start_date=START_DATE,
            end_date=END_DATE,
            fields='cal_date,is_open'
        )

    if trade_cal_df is None or trade_cal_df.empty:
        return []

    open_dates = trade_cal_df.loc[trade_cal_df['is_open'] == 1, 'cal_date'].astype(str).tolist()
    open_dates.sort()

    chunks = []
    for i in range(0, len(open_dates), TRADE_CAL_CHUNK_DAYS):
        chunk_start = open_dates[i]
        chunk_end = open_dates[min(i + TRADE_CAL_CHUNK_DAYS - 1, len(open_dates) - 1)]
        chunks.append((chunk_start, chunk_end))

    return chunks


def fetch_and_save_stock_data(ts_code, trade_date_chunks):
    """
    获取给定股票代码的股票数据并将其保存到 CSV 文件。
    增加了防重爬机制。
    """
    # 防重爬机制
    file_name = f"{ts_code}_{START_DATE}_{END_DATE}_{FREQ}.csv"
    output_path = os.path.join(OUTPUT_DIR, file_name)
    if os.path.exists(output_path):
        print(f"文件 {output_path} 已存在，跳过 {ts_code}。")
        return f"Skipped: {ts_code}"

    if not trade_date_chunks:
        print(f"未能获取交易区间，跳过 {ts_code}。")
        return f"No calendar for {ts_code}"

    print(f"正在获取 {ts_code} 的数据...")
    frames = []

    try:
        for chunk_start, chunk_end in trade_date_chunks:
            with rate_limiter:
                chunk_df = ts.pro_bar(
                    ts_code=ts_code,
                    start_date=chunk_start,
                    end_date=chunk_end,
                    adj="qfq",
                    freq=FREQ,
                    asset='E'
                )

            if chunk_df is None or chunk_df.empty:
                continue

            frames.append(chunk_df)

        if not frames:
            print(f"未找到 {ts_code} 在指定日期范围内的数据。")
            return f"No data for {ts_code}"

        df = pd.concat(frames, ignore_index=True)

        # 对结果按时间排序并去重，保证数据完整性
        if 'trade_time' in df.columns:
            df = df.sort_values(by='trade_time')
        elif 'datetime' in df.columns:
            df = df.sort_values(by='datetime')
        df = df.drop_duplicates()

        # 保存到 CSV
        df.to_csv(output_path, index=False)
        print(f"已将 {ts_code} 的数据保存到 {output_path}")
        return f"Success: {ts_code}"
    except Exception as e:
        print(f"获取 {ts_code} 数据时出错: {e}")
        return f"Error: {ts_code}: {e}"

def main():
    """
    主函数，用于读取股票列表并使用线程池获取数据。
    增加了命令行参数，支持特选股票。
    """
    # 增加命令行参数解析
    parser = argparse.ArgumentParser(description="异步股票数据采集程序")
    parser.add_argument('--stocks', type=str, help='指定要采集的股票代码，用逗号分隔 (例如: 000001.SZ,600000.SH)')
    args = parser.parse_args()

    # 确保输出目录存在
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    ts_codes = []
    if args.stocks:
        # 如果指定了股票代码，则使用指定的列表
        ts_codes = [s.strip() for s in args.stocks.split(',')]
        print(f"将要采集指定的 {len(ts_codes)} 个股票代码。")
    else:
        # 否则从 CSV 文件读取股票代码
        try:
            stock_list_df = pd.read_csv(INPUT_FILE)
            # 假设股票代码在 'ts_code' 列
            ts_codes = stock_list_df['ts_code'].tolist()
            print(f"从 {INPUT_FILE} 加载了 {len(ts_codes)} 个股票代码。")
        except FileNotFoundError:
            print(f"错误: 输入文件 {INPUT_FILE} 未找到。")
            return
        except KeyError:
            print(f"错误: 输入文件 {INPUT_FILE} 中缺少 'ts_code' 列。")
            return

    if not ts_codes:
        print("没有要处理的股票代码。")
        return

    try:
        trade_date_chunks = compute_trade_date_chunks()
    except Exception as e:
        print(f"错误: 无法获取交易日历: {e}")
        return

    if not trade_date_chunks:
        print("错误: 未能获取有效的交易区间，无法采集数据。")
        return

    start_time = time.time()
    
    # 使用 ThreadPoolExecutor 进行并发下载
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_code = {executor.submit(fetch_and_save_stock_data, code, trade_date_chunks): code for code in ts_codes}
        
        for future in as_completed(future_to_code):
            code = future_to_code[future]
            try:
                result = future.result()
                print(result)
            except Exception as exc:
                print(f'{code} 生成了一个异常: {exc}')

    end_time = time.time()
    print(f"所有任务完成，总耗时: {end_time - start_time:.2f} 秒。")

if __name__ == "__main__":
    main()
