import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import tushare as ts
import pandas as pd
import numpy as np
import time
import os
import threading
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from ratelimiter import RateLimiter
from concurrent.futures import as_completed
import datetime


start_time = time.time()

ts.set_token('384d879841d7a6f1a2d238ce2561a58de3de4ed1493d6b07772dc371')
pro = ts.pro_api()

sender_email = 'zhangjiawei0755@163.com'
sender_password = 'VUHIPWHHZHIPEGCQ'
receiver_email = 'jiawei.zhang95@outlook.com'
today_date = pd.Timestamp.now().strftime('%Y-%m-%d')
mail_subject = today_date + ' 突破年线股票观察池'
mail_body = f'观察池列表：\n筛选条件：今日收盘价高于250日均线且高于今日开盘价，近15个交易日收盘价低于250日均线，今日成交量高于30日平均成交量的股票。\n'

stocks = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry')
codes = stocks['ts_code'].tolist()
total_stocks = len(codes)

# 获取当前日期
today = pd.Timestamp.now().strftime('%Y%m%d')

print(f"总共有: {total_stocks}只股票")

pbar = tqdm(total=len(codes))

progress = 0
lock = threading.Lock()

rate_limiter = RateLimiter(max_calls=480, period=60)


def process_stock(code, stocks, rate_limiter):
    global progress
    with rate_limiter:
        cache_file = f"cache_{today_date}/{code}.csv"

        if not os.path.exists(f"cache_{today_date}/"):
            os.mkdir(f"cache_{today_date}/")

        if os.path.exists(cache_file):
            try:
                df = pd.read_csv(cache_file, on_bad_lines='skip')
            except pd.errors.EmptyDataError:
                return None
        else:
            try:
                df = pro.daily(ts_code=code, end_date=today, limit=260, timeout=60)
            except Exception as e:
                print(f"Error occurred for {code}: {e}")
                return None

            df.to_csv(cache_file, index=False)

        if len(df) < 250:
            return None

        if df.empty:
            return None

        avg_250_days = df['close'][1:250].mean()
        avg_30_days = df['close'][1:30].mean()
        avg_30_volume = df['vol'][1:30].mean()

        # 获取昨日收盘价和成交量
        today_close = df.iloc[0]['close']
        today_vol = df.iloc[0]['vol']
        today_open = df.iloc[0]['open']

        if today_close > avg_250_days and today_close > today_open and np.all(
                df['close'][1:30] < avg_250_days) and today_vol > avg_30_volume:
            daily_increase = (today_close - df.iloc[1]['close']) / df.iloc[1]['close']
            daily_increase_percent = f"{daily_increase:.2%}"
            stock_info = stocks[stocks['ts_code'] == code]
            stock_data = {'ts_code': code, 'name': stock_info.iloc[0]['name'],
                          'industry': stock_info.iloc[0]['industry'],
                          'today_close': today_close, 'avg_250_days': avg_250_days,
                          'increase': daily_increase_percent}
            return stock_data

        with lock:
            progress += 1

        return None


with ThreadPoolExecutor(max_workers=30) as executor:
    futures = {executor.submit(process_stock, code, stocks, rate_limiter): code for code in codes}

    for future in as_completed(futures):
        try:
            result = future.result()
            pbar.update(1)
        except Exception as e:
            print(f"Error occurred: {e}")

pbar.close()

watch_pool = []

for future in futures:
    result = future.result()
    if result is not None:
        watch_pool.append(result)
    pbar.update(1)

print(f"观察池大小：{len(watch_pool)}")

# 转换观察池为 DataFrame
watch_pool_df = pd.DataFrame(watch_pool)

# 选取涨幅前十的股票并保存为 Excel 文件

# 选取涨幅前十的股票并保存为 Excel 文件
timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
top10_filename = f"{today_date}_涨幅前十股票_{timestamp}.xlsx"
top10_df = watch_pool_df.sort_values(by='increase', ascending=False).head(20)
top10_df.to_excel(top10_filename, index=False, columns=['ts_code', 'name', 'industry', 'today_close', 'avg_250_days', 'increase'])


# 创建邮件对象
message = MIMEMultipart()
message["From"] = sender_email
message["To"] = receiver_email
message["Subject"] = mail_subject

# 邮件内容
message.attach(MIMEText(mail_body, "plain"))

# 添加涨幅前十 Excel 文件附件
with open(top10_filename, "rb") as attachment:
    top10_base = MIMEBase("application", "vnd.ms-excel")
    top10_base.set_payload(attachment.read())

encoders.encode_base64(top10_base)
top10_base.add_header("Content-Disposition", f"attachment; filename= {top10_filename}")
message.attach(top10_base)

# 发送邮件
try:
    # server = smtplib.SMTP_SSL("smtp.163.com", 465)
    # server.login(sender_email, sender_password)
    # server.sendmail(sender_email, receiver_email, message.as_string())
    # server.quit()
    print(f"邮件已发送至 {receiver_email}")
except Exception as e:
    print(f"邮件发送失败: {e}")

print("--- %s seconds ---" % (time.time() - start_time))
