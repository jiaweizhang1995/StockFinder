import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import tushare as ts
import pandas as pd
import numpy as np
import time
from tqdm import tqdm

start_time = time.time()

# 请在这里填写你的 Tushare Pro API Token
ts.set_token('384d879841d7a6f1a2d238ce2561a58de3de4ed1493d6b07772dc371')
pro = ts.pro_api()

sender_email = 'zhangjiawei0755@163.com'
sender_password = 'VUHIPWHHZHIPEGCQ'

# 接收邮件的邮箱
receiver_email = 'jiawei.zhang95@outlook.com'

# 邮件标题和内容
today_date = pd.Timestamp.now().strftime('%Y-%m-%d')
mail_subject = today_date + ' 突破年线股票观察池'
mail_body = f'观察池列表：\n筛选条件：今日收盘价高于250日均线且高于今日开盘价，近15个交易日收盘价低于250日均线，今日成交量高于30日平均成交量的股票。\n'

# 获取所有A股股票列表
stocks = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry')

# 获取股票代码列表
codes = stocks['ts_code'].tolist()[0:500]

# 记录股票总数
total_stocks = len(codes)

print(f"总共有: {total_stocks}只股票")


print(pd.__version__)

# 获取当前日期
today = pd.Timestamp.now().strftime('%Y%m%d')

# 初始化观察池
watch_pool = []

# 遍历所有股票
for index, code in enumerate(tqdm(codes)):
    # 获取股票的日线数据
    try:
        df = pro.daily(ts_code=code, end_date=today, limit=260, timeout=60)
    except Exception as e:
        print(f"Error occurred for {code}: {e}")
        continue

    # 如果数据不足250个交易日，从观察池中删除该股票
    if len(df) < 250:
        continue

    # 检查数据是否为空
    if df.empty:
        continue

    # 计算250日平均收盘价和30日平均成交量
    avg_250_days = df['close'][1:250].mean()
    avg_30_days = df['close'][1:30].mean()
    avg_30_volume = df['vol'][1:30].mean()

    # 获取昨日收盘价和成交量
    today_close = df.iloc[0]['close']
    today_vol = df.iloc[0]['vol']
    today_open = df.iloc[0]['open']

    # 检查是否符合突破条件
    if today_close > avg_250_days and today_close > today_open and np.all(
            df['close'][1:15] < avg_250_days) and today_vol > avg_30_volume:
        daily_increase = (today_close - df.iloc[1]['close']) / df.iloc[1]['close']
        daily_increase_percent = f"{daily_increase:.2%}"
        today_turnover_rate = df.iloc[0]['turnover_rate']
        watch_pool.append(
            {'ts_code': code, 'name': stocks.loc[index, 'name'], 'industry': stocks.loc[index, 'industry'],
             'today_close': today_close, 'avg_250_days': avg_250_days, 'increase': daily_increase_percent,
             'turnover_rate': today_turnover_rate})

# 转换观察池为 DataFrame
watch_pool_df = pd.DataFrame(watch_pool)

# 保存观察池为 Excel 文件
watch_pool_filename = f"{today_date} 观察池.xlsx"
watch_pool_df.to_excel(watch_pool_filename, index=False)

# 创建邮件对象
message = MIMEMultipart()
message["From"] = sender_email
message["To"] = receiver_email
message["Subject"] = mail_subject

# 邮件内容
message.attach(MIMEText(mail_body, "plain"))

# 添加 Excel 文件附件
with open(watch_pool_filename, "rb") as attachment:
    file_base = MIMEBase("application", "vnd.ms-excel")
    file_base.set_payload(attachment.read())

encoders.encode_base64(file_base)
file_base.add_header("Content-Disposition", f"attachment; filename= {watch_pool_filename}")
message.attach(file_base)


# 发送邮件
try:
    server = smtplib.SMTP_SSL("smtp.163.com", 465)
    server.login(sender_email, sender_password)
    server.sendmail(sender_email, receiver_email, message.as_string())
    server.quit()
    print(f"邮件已发送至 {receiver_email}")
except Exception as e:
    print(f"邮件发送失败: {e}")

print("--- %s seconds ---" % (time.time() - start_time))


