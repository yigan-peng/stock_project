# ==================== 导入依赖库 ====================
import akshare as ak                    # 第三方金融数据接口库，用于获取股票/行业资金流等数据
import pandas as pd                     # 数据处理库，用于解析表格数据、数值转换等
import json                             # JSON序列化/反序列化，用于数据库存储和API传输
import os                               # 操作系统接口，用于文件路径处理
import time                             # 时间相关函数，用于防刷冷却计时
import sqlite3                          # SQLite数据库驱动，用于持久化存储数据
import asyncio                          # 异步IO库，用于FastAPI的异步锁
import threading                        # 多线程库，用于后台分时采集线程
import requests                         # HTTP请求库，用于直接请求同花顺API
import py_mini_racer                    # JavaScript执行引擎，用于生成同花顺hexin-v验证码
from datetime import datetime, timedelta  # 日期时间处理，用于生成交易日期序列
from contextlib import asynccontextmanager  # 异步上下文管理器，用于FastAPI生命周期管理
from io import StringIO                 # 字符串转IO流，用于pd.read_html解析HTML表格
from fastapi import FastAPI             # Web框架，提供HTTP API服务
from fastapi.responses import FileResponse      # 文件响应，用于返回HTML页面
from starlette.responses import JSONResponse    # JSON响应，用于返回错误状态码
from fastapi.staticfiles import StaticFiles     # 静态文件服务，用于托管前端HTML/JS文件
from akshare.stock_feature.stock_fund_flow import _get_file_content_ths  # 获取同花顺JS验证码文件内容
import uvicorn                          # ASGI服务器，用于运行FastAPI应用

# ==================== 全局配置 ====================
DB_DAILY = "fund_flow.db"               # 每日数据数据库文件名（存储近20个交易日数据）
DB_INTRADAY = "fund_flow_intraday.db"   # 分时数据数据库文件名（存储当天240个分钟点数据）
STATIC_DIR = "static"                   # 前端静态文件目录（存放index.html）
MIN_REFRESH_INTERVAL = 180              # 每日数据刷新最小间隔（秒），防止频繁请求被封IP
MAX_DAILY_REQUESTS = 50                 # 每日数据每日最大请求次数，超过则拒绝
DAILY_COUNT_FILE = "daily_count.json"   # 记录每日请求次数的本地JSON文件
TOP_N = 90                              # 展示的行业数量（取净流入排名前20的行业）

LAST_REFRESH_TIME = 0                   # 上次刷新每日数据的时间戳（用于冷却计算）
_refresh_lock: asyncio.Lock | None = None  # 异步锁，防止多个刷新请求并发执行

# ==================== 分时时间轴生成 ====================
def _get_intraday_time_points():
    """
    生成A股完整交易时间的分钟点：
    - 上午盘：9:31 ~ 11:30 = 120分钟
    - 下午盘：13:00 ~ 15:00 = 121分钟
    - 合计：241个整分钟时间点
    每个点代表该分钟结束时的资金净流入快照
    """
    points = []                         # 存放所有时间点的列表，格式如 ["09:31", "09:32", ...]
    # --- 上午盘 9:31 ~ 11:30 ---
    for h in range(9, 12):              # 遍历小时 9, 10, 11
        start_m = 31 if h == 9 else 0   # 9点从31分开始，10点和11点从0分开始
        end_m = 59 if h < 11 else 30    # 9点和10点到59分，11点到30分结束
        for m in range(start_m, end_m + 1):  # 遍历该小时内的每一分钟
            points.append(f"{h:02d}:{m:02d}")  # 格式化为 "HH:MM" 字符串
    # --- 下午盘 13:00 ~ 15:00 ---
    for h in range(13, 16):             # 遍历小时 13, 14, 15
        start_m = 0                     # 13点、14点、15点都从0分开始
        end_m = 59 if h < 15 else 0     # 13点和14点到59分，15点只到0分（即15:00）
        for m in range(start_m, end_m + 1):  # 遍历该小时内的每一分钟
            points.append(f"{h:02d}:{m:02d}")  # 格式化为 "HH:MM" 字符串
    return points                       # 返回完整的时间点列表

INTRADAY_POINTS = _get_intraday_time_points()  # 启动时生成一次，作为全局常量
assert len(INTRADAY_POINTS) == 241, f"Expected 241, got {len(INTRADAY_POINTS)}"  # 断言检查：确保正好241个点

def _gen_time_labels(start_dt, max_points=240):
    """从指定时间开始，动态生成最多max_points个分钟时间标签，跳过午休(11:31~12:59)"""
    labels = []
    h, m = start_dt.hour, start_dt.minute
    while len(labels) < max_points:
        # 跳过午休时段
        if h == 11 and m > 30:
            h, m = 13, 1                # 跳到13:01
        elif h == 12:
            h, m = 13, 1                # 跳到13:01
        elif h >= 15 and m > 0:
            break                        # 超过15:00，停止
        elif h >= 16:
            break                        # 超过16:00，停止
        labels.append(f"{h:02d}:{m:02d}")  # 添加到标签列表
        m += 1                           # 下一分钟
        if m >= 60:
            m = 0
            h += 1
    return labels

# ==================== 分时采集线程状态变量 ====================
_collection_thread: threading.Thread | None = None  # 分时采集的后台线程对象
_collection_stop = threading.Event()    # 线程停止信号标志位，set()时线程退出循环
_collection_idx = 0                     # 当前正在采集第几个点（0~N），也是进度指示器
_collection_active = False              # 采集是否正在进行中的标志
_collection_total = 241                 # 本次采集的总点数

# ==================== 数据库操作函数 ====================
def get_db(path):
    """创建并返回一个SQLite数据库连接，row_factory使查询结果可通过列名访问"""
    conn = sqlite3.connect(path)        # 连接到指定路径的SQLite数据库文件
    conn.row_factory = sqlite3.Row      # 设置行工厂，让结果可以用 row["列名"] 方式访问
    return conn                         # 返回数据库连接对象

def init_db():
    """初始化两个数据库，创建数据表（如果不存在）"""
    # --- 每日数据库：存储近20个交易日的行业净流入数据 ---
    conn = get_db(DB_DAILY)             # 连接每日数据库
    conn.execute("""CREATE TABLE IF NOT EXISTS fund_flow (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL UNIQUE,
        time_points TEXT NOT NULL, sectors TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    # 表结构说明：
    #   id: 自增主键
    #   date: 数据日期（如"2025-07-04"），唯一约束防止同一天重复
    #   time_points: JSON数组，如 ["06-20","06-21",...,"07-04"] 共20个日期
    #   sectors: JSON数组，每个元素 {"name":"行业名","values":[20个数值]}
    #   created_at: 记录创建时间
    conn.commit(); conn.close()         # 提交并关闭连接

    # --- 分时数据库：存储当天240个分钟点的行业净流入数据 ---
    conn = get_db(DB_INTRADAY)          # 连接分时数据库
    conn.execute("""CREATE TABLE IF NOT EXISTS intraday_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL UNIQUE,
        time_points TEXT NOT NULL, sectors TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    # 表结构说明：
    #   date: 当天日期（如"2025-07-04"）
    #   time_points: 固定240个时间点 ["09:31","09:32",...,"15:00"]
    #   sectors: 每个元素 {"name":"行业名","values":[240个数值]}
    conn.commit(); conn.close()         # 提交并关闭连接

def save_daily(date_str, time_points, sectors):
    """保存每日数据到数据库，同一天会先删除旧数据再插入（覆盖更新）"""
    conn = get_db(DB_DAILY)             # 连接每日数据库
    conn.execute("DELETE FROM fund_flow WHERE date = ?", (date_str,))  # 先删除该日期的旧记录
    conn.execute("INSERT INTO fund_flow (date, time_points, sectors) VALUES (?, ?, ?)",
        (date_str,                      # 日期字符串
         json.dumps(time_points, ensure_ascii=False),  # 时间列表→JSON字符串
         json.dumps(sectors, ensure_ascii=False)))     # 行业数据列表→JSON字符串
    conn.commit(); conn.close()         # 提交事务并关闭连接

def load_daily():
    """从数据库加载最新的每日数据，返回None表示无数据"""
    conn = get_db(DB_DAILY)             # 连接每日数据库
    row = conn.execute(
        "SELECT date, time_points, sectors FROM fund_flow ORDER BY date DESC LIMIT 1"
    ).fetchone()                        # 按日期倒序取最新一条记录
    conn.close()                        # 关闭连接
    if row:                             # 如果有数据
        return {
            "date": row["date"],        # 数据日期
            "time_points": json.loads(row["time_points"]),  # JSON字符串→Python列表
            "sectors": json.loads(row["sectors"])           # JSON字符串→Python列表
        }
    return None                         # 无数据返回None

def save_intraday(date_str, time_points, sectors):
    """保存分时数据到数据库，同一天会覆盖更新（每分钟采集后都调用）"""
    conn = get_db(DB_INTRADAY)          # 连接分时数据库
    conn.execute("DELETE FROM intraday_data WHERE date = ?", (date_str,))  # 删除当天旧记录
    conn.execute("INSERT INTO intraday_data (date, time_points, sectors) VALUES (?, ?, ?)",
        (date_str,                      # 日期字符串
         json.dumps(time_points, ensure_ascii=False),  # 240个时间点→JSON
         json.dumps(sectors, ensure_ascii=False)))     # 行业数据→JSON
    conn.commit(); conn.close()         # 提交并关闭

def load_intraday():
    """从数据库加载当天的分时数据"""
    conn = get_db(DB_INTRADAY)          # 连接分时数据库
    today = datetime.now().strftime("%Y-%m-%d")  # 获取当天日期
    row = conn.execute(
        "SELECT date, time_points, sectors FROM intraday_data WHERE date = ?",
        (today,)                        # 只查当天的数据
    ).fetchone()
    conn.close()                        # 关闭连接
    if row:                             # 如果有当天数据
        return {
            "date": row["date"],
            "time_points": json.loads(row["time_points"]),
            "sectors": json.loads(row["sectors"])
        }
    return None                         # 无当天数据返回None

# ==================== 防频繁请求（限流保护） ====================
def _load_daily_count() -> dict:
    """从本地JSON文件加载今日请求计数，如果不是今天则重置为0"""
    today = datetime.now().date().isoformat()  # 获取今天日期字符串 "YYYY-MM-DD"
    try:
        with open(DAILY_COUNT_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)         # 读取计数文件
        if data.get("date") == today: return data  # 如果是今天的数据，直接返回
    except: pass                        # 文件不存在或格式错误，忽略
    return {"date": today, "count": 0}  # 返回新的计数对象（今天0次）

def _save_daily_count(data: dict):
    """将请求计数保存到本地JSON文件"""
    with open(DAILY_COUNT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)  # 序列化写入文件

def check_and_increment_daily() -> tuple[bool, int]:
    """
    检查是否可以发起每日数据请求，并递增计数
    返回: (是否允许, 剩余次数)
    """
    data = _load_daily_count()          # 加载当前计数
    if data["count"] >= MAX_DAILY_REQUESTS: return False, 0  # 超过50次上限，拒绝
    data["count"] += 1; _save_daily_count(data)  # 计数+1并保存
    return True, MAX_DAILY_REQUESTS - data["count"]  # 返回允许和剩余次数

# ==================== 同花顺数据获取（核心数据源） ====================
def _get_v_code():
    """
    生成同花顺API所需的hexin-v验证码
    原理：执行同花顺官方JS文件中的v()函数，生成一个动态token
    """
    js_code = py_mini_racer.MiniRacer()  # 创建JS执行引擎实例
    js_content = _get_file_content_ths("ths.js")  # 从akshare获取同花顺JS验证码脚本内容
    js_code.eval(js_content)            # 执行JS代码（注册v函数）
    return js_code.call("v")            # 调用v()函数获取验证码字符串

def _fetch_ths_page(url_tpl):
    """
    请求同花顺行业资金流页面，解析返回的HTML表格数据
    url_tpl: URL模板，包含{}占位符（页码）
    返回: pandas DataFrame（表格数据）或 None
    """
    v_code = _get_v_code()              # 生成hexin-v验证码
    headers = {
        "Accept": "text/html, */*; q=0.01",  # 接受HTML响应
        "hexin-v": v_code,              # 验证码（必须携带，否则被拒绝）
        "Host": "data.10jqka.com.cn",   # 请求主机
        "Referer": "http://data.10jqka.com.cn/funds/hyzjl/",  # 来源页面（反爬需要）
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",  # 模拟浏览器
        "X-Requested-With": "XMLHttpRequest"  # 标记为AJAX请求
    }
    r = requests.get(url_tpl.format(1), headers=headers, timeout=15)  # 请求第1页数据
    tables = pd.read_html(StringIO(r.text))  # 解析HTML中的所有<table>标签为DataFrame
    return tables[0] if tables else None  # 返回第一个表格（行业列表），无表格返回None

def _fetch_snapshot():
    """
    获取当前所有行业净流入的即时快照（一个时间点的数据）
    返回: {"行业名": 净流入金额, ...} 如 {"半导体": 12.35, "银行": -5.21, ...}
    """
    try:
        df = ak.stock_fund_flow_industry(symbol="即时")  # 调用akshare获取即时行业资金流
        if df is None or df.empty: return {}  # 无数据返回空字典
        df["净额"] = pd.to_numeric(df["净额"], errors="coerce").fillna(0)  # 净额列转数值，无效值填0
        result = {}                     # 构建结果字典
        for _, row in df.iterrows():    # 遍历每一行（每个行业）
            result[str(row["行业"])] = round(float(row["净额"]), 2)  # 行业名→净流入值（保留2位小数）

        # ========== 新增打印 ==========
        sorted_items = sorted(result.items(), key=lambda x: x[1], reverse=True)
        print(f"\n📊 当前全行业资金流向快照 ({len(result)} 个行业):")
        print("-" * 48)
        max_val_len = max(len(f"{v:+.2f}") for v in result.values())
        for rank, (name, value) in enumerate(sorted_items, 1):
            sign = "+" if value > 0 else ""
            num_str = f"{sign}{value:.2f}"
            print(f"  {rank:2d}. {name:<8s}  {num_str:>{max_val_len}} 亿")
        print("-" * 48)
        # ==============================

        return result                   # 返回 {"行业1": 值1, "行业2": 值2, ...}
    except Exception as e:
        print(f"❌ 快照获取失败: {e}")   # 打印错误信息
        return {}                       # 出错返回空字典

def _fetch_cumulative_fund_flow():
    """
    获取同花顺5个时间窗口的真实累计净流入数据：
    - 即时：当天累计净流入
    - 3日：近3天累计净流入
    - 5日：近5天累计净流入
    - 10日：近10天累计净流入
    - 20日：近20天累计净流入
    返回: {"即时": {行业:值}, "3日": {行业:值}, "5日": {行业:值}, "10日": {行业:值}, "20日": {行业:值}}
    """
    urls = {
        # 5个时间窗口对应的同花顺API URL模板（{}是页码占位符）
        "即时": "http://data.10jqka.com.cn/funds/hyzjl/field/tradezdf/order/desc/page/{}/ajax/1/free/1/",
        "3日": "http://data.10jqka.com.cn/funds/hyzjl/board/3/field/tradezdf/order/desc/page/{}/ajax/1/free/1/",
        "5日": "http://data.10jqka.com.cn/funds/hyzjl/board/5/field/tradezdf/order/desc/page/{}/ajax/1/free/1/",
        "10日": "http://data.10jqka.com.cn/funds/hyzjl/board/10/field/tradezdf/order/desc/page/{}/ajax/1/free/1/",
        "20日": "http://data.10jqka.com.cn/funds/hyzjl/board/20/field/tradezdf/order/desc/page/{}/ajax/1/free/1/",
    }
    result = {}                         # 存放5个窗口的结果
    for i, (period, url_tpl) in enumerate(urls.items()):  # 遍历每个时间窗口
        if i > 0:                       # 第一个请求前不等待
            time.sleep(3)               # 每个窗口之间间隔3秒，防止限频
        try:
            df = _fetch_ths_page(url_tpl)  # 请求该窗口的页面数据
            if df is not None and not df.empty:  # 如果有数据
                col_net = df.columns[6] if period == "即时" else df.columns[7]  # 净额列的列名（即时和其他窗口列数不同）
                col_name = df.columns[1]  # 行业名列名
                data = {}                 # 当前窗口的行业数据字典
                for _, row in df.iterrows():  # 遍历每行
                    val = pd.to_numeric(row[col_net], errors="coerce")  # 转换为数值
                    data[str(row[col_name])] = round(float(val), 2) if not pd.isna(val) else 0.0  # 存入字典
                result[period] = data     # 保存该窗口数据
                print(f"  ✅ {period}: {len(data)} 行业")  # 打印成功信息
            else: result[period] = {}     # 无数据填空字典
        except Exception as e:
            print(f"  ❌ {period}: {e}"); result[period] = {}  # 出错填空字典
    return result                       # 返回5个窗口的完整数据

def _fetch_daily_turnover(name, start, end):
    """
    获取某个行业在指定日期范围内的每日成交额数据
    用于后续按成交额权重分配累计值到每一天
    name: 行业名称（如"半导体"）
    start/end: 起止日期字符串（如"20250601"）
    返回: numpy数组（每日成交额列表）或 None
    """
    try:
        df = ak.stock_board_industry_index_ths(symbol=name, start_date=start, end_date=end)  # 获取行业日K线
        if df is not None and not df.empty:  # 如果有数据
            df["日期"] = pd.to_datetime(df["日期"])  # 日期列转datetime类型
            # 按日期排序，提取成交额列转数值，无效值填0，返回numpy数组
            return df.sort_values("日期")["成交额"].apply(lambda x: pd.to_numeric(x, errors="coerce")).fillna(0).values
    except: pass                        # 出错忽略
    return None                         # 失败返回None

def _distribute(total, turnover):
    """
    按成交额权重将一个总量分配到多天
    total: 需要分配的总量（如10日累计净流入差值）
    turnover: 每天的成交额数组（作为权重依据）
    返回: 每天分配到的值列表
    原理: 成交额越大的天，分配到的资金量越多（正比关系）
    """
    s = sum(turnover)                   # 计算成交额总和
    if s <= 0: return [round(total / len(turnover), 2)] * len(turnover)  # 总和为0则平均分配
    return [round(total * t / s, 2) for t in turnover]  # 按成交额占比分配

def _get_trading_dates(n=20):
    """
    获取最近n个交易日的日期列表（跳过周末）
    返回: [datetime.date对象列表] 从最早到最新排列
    注意：这只是简单跳过周末，未考虑法定节假日
    """
    dates = []; d = datetime.now().date()  # 从今天开始往回推
    while len(dates) < n:               # 收集够n个为止
        if d.weekday() < 5: dates.append(d)  # weekday() 0~4是周一到周五
        d -= timedelta(days=1)          # 往前推一天
    dates.reverse(); return dates       # 反转列表（从最早到最新），返回

# ==================== 每日数据构建（核心算法） ====================
def _build_daily_data(cumulative):
    """
    根据5个时间窗口的累计数据和每日成交额，构建20天的每日净流入数据
    算法原理：
    - 20日累计 = day1~20的总和
    - 10日累计 = day11~20的总和（最近10天）
    - 5日累计  = day16~20的总和（最近5天）
    - 3日累计  = day18~20的总和（最近3天）
    - 即时     = day20的值（当天）
    通过差分+成交额权重，将累计值拆解为每一天的值
    """
    trading_dates = _get_trading_dates(20)  # 获取最近20个交易日日期
    date_strs = [d.strftime("%m-%d") for d in trading_dates]  # 格式化为 "MM-DD" 字符串列表
    start_str = trading_dates[0].strftime("%Y%m%d")  # 起始日期 "YYYYMMDD"
    end_str = trading_dates[-1].strftime("%Y%m%d")   # 结束日期 "YYYYMMDD"
    d20 = cumulative.get("20日", {})    # 20日累计数据字典
    # 按20日累计值排序，取前20个行业
    top_names = sorted(d20.keys(), key=lambda x: d20.get(x, 0), reverse=True)[:TOP_N]
    sectors = []                        # 存放所有行业的数据
    for name in top_names:              # 遍历每个行业
        inst = cumulative.get("即时", {}).get(name, 0)   # 当天即时净流入
        d3 = cumulative.get("3日", {}).get(name, 0)      # 近3天累计
        d5 = cumulative.get("5日", {}).get(name, 0)      # 近5天累计
        d10 = cumulative.get("10日", {}).get(name, 0)    # 近10天累计
        t = _fetch_daily_turnover(name, start_str, end_str)  # 获取该行业20天的每日成交额
        if t is not None and len(t) >= 20:  # 如果成交额数据完整
            t = list(t[-20:])           # 取最近20天的成交额
            # 差分拆解 + 按成交额权重分配：
            # day1~10:  10日累计 - 5日累计 的差值，按成交额分配到前10天
            # day11~15: 5日累计 - 3日累计 的差值，按成交额分配到中间5天
            # day16~17: 3日累计 - 即时 的差值，按成交额分配到2天
            # day18~19: 即时值，按成交额分配到2天
            # day20:    即时值（当天实际值）
            vals = (_distribute(d10 - d5, t[0:10]) + _distribute(d5 - d3, t[10:15])
                  + _distribute(d3 - inst, t[15:17]) + _distribute(inst, t[17:19])
                  + [round(inst, 2)])
        else:                           # 成交额数据不完整时，简单平均分配
            avg = lambda total, n: round(total / n, 2)  # 平均分配函数
            vals = ([avg(d10-d5,10)]*10 + [avg(d5-d3,5)]*5 + [avg(d3-inst,2)]*2
                  + [avg(inst,2)]*2 + [round(inst,2)])
        sectors.append({"name": name, "values": vals})  # 添加该行业20天数据
        time.sleep(2)                   # 每个行业请求间隔2秒，防止限频
    return {"date": datetime.now().strftime("%Y-%m-%d"), "time_points": date_strs, "sectors": sectors}

def _print_daily_data(data):
    """在控制台打印每日数据摘要"""
    print(f"\n📊 每日净流入 ({data['date']})")
    print(f"   时间点: {data['time_points'][0]} ~ {data['time_points'][-1]}, 共{len(data['time_points'])}个点")
    print(f"   行业数: {len(data['sectors'])}")
    for s in data['sectors'][:3]:
        vals = s['values']
        print(f"   {s['name']}: 首={vals[0]:+.2f}, 末={vals[-1]:+.2f}")
    print()

def do_daily_refresh():
    """执行每日数据刷新：获取→构建→保存→打印"""
    print("📡 正在获取每日数据...")      # 控制台提示
    cumulative = _fetch_cumulative_fund_flow()  # 步骤1：获取5个窗口的累计数据
    data = _build_daily_data(cumulative)  # 步骤2：用累计数据构建20天每日数据
    save_daily(data["date"], data["time_points"], data["sectors"])  # 步骤3：保存到数据库
    _print_daily_data(data)             # 步骤4：在控制台打印表格
    print(f"✅ 每日数据已保存: {len(data['sectors'])} 行业 × {len(data['time_points'])} 天")
    return data                         # 返回构建好的数据（给API响应）

# ==================== 分时采集核心（后台线程） ====================
def _collection_worker():
    """
    分时采集线程的主函数：
    - 按北京时间每分钟对齐采集240个点
    - 例如第1个点等到9:31再采集，第2个点等9:32采集...
    - 采集完240个点后自动结束
    - 支持断点续采（重启后从上次位置继续）
    """
    print("🔥 测试点：非交易时间之后的第一行代码")
    global _collection_idx, _collection_active  # 声明全局变量（需要修改）
    today = datetime.now().strftime("%Y-%m-%d")  # 获取当天日期
    now = datetime.now()                  # 当前时间
    t_min = now.hour * 60 + now.minute    # 当前时间转总分钟
    is_weekday = now.weekday() < 5        # 是否工作日
    after_close = is_weekday and t_min > 15*60  # 是否已收盘（15:00之后）

    # --- 收盘后启动：直接获取当天最终数据填入最后一个点，然后结束 ---
    if after_close:
        print(f"🕒 当前 {now.strftime('%H:%M')} 已收盘，直接获取当天最终净流入数据...")
        snapshot = _fetch_snapshot()    # 获取一次快照
        if snapshot:
            sorted_items = sorted(snapshot.items(), key=lambda x: x[1], reverse=True) #按净流入金额从大到小排序
            all_names = [name for name, _ in sorted_items[:TOP_N]]
            time_points = list(INTRADAY_POINTS)
            
            # 直接操作数据库，只更新最后一个点，不碰其他点
            conn = get_db(DB_INTRADAY)  # 连接分时数据库
            existing = conn.execute(    # 查询当天是否已有数据
                "SELECT sectors FROM intraday_data WHERE date = ?", (today,)
            ).fetchone()
            
            if existing:  # 如果已经有当天数据
                old_sectors = json.loads(existing["sectors"])  # 把数据库里的JSON解析成Python列表
                old_dict = {s["name"]: s["values"] for s in old_sectors}  # 转成字典方便按行业名查找
                
                for name in all_names:  # 遍历要更新的行业
                    if name in old_dict:  # 如果这个行业在旧数据中已存在
                        old_dict[name][-1] = snapshot.get(name, 0)  # 只改最后一个点的值，其他点不动
                
                sectors = [{"name": n, "values": old_dict[n]} for n in old_dict]  # 字典转回列表格式
                
                conn.execute("DELETE FROM intraday_data WHERE date = ?", (today,))  # 删除当天旧记录
                conn.execute("INSERT INTO intraday_data (date, time_points, sectors) VALUES (?, ?, ?)",  # 写入新记录
                    (today,
                    json.dumps(time_points, ensure_ascii=False),  # 时间点转JSON
                    json.dumps(sectors, ensure_ascii=False)))     # 行业数据转JSON
                conn.commit()  # 提交事务
            conn.close()  # 关闭数据库连接

            _collection_idx = 241       # 标记为已完成
            top5 = sorted_items[:5]
            top5_str = ", ".join(f"{n}({v:+.1f})" for n, v in top5)
            print(f"✅ 收盘数据已保存 [241/241] TOP5: {top5_str}")
        else:
            print(f"❌ 收盘数据获取失败")
        _collection_active = False
        return                          # 直接结束

    # --- 非交易时间提示 ---
    in_trading = is_weekday and ((9*60+31 <= t_min <= 11*60+30) or (13*60 <= t_min <= 15*60))
    if not in_trading:
        print(f"⚠️️  当前 {now.strftime('%H:%M')} 非交易时间，将每分钟采集一次（数据可能相同）")
    # --- 断点续采：检查是否有当天已有的数据 ---
    existing = load_intraday()          # 从数据库加载当天分时数据
    if existing and existing["date"] == today:  # 如果有当天的数据
        # 恢复进度：从后往前找最后一个有数据的点
        tp = existing["time_points"]    # 240个时间点列表
        sectors_data = existing["sectors"]  # 行业数据列表
        last_filled = -1                # 最后一个有数据的索引
        for i in range(len(tp) - 1, -1, -1):  # 从最后一个点往前遍历
            has_data = any(s["values"][i] != 0 for s in sectors_data)  # 检查该点是否有数据
            if has_data:                # 找到最后一个有数据的点
                last_filled = i
                break
        # 下一采集索引：取“数据库最后填充点+1”和“当前时间对应索引”的较大值
        current_hm = f"{now.hour:02d}:{now.minute:02d}"
        time_idx = 0
        for i, tp_str in enumerate(INTRADAY_POINTS):
            if tp_str >= current_hm:
                time_idx = i
                break
        else:
            time_idx = 241
        _collection_idx = max(last_filled + 1, time_idx)  # 取较大值，确保不后退
        last_time = tp[last_filled] if last_filled >= 0 else "无"
        print(f"📂 恢复分时进度: 数据库最后填充={last_filled+1}/241(时间{last_time}), 当前时间={current_hm}(索引{time_idx}), 下一采集={_collection_idx+1}/241")
    else:
        # 首次启动：找到当前时间对应的索引，之前的点留0
        current_hm = f"{now.hour:02d}:{now.minute:02d}"  # 当前北京时间
        _collection_idx = 0             # 默认从0开始
        for i, tp in enumerate(INTRADAY_POINTS):
            if tp >= current_hm:        # 找到第一个 >= 当前时间的点
                _collection_idx = i
                break
        else:
            _collection_idx = 241       # 所有点都已过去
        if _collection_idx > 0:
            print(f"⏩ 跳过已过去的 {_collection_idx} 个点 (09:31~{INTRADAY_POINTS[_collection_idx-1]})，从 {INTRADAY_POINTS[_collection_idx]} 开始采集")

    # --- 初始化数据容器 ---
    time_points = list(INTRADAY_POINTS)  # 固定240个时间标签
    all_names = []                      # 行业名列表（首次采集时确定）
    name_values = {}                    # {行业名: [240个数值]} 存放所有行业的所有数据
    # 恢复已有数据：从数据库加载到内存，避免被覆盖为0
    if existing and existing["date"] == today and existing["sectors"]:
        for s in existing["sectors"]:   # 遍历数据库中的每个行业
            all_names.append(s["name"])  # 恢复行业名
            name_values[s["name"]] = list(s["values"])  # 恢复240个数值（保留已有数据）

    # --- 主采集循环：根据实际时间动态定位索引，每分钟采集一次 ---
    _collection_total = 241             # 固定241个点
    _collection_idx = 0
    while _collection_idx < _collection_total and not _collection_stop.is_set():  # 未采集完且未收到停止信号
        try:
            # --- 等待下一分钟到来（始终等待真实时间，不跳过） ---
            print("⏳ 进入等待下一分钟...")
            prev_minute = datetime.now().minute  # 记录当前分钟
            while not _collection_stop.is_set():
                now = datetime.now()
                if now.minute != prev_minute and now.second >= 1:  # 新的一分钟到了
                    break
                _collection_stop.wait(timeout=0.3)  # 0.3秒检查一次
    
            if _collection_stop.is_set():   # 收到停止信号
                break                       # 退出主循环
    
            # --- 根据实际时间动态定位索引（核心：标签必须匹配实际时间） ---
            now = datetime.now()
            actual_hm = f"{now.hour:02d}:{now.minute:02d}"  # 真实北京时间
            # 在INTRADAY_POINTS中找到实际时间对应的索引
            actual_idx = -1
            for i, tp in enumerate(INTRADAY_POINTS):
                if tp == actual_hm:
                    actual_idx = i
                    break
            print("actual_idx:{actual_idx}")
            # 找不到对应索引（午休11:31~12:59或其他非交易时间）→ 跳过本次，不采集
            if actual_idx < 0:
                # 计算下一个采集时间
                t_min_now = now.hour * 60 + now.minute
                if t_min_now < 9*60+31:
                    next_time = "09:31"
                elif t_min_now < 11*60+30:
                    next_time = f"{now.hour:02d}:{now.minute+1:02d}"  # 下一分钟
                elif t_min_now < 13*60:
                    next_time = "13:00"  # 午休→下午盘
                elif t_min_now < 15*60:
                    next_time = f"{now.hour:02d}:{now.minute+1:02d}"  # 下一分钟
                else:
                    next_time = "明天09:31"  # 已收盘
                print(f"⏸️️  当前:{actual_hm} 非采集时间，跳过 | 下次采集:{next_time} | 已采集:{_collection_idx}/{_collection_total}")
                continue  # 跳过本次循环，等待下一分钟

            if actual_idx > _collection_idx:
                _collection_idx = actual_idx  # 跳到实际时间对应的索引
            elif actual_idx < _collection_idx:
                pass  # 实际时间已超过当前索引，继续用当前索引（不后退）
    
            label_time = time_points[_collection_idx]  # 标签时间点（应等于实际时间）
            actual_time = now.strftime("%H:%M")        # 真实北京时间
            t_min = now.hour * 60 + now.minute
            is_trading = (now.weekday() < 5 and ((9*60+31 <= t_min <= 11*60+30) or (13*60 <= t_min <= 15*60)))
            trading_tag = "交易时间" if is_trading else "非交易时间"
    
            # --- 执行采集 ---
            print(f"🔍 正在获取{label_time}的资金流向数据...")  # 采集前提示
            snapshot = _fetch_snapshot()    # 调用API获取全行业即时快照
            if snapshot:                    # 如果获取成功
                sector_count = len(snapshot)  # 获取到的行业数
                # 首次采集时确定行业列表（按净流入排序取前20）
                if not all_names:           # 行业列表为空（第一次采集）
                    sorted_items = sorted(snapshot.items(), key=lambda x: x[1], reverse=True)  # 按净流入降序排列
                    all_names = [name for name, _ in sorted_items[:TOP_N]]  # 取前20个行业名
                    for name in all_names:  # 为每个行业初始化241个0值的数组
                        name_values[name] = [0.0] * 241
                    print(f"📊 首次采集，确定{len(all_names)}个行业: {', '.join(all_names[:5])}...")
    
                # 将本次采集的数据填入对应位置
                for name in all_names:      # 遍历所有行业
                    name_values[name][_collection_idx] = snapshot.get(name, 0)  # 填入该行业当前值
    
                # 打印详细信息：序号、标签时间、真实时间、交易状态、TOP5数据
                top5 = sorted(snapshot.items(), key=lambda x: x[1], reverse=True)[:5]
                top5_str = ", ".join(f"{n}({v:+.1f})" for n, v in top5)
                print(f"✅ 获取成功: {sector_count}个行业 | [{_collection_idx+1}/{_collection_total}] 标签:{label_time} 实际:{actual_time} [{trading_tag}]")
                print(f"   TOP5: {top5_str}")
    
                # 每次采集后立即保存到数据库（防止程序崩溃丢失数据）
                sectors = [{"name": n, "values": name_values[n]} for n in all_names]  # 构建行业数据
                save_intraday(today, time_points, sectors)  # 保存到分时数据库
                print(f"💾 已保存到数据库")
            else:
                print(f"❌ 获取失败: [{_collection_idx+1}/{_collection_total}] 标签:{label_time} 实际:{actual_time} [{trading_tag}] API返回空")
    
            _collection_idx += 1            # 进度+1，准备采集下一个点
    
        except Exception as e:
            # 捕获异常，防止线程崩溃，继续下一轮循环
            print(f"❌ 采集线程异常(已捕获，继续运行): {e}")
            import traceback
            traceback.print_exc()
            import time as _time
            _time.sleep(5)  # 异常后等待5秒再继续

    # --- 采集结束 ---
    # _collection_active = False          # 标记采集不再活跃
    if _collection_idx >= 241:          # 如果完成了全部241个点
        print("✅ 分时采集完成! 241/241 点")
    else:                               # 被手动停止
        print(f"⛷ 分时采集已停止: {_collection_idx}/241 点")

def start_collection():
    """启动分时采集线程，返回 (是否成功, 提示信息)"""
    global _collection_thread, _collection_active, _collection_stop  # 声明全局变量
    if _collection_active:              # 如果已经在采集
        return False, "采集已在运行中"   # 拒绝重复启动
    _collection_stop = threading.Event()  # 创建新的停止信号（重置）
    _collection_active = True           # 标记为活跃状态
    _collection_thread = threading.Thread(target=_collection_worker, daemon=True)  # 创建守护线程
    _collection_thread.start()          # 启动线程（开始采集）
    return True, "分时采集已启动"       # 返回成功

def stop_collection():
    """停止分时采集线程，返回 (是否成功, 提示信息)"""
    global _collection_active           # 声明全局变量
    if not _collection_active:          # 如果没在采集
        return False, "采集未在运行"    # 提示未运行
    _collection_stop.set()              # 设置停止信号（线程会在下次循环检测到并退出）
    _collection_active = False          # 标记为非活跃
    return True, "分时采集已停止"       # 返回成功

# ==================== 分时→每日 滑动更新 ====================
def _get_latest_intraday_values():
    """
    从分时数据中提取每个行业最新一个非零值
    即使240点没采完，也能用已采集到的最新值
    返回: {"行业名": 最新值, ...} 或 None
    """
    data = load_intraday()              # 从数据库加载当天分时数据
    if not data or not data.get("sectors"):  # 无数据
        return None                     # 返回None
    result = {}                         # 结果字典
    for s in data["sectors"]:           # 遍历每个行业
        name = s["name"]                # 行业名
        vals = s["values"]              # 该行业的240个值
        # 从后往前找最后一个非零值（即最新的有效数据）
        for i in range(len(vals) - 1, -1, -1):  # 从第239个点往前遍历
            if vals[i] != 0:            # 找到非零值
                result[name] = vals[i]  # 记录该值
                break                   # 找到就停
        else:                           # for循环正常结束（没找到非零值）
            result[name] = 0            # 该行业全部为0
    return result                       # 返回每个行业的最新值

def do_merge_intraday_to_daily():
    """
    将分时数据（241个点）合并到每日数据库：
    - 每天存储完整的241个时间点数据
    - 最大存储30天
    - 超过30天自动删除最早的一天
    返回: (更新后的数据, 错误信息) 成功时错误信息为None
    """
    # 加载当天分时数据（241个点）
    intraday = load_intraday()
    if not intraday or not intraday.get("sectors"):
        return None, "无分时数据可合并"

    today = datetime.now().strftime("%Y-%m-%d")

    # 从数据库加载所有记录
    conn = get_db(DB_DAILY)
    rows = conn.execute(
        "SELECT date, time_points, sectors FROM fund_flow ORDER BY date ASC"
    ).fetchall()
    
    # 转为列表
    records = []
    for row in rows:
        records.append({
            "date": row["date"],
            "time_points": json.loads(row["time_points"]),
            "sectors": json.loads(row["sectors"])
        })

    # 检查今天是否已有记录
    found = False
    for rec in records:
        if rec["date"] == today:
            # 更新今天的记录
            rec["time_points"] = intraday["time_points"]
            rec["sectors"] = intraday["sectors"]
            found = True
            break

    if not found:
        # 添加今天的记录
        records.append({
            "date": today,
            "time_points": intraday["time_points"],
            "sectors": intraday["sectors"]
        })

    # 超过30天则删除最早的
    MAX_DAYS = 30
    while len(records) > MAX_DAYS:
        removed = records.pop(0)
        print(f"🗑️ 删除过期数据: {removed['date']}")

    # 全部重新写入数据库
    conn.execute("DELETE FROM fund_flow")
    for rec in records:
        conn.execute(
            "INSERT INTO fund_flow (date, time_points, sectors) VALUES (?, ?, ?)",
            (rec["date"],
             json.dumps(rec["time_points"], ensure_ascii=False),
             json.dumps(rec["sectors"], ensure_ascii=False))
        )
    conn.commit()
    conn.close()

    # ===== 从每日数据库中读取30天数据并构造前端显示数据 =====
    # 重新读取数据库（确保是最新数据）
    conn = get_db(DB_DAILY)
    rows = conn.execute(
        "SELECT date, time_points, sectors FROM fund_flow ORDER BY date ASC"
    ).fetchall()
    conn.close()
    
    records = []
    for row in rows:
        records.append({
            "date": row["date"],
            "time_points": json.loads(row["time_points"]),
            "sectors": json.loads(row["sectors"])
        })
    
    # ===== 构造前端显示数据：30个自然日，横坐标日期，纵坐标每天15:00的值 =====
    today_date = datetime.now().date()
    # 生成近30个自然日的日期列表（从今天往前推29天）
    dates = []
    for i in range(29, -1, -1):  # 29, 28, ..., 0
        d = today_date - timedelta(days=i)
        dates.append(d.strftime("%Y-%m-%d"))
    
    # 将数据库记录按日期建立索引，方便查找
    record_by_date = {}
    for rec in records:
        record_by_date[rec["date"]] = rec
    
    # 取最近一天有数据的行业列表作为基准
    base_sectors = []
    for rec in reversed(records):
        if rec["sectors"]:
            base_sectors = rec["sectors"]
            break
    
    sectors_list = []
    if base_sectors:
        for sector_template in base_sectors:
            name = sector_template["name"]
            values = []
            for date_str in dates:
                if date_str in record_by_date:
                    rec = record_by_date[date_str]
                    sector_value = 0
                    for s in rec["sectors"]:
                        if s["name"] == name:
                            sector_value = s["values"][240] if len(s["values"]) > 240 else 0
                            break
                    values.append(sector_value)
                else:
                    values.append(0)  # 没有数据的日期填0
            sectors_list.append({"name": name, "values": values})
    
    result = {
        "date": dates[-1],  # 最后一天是今天
        "time_points": dates,  # 横坐标：30个自然日
        "sectors": sectors_list  # 每个行业30个值
    }

    # 返回今天的记录
    print(f"✅ 分时数据已合并到每日: {len(intraday['sectors'])} 行业 × 241个时间点 | 共{len(records)}天数据")
    return result, None

# ==================== FastAPI Web服务 ====================
async def _check_collection_alive():
    """定期检查采集线程是否存活，交易时间内自动重启崩溃的线程"""
    import asyncio
    while True:
        await asyncio.sleep(60)  # 每60秒检查一次
        now = datetime.now()
        t_min = now.hour * 60 + now.minute
        # 交易时间内检查（9:31~11:30, 13:00~15:00）
        in_trading = now.weekday() < 5 and (
            (9*60+31 <= t_min <= 11*60+30) or 
            (13*60 <= t_min <= 15*60)
        )
        if in_trading:
            # 检查线程是否存活
            if _collection_thread and not _collection_thread.is_alive():
                print(f"⚠️️️  采集线程已崩溃，自动重启...")
                global _collection_active
                _collection_active = False  # 重置状态
                ok, msg = start_collection()
                if ok:
                    print(f"✅ 采集线程已自动重启")
                else:
                    print(f"❌ 采集线程重启失败: {msg}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI生命周期管理：启动时初始化，关闭时清理"""
    global _refresh_lock                # 声明全局变量
    init_db()                           # 初始化数据库（创建表）
    _refresh_lock = asyncio.Lock()      # 创建异步锁（防止并发刷新）
    # 启动时自动开始分时采集
    ok, msg = start_collection()
    if ok:
        print(f"🚀 服务启动，自动开始分时采集: {msg}")
    else:
        print(f"⚠️️  服务启动，自动采集未启动: {msg}")
    # 启动后台监控任务：定期检查采集线程是否存活
    import asyncio as _asyncio
    _asyncio.create_task(_check_collection_alive())
    yield                               # 应用运行中...
    # （应用关闭后的清理代码可以写在这里）

app = FastAPI(lifespan=lifespan)        # 创建FastAPI应用实例
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")  # 挂载静态文件目录

@app.get("/")                           # 根路径：返回前端页面
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))  # 返回index.html文件

# --- 每日数据API ---
@app.get("/api/daily/history")          # GET请求：获取每日历史数据
async def daily_history():
    d = load_daily()
    if not d:
        return {"error": "暂无每日数据，请点击「更新数据(每日)」"}
    
    conn = get_db(DB_DAILY)
    rows = conn.execute(
        "SELECT date, time_points, sectors FROM fund_flow ORDER BY date ASC"
    ).fetchall()
    conn.close()
    
    records = []
    for row in rows:
        records.append({
            "date": row["date"],
            "time_points": json.loads(row["time_points"]),
            "sectors": json.loads(row["sectors"])
        })
    
    # 生成近30个自然日的日期列表
    today_date = datetime.now().date()
    dates = []
    for i in range(29, -1, -1):
        d = today_date - timedelta(days=i)
        dates.append(d.strftime("%Y-%m-%d"))
    
    # 按日期建立索引
    record_by_date = {}
    for rec in records:
        record_by_date[rec["date"]] = rec
    
    # 取最近一天有数据的行业列表
    base_sectors = []
    for rec in reversed(records):
        if rec["sectors"]:
            base_sectors = rec["sectors"]
            break
    
    sectors_list = []
    if base_sectors:
        for sector_template in base_sectors:
            name = sector_template["name"]
            values = []
            for date_str in dates:
                if date_str in record_by_date:
                    rec = record_by_date[date_str]
                    sector_value = 0
                    for s in rec["sectors"]:
                        if s["name"] == name:
                            sector_value = s["values"][240] if len(s["values"]) > 240 else 0
                            break
                    values.append(sector_value)
                else:
                    values.append(0)
            sectors_list.append({"name": name, "values": values})
    
    return {
        "date": dates[-1],
        "time_points": dates,  # 横坐标：30个自然日
        "sectors": sectors_list  # 每个行业30个值
    }

@app.post("/api/daily/refresh")         # POST请求：刷新每日数据
async def daily_refresh():
    global LAST_REFRESH_TIME            # 声明全局变量
    if _refresh_lock and _refresh_lock.locked():  # 检查是否有其他请求正在执行
        return JSONResponse(status_code=429, content={"error": "已有任务在执行中"})  # 返回429（太忙）
    async with _refresh_lock:           # 获取锁（防止并发）
        now = time.time()               # 当前时间戳
        if now - LAST_REFRESH_TIME < MIN_REFRESH_INTERVAL:  # 检查冷却时间（180秒）
            return JSONResponse(status_code=429, content={"error": f"请 {int(MIN_REFRESH_INTERVAL-(now-LAST_REFRESH_TIME))} 秒后再更新"})
        ok, remain = check_and_increment_daily()  # 检查每日请求次数上限
        if not ok:                      # 超过上限
            return JSONResponse(status_code=429, content={"error": f"今日上限{MAX_DAILY_REQUESTS}次"})
        try:
            result = do_daily_refresh()  # 执行刷新逻辑
            LAST_REFRESH_TIME = time.time()  # 更新上次刷新时间
           # 构造30个自然日的数据
            conn = get_db(DB_DAILY)
            rows = conn.execute(
                "SELECT date, time_points, sectors FROM fund_flow ORDER BY date ASC"
            ).fetchall()
            conn.close()
            
            records = []
            for row in rows:
                records.append({
                    "date": row["date"],
                    "time_points": json.loads(row["time_points"]),
                    "sectors": json.loads(row["sectors"])
                })
            
            today_date = datetime.now().date()
            dates = []
            for i in range(29, -1, -1):
                d = today_date - timedelta(days=i)
                dates.append(d.strftime("%Y-%m-%d"))
            
            record_by_date = {}
            for rec in records:
                record_by_date[rec["date"]] = rec
            
            base_sectors = []
            for rec in reversed(records):
                if rec["sectors"]:
                    base_sectors = rec["sectors"]
                    break
            
            sectors_list = []
            if base_sectors:
                for sector_template in base_sectors:
                    name = sector_template["name"]
                    values = []
                    for date_str in dates:
                        if date_str in record_by_date:
                            rec = record_by_date[date_str]
                            sector_value = 0
                            for s in rec["sectors"]:
                                if s["name"] == name:
                                    sector_value = s["values"][240] if len(s["values"]) > 240 else 0
                                    break
                            values.append(sector_value)
                        else:
                            values.append(0)
                    sectors_list.append({"name": name, "values": values})
            
            return {
                "date": dates[-1],
                "time_points": dates,
                "sectors": sectors_list
            }
        except Exception as e:
            return JSONResponse(status_code=500, content={"error": str(e)})

# --- 分时数据API ---
@app.get("/api/intraday/history")       # GET请求：获取当天分时数据
async def intraday_history():
    d = load_intraday()                 # 从数据库加载当天数据
    if d: return d                      # 有数据直接返回
    # 无数据时返回空的240点结构（前端可正常渲染空图表）
    return {"date": datetime.now().strftime("%Y-%m-%d"), "time_points": INTRADAY_POINTS, "sectors": []}

@app.post("/api/intraday/start")        # POST请求：启动分时采集
async def intraday_start():
    ok, msg = start_collection()        # 调用启动函数
    return {"success": ok, "message": msg, "total_points": 241}  # 返回结果

@app.post("/api/intraday/stop")         # POST请求：停止分时采集
async def intraday_stop():
    ok, msg = stop_collection()         # 调用停止函数
    return {"success": ok, "message": msg}  # 返回结果

@app.get("/api/intraday/status")        # GET请求：获取采集状态（前端轮询用）
async def intraday_status():
    return {"active": _collection_active, "current_idx": _collection_idx, "total": 241}

@app.post("/api/intraday/refresh")      # POST请求：刷新分时数据（获取当前快照并更新）
async def intraday_refresh():
    """刷新分时数据：获取当前快照，追加或更新最后一个动态点"""
    today = datetime.now().strftime("%Y-%m-%d")  # 当天日期
    now = datetime.now()
    current_hm = f"{now.hour:02d}:{now.minute:02d}"  # 当前时间标签
    snapshot = _fetch_snapshot()          # 获取当前全行业快照
    if not snapshot:
        return {"error": "API返回空，请重试"}
    # 加载数据库中已有的分时数据
    existing = load_intraday()
    if existing and existing["date"] == today and existing["sectors"]:
        tp = existing["time_points"]
        # 清理多余动态点：保留241个固定点 + 最多1个动态点
        fixed_count = sum(1 for t in tp if t in INTRADAY_POINTS)
        extra = len(tp) - fixed_count     # 动态点数量
        if extra > 1:
            # 多个动态点 → 只保留最后一个，删除前面的
            keep = fixed_count + 1        # 固定点 + 1个动态点
            tp = tp[:keep]                # 截断到keep个
            for s in existing["sectors"]:
                s["values"] = s["values"][:keep]
        # 判断最后一个点状态
        last_tp = tp[-1] if tp else ""
        is_fixed = last_tp in INTRADAY_POINTS
        if is_fixed:
            # 最后一个点是固定点 → 追加新的动态点
            tp.append(current_hm)
            for s in existing["sectors"]:
                s["values"].append(0.0)
            idx = len(tp) - 1
        else:
            # 最后一个点已经是动态点 → 更新它（不新增）
            tp[-1] = current_hm          # 更新时间标签
            idx = len(tp) - 1
        # 更新该点的数据
        for s in existing["sectors"]:
            s["values"][idx] = snapshot.get(s["name"], 0)
        save_intraday(today, tp, existing["sectors"])
        return {"date": today, "time_points": tp, "sectors": existing["sectors"]}
    else:
        # 无当天数据：首次创建
        sorted_items = sorted(snapshot.items(), key=lambda x: x[1], reverse=True)
        all_names = [name for name, _ in sorted_items[:TOP_N]]
        time_points = list(INTRADAY_POINTS)       # 只有一个时间点
        sectors = []
        for name in all_names:
            sectors.append({"name": name, "values": [0.0] * 241})
        save_intraday(today, time_points, sectors)
        return {"date": today, "time_points": time_points, "sectors": sectors}

# --- 分时→每日 滑动合并API ---
@app.post("/api/daily/merge_intraday")  # POST请求：将分时数据合并到每日
async def merge_intraday():
    try:
        result, err = do_merge_intraday_to_daily()
        if err:
            return JSONResponse(status_code=400, content={"error": err})
        
        # result 已经包含了30个自然日的数据，直接返回
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    
# ==================== 程序入口 ====================
if __name__ == "__main__":              # 直接运行此文件时执行
    print("🚀 启动服务器...")            # 控制台提示
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
    # 启动uvicorn服务器：
    #   app: FastAPI应用实例
    #   host="0.0.0.0": 监听所有网络接口（允许局域网访问）
    #   port=8000: 端口号8000
    #   log_level="info": 日志级别
