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
from io import StringIO                 # 字符串转IO流，用于pd.read_html解析HTML表格
from fastapi import FastAPI             # Web框架，提供HTTP API服务
from fastapi.responses import FileResponse      # 文件响应，用于返回HTML页面
from starlette.responses import JSONResponse    # JSON响应，用于返回错误状态码
from fastapi.staticfiles import StaticFiles     # 静态文件服务，用于托管前端HTML/JS文件
from akshare.stock_feature.stock_fund_flow import _get_file_content_ths  # 获取同花顺JS验证码文件内容
from fastapi import Request
import uvicorn                          # ASGI服务器，用于运行FastAPI应用

# ==================== 全局配置 ====================
DB_DAILY = "fund_flow.db"               # 每日数据数据库文件名（存储近20个交易日数据）
DB_INTRADAY = "fund_flow_intraday.db"   # 分时数据数据库文件名（存储当天240个分钟点数据）
STATIC_DIR = "static"                   # 前端静态文件目录（存放index.html）
TOP_N = 90                              # 展示的行业数量（取净流入排名前20的行业）

# ==================== 个股追踪配置（修改此处增减股票，code为空则跳过） ====================
STOCK_LIST = [
    {"code": "600276", "name": "恒瑞医疗", "market": "sh"},
    {"code": "000651", "name": "格力电器", "market": "sz"},
    {"code": "000568", "name": "泸州老窖", "market": "sz"},
]
STOCK_INTERVAL = 2                      # 每只股票采集间隔秒数（防触发限制）
DB_STOCK_INTRADAY = "stock_intraday.db" # 个股分时数据库文件名
DB_STOCK_DAILY = "stock_daily.db"       # 个股每日数据库文件名


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
_collection_lock = threading.Lock()     # 线程锁：保护全局变量的并发访问

# ==================== 个股采集状态变量 ====================
_stock_names = {}                       # {股票代码: 股票名称} 如 {"000001": "平安银行"}
_stock_values = {}                      # {股票代码: [241个数值]} 存放所有个股的所有分时数据

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

# ==================== 个股数据库操作 ====================
def init_stock_db():
    """初始化个股分时和每日数据库"""
    conn = get_db(DB_STOCK_INTRADAY)
    conn.execute("""CREATE TABLE IF NOT EXISTS stock_intraday (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL UNIQUE,
        time_points TEXT NOT NULL, stocks TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    conn.commit(); conn.close()

    conn = get_db(DB_STOCK_DAILY)
    conn.execute("""CREATE TABLE IF NOT EXISTS stock_daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT, date TEXT NOT NULL UNIQUE,
        time_points TEXT NOT NULL, stocks TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    conn.commit(); conn.close()

def save_stock_intraday(date_str, time_points, stocks):
    """保存个股分时数据"""
    conn = get_db(DB_STOCK_INTRADAY)
    conn.execute("DELETE FROM stock_intraday WHERE date = ?", (date_str,))
    conn.execute("INSERT INTO stock_intraday (date, time_points, stocks) VALUES (?, ?, ?)",
        (date_str, json.dumps(time_points, ensure_ascii=False), json.dumps(stocks, ensure_ascii=False)))
    conn.commit(); conn.close()

def load_stock_intraday():
    """加载当天个股分时数据"""
    conn = get_db(DB_STOCK_INTRADAY)
    today = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT date, time_points, stocks FROM stock_intraday WHERE date = ?", (today,)
    ).fetchone()
    conn.close()
    if row:
        return {"date": row["date"], "time_points": json.loads(row["time_points"]), "stocks": json.loads(row["stocks"])}
    return None

# 全局复用的Session，避免每次请求都新建连接（减少被服务器断连的概率）
_stock_session = None

def _get_stock_session():
    """获取或创建一个带完整浏览器请求头的requests.Session"""
    global _stock_session
    if _stock_session is None:
        _stock_session = requests.Session()
        _stock_session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://data.eastmoney.com/",
            "Origin": "https://data.eastmoney.com",
        })
    return _stock_session

def _load_latest_allstock_net_flow():
    """
    从全部个股数据库(allstock.db)加载最近一天(今天优先，其次最近有数据的日期)
    的 {股票代码: 主力净流入净额(元)} 字典。
    用于个股追踪数据优先读库，避免启动/盘中反复拉取全市场5210只股票。
    返回空字典 {} 表示数据库无数据。
    """
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_ALLSTOCK)
    c = conn.cursor()
    load_date = today
    # 优先今天，其次最近有数据的日期
    row = c.execute("SELECT 1 FROM allstock_progress WHERE fetch_date=?", (today,)).fetchone()
    if not row:
        r2 = c.execute("SELECT fetch_date FROM allstock_progress ORDER BY fetch_date DESC LIMIT 1").fetchone()
        if r2:
            load_date = r2[0]
        else:
            conn.close()
            return {}
    rows = c.execute(
        "SELECT stock_code, net_flow FROM allstock_data WHERE fetch_date=?", (load_date,)
    ).fetchall()
    conn.close()
    result = {}
    for code, net_flow in rows:
        try:
            result[code] = float(net_flow) if net_flow not in (None, "") else 0.0
        except (ValueError, TypeError):
            result[code] = 0.0
    return result

def _fetch_stock_snapshot():
    """
    获取STOCK_LIST中所有有效个股的资金净流入（亿元）
    优先从全部个股数据库(allstock.db)读取最近一天的数据（启动/盘中不拉取全市场5210只），
    数据库无数据时才实时请求同花顺全市场接口（保底，如首次运行）
    返回: [{"code":"000001","name":"平安银行","value":1.23}, ...] 按STOCK_LIST顺序
    code为空的条目会被跳过，但保留位置（value=0）
    """
    results = []
    
    # 先检查是否有有效股票需要获取
    valid_stocks = [s for s in STOCK_LIST if s["code"]]
    if not valid_stocks:
        return [{"code": "", "name": "", "value": 0} for _ in STOCK_LIST]
    
    # 优先从数据库读取最近一天数据（交易日15:00自动获取后写入allstock.db）
    db_data = _load_latest_allstock_net_flow()
    if db_data:
        # 按STOCK_LIST顺序返回结果
        for stock in STOCK_LIST:
            if not stock["code"]:
                results.append({"code": "", "name": "", "value": 0})
                continue
            val_yuan = db_data.get(stock["code"], 0)
            results.append({
                "code": stock["code"],
                "name": stock["name"],
                "value": round(val_yuan / 100000000, 2),  # 元 → 亿元
            })
        print(f"✅ 从数据库读取个股数据: {len(results)} 只股票")
        return results
    
    # 数据库无数据（首次运行等）：才实时请求一次全市场个股资金流数据（同花顺，保底）
    all_stock_data = {}
    try:
        df = ak.stock_fund_flow_individual(symbol="即时")
        if df is not None and not df.empty:
            # 构建 {股票代码: 主力净流入} 字典
            for _, row in df.iterrows():
                code = str(row.get("股票代码", "")).strip()
                val = row.get("主力净流入-净额", 0)
                if code:
                    try:
                        all_stock_data[code] = float(val) if pd.notna(val) else 0
                    except (ValueError, TypeError):
                        all_stock_data[code] = 0
            print(f"✅ 同花顺个股数据获取成功: {len(all_stock_data)} 只股票")
    except Exception as e:
        print(f"⚠️ 同花顺个股数据获取失败: {e}")
    
    # 按STOCK_LIST顺序返回结果
    for stock in STOCK_LIST:
        if not stock["code"]:
            results.append({"code": "", "name": "", "value": 0})
            continue
        
        val_yuan = all_stock_data.get(stock["code"], 0)
        results.append({
            "code": stock["code"],
            "name": stock["name"],
            "value": round(val_yuan / 100000000, 2),  # 元 → 亿元
        })
    
    return results

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
            # 自动合并行业分时到每日数据库
            print("🔄 自动合并行业分时数据到每日数据库...")
            try:
                result, err = do_merge_intraday_to_daily()
                if err:
                    print(f"❌ 行业自动合并失败: {err}")
                else:
                    print(f"✅ 行业自动合并成功! 共{len(result.get('time_points', []))}天数据")
            except Exception as e:
                print(f"❌ 行业自动合并异常: {e}")
            # 收盘后也采集个股数据
            if STOCK_LIST:
                print(f"📈 收盘后采集{len([s for s in STOCK_LIST if s['code']])}只个股数据...")
                stock_snapshot = _fetch_stock_snapshot()
                if stock_snapshot:
                    if not _stock_names:
                        for s in STOCK_LIST:
                            if s["code"]:
                                _stock_names[s["code"]] = s["name"]
                                _stock_values[s["code"]] = [0.0] * 241
                    for i, s in enumerate(STOCK_LIST):
                        if s["code"] and i < len(stock_snapshot):
                            _stock_values[s["code"]][240] = stock_snapshot[i]["value"]
                    stock_sectors = [{"code": s["code"], "name": s["name"], "values": _stock_values[s["code"]]}
                                     for s in STOCK_LIST if s["code"]]
                    save_stock_intraday(today, time_points, stock_sectors)
                    stock_top3 = sorted([(s["name"], s["value"]) for s in stock_snapshot if s["code"]],
                                       key=lambda x: x[1], reverse=True)[:3]
                    stock_top3_str = ", ".join(f"{n}({v:+.1f})" for n, v in stock_top3)
                    print(f"💾 个股数据已保存 | TOP3: {stock_top3_str}")
                    # 自动合并个股到每日
                    print("🔄 自动合并个股分时数据到每日数据库...")
                    try:
                        stock_result, stock_err = do_merge_stock_intraday_to_daily()
                        if stock_err:
                            print(f"❌ 个股自动合并失败: {stock_err}")
                        else:
                            print(f"✅ 个股自动合并成功!")
                    except Exception as e:
                        print(f"❌ 个股自动合并异常: {e}")
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
        tp = existing["time_points"]    # 时间点列表
        sectors_data = existing["sectors"]  # 行业数据列表
        last_filled = -1                # 最后一个有数据的索引
        # 修复：使用INTRADAY_POINTS的长度作为上限，避免数据库中存储了错误长度的数据
        max_idx = min(len(tp), len(INTRADAY_POINTS)) - 1
        for i in range(max_idx, -1, -1):  # 从最后一个点往前遍历
            # 修复：添加对values数组长度的检查，避免越界
            has_data = any(
                i < len(s["values"]) and s["values"][i] != 0 
                for s in sectors_data
            )
            if has_data:                # 找到最后一个有数据的点
                last_filled = i
                break
        # 下一采集索引：取"数据库最后填充点+1"和"当前时间对应索引"的较大值
        current_hm = f"{now.hour:02d}:{now.minute:02d}"
        t_min_now = now.hour * 60 + now.minute
        time_idx = 0
        
        # 修复：处理午休时间（11:31~12:59），应该等待到13:00继续采集
        if t_min_now > 11*60+30 and t_min_now < 13*60:
            # 午休时间，下一个采集点是13:00（索引120）
            time_idx = 120  # 13:00在INTRADAY_POINTS中的索引
            print(f"🕐 当前午休时间{current_hm}，下午盘13:00继续采集")
        elif t_min_now >= 15*60:
            # 已收盘
            time_idx = 241
        else:
            for i, tp_str in enumerate(INTRADAY_POINTS):
                if tp_str >= current_hm:
                    time_idx = i
                    break
            else:
                time_idx = 241
        
        # 修复：先修正last_filled边界，再计算_collection_idx（之前的顺序反了）
        if last_filled >= len(INTRADAY_POINTS):
            print(f"⚠️ 数据库last_filled={last_filled}超出范围(总点数{len(INTRADAY_POINTS)})，修正为{len(INTRADAY_POINTS)-1}")
            last_filled = len(INTRADAY_POINTS) - 1
        
        # 修复：检测数据库数据是否异常（last_filled超出当前时间应有的进度）
        # 如果数据库记录的最后填充点 > 当前时间对应的索引，说明数据异常
        db_progress = last_filled + 1
        if db_progress > time_idx and time_idx < 241:
            # 数据库进度超前于当前时间，数据可能损坏
            print(f"⚠️ 检测到数据库数据异常: 数据库记录进度={db_progress}/241, 但当前时间{current_hm}应对应索引{time_idx}")
            print(f"🔄 忽略数据库异常进度，使用当前时间索引: {time_idx}")
            _collection_idx = time_idx
        else:
            _collection_idx = max(last_filled + 1, time_idx)  # 正常情况取较大值
        
        # 修复：确保_collection_idx不超过有效范围（0~241）
        if _collection_idx > len(INTRADAY_POINTS):
            _collection_idx = len(INTRADAY_POINTS)
        # 修复：添加边界检查，避免tp[last_filled]越界
        last_time = tp[last_filled] if 0 <= last_filled < len(tp) else "无"
        
        # 修复：如果_collection_idx已经>=241，说明当天采集已完成
        if _collection_idx >= len(INTRADAY_POINTS):
            print(f"✅ 当天分时采集已完成: 数据库已有{last_filled+1}/241个数据点，无需继续采集")
            _collection_active = False
            return
        print(f"📂 恢复分时进度: 数据库最后填充={last_filled+1}/241(时间{last_time}), 当前时间={current_hm}(索引{time_idx}), 下一采集={_collection_idx+1}/241")
    else:
        # 首次启动：找到当前时间对应的索引，之前的点留0
        current_hm = f"{now.hour:02d}:{now.minute:02d}"  # 当前北京时间
        t_min_now = now.hour * 60 + now.minute
        _collection_idx = 0             # 默认从0开始
        
        # 修复：处理午休时间（11:31~12:59），应该等待到13:00继续采集
        if t_min_now > 11*60+30 and t_min_now < 13*60:
            # 午休时间，下一个采集点是13:00（索引120）
            _collection_idx = 120  # 13:00在INTRADAY_POINTS中的索引
            print(f"🕐 当前午休时间{current_hm}，下午盘13:00开始采集")
        elif t_min_now >= 15*60:
            # 已收盘
            _collection_idx = 241
        else:
            for i, tp in enumerate(INTRADAY_POINTS):
                if tp >= current_hm:        # 找到第一个 >= 当前时间的点
                    _collection_idx = i
                    break
            else:
                _collection_idx = 241       # 所有点都已过去
        
        if 0 < _collection_idx < 241:
            print(f"⏩ 跳过已过去的 {_collection_idx} 个点 (09:31~{INTRADAY_POINTS[_collection_idx-1]})，从 {INTRADAY_POINTS[_collection_idx]} 开始采集")

    # --- 初始化数据容器 ---
    time_points = list(INTRADAY_POINTS)  # 固定241个时间标签
    all_names = []                      # 行业名列表（首次采集时确定）
    name_values = {}                    # {行业名: [241个数值]} 存放所有行业的所有数据
    # 恢复已有数据：从数据库加载到内存，避免被覆盖为0
    if existing and existing["date"] == today and existing["sectors"]:
        for s in existing["sectors"]:   # 遍历数据库中的每个行业
            all_names.append(s["name"])  # 恢复行业名
            name_values[s["name"]] = list(s["values"])  # 恢复241个数值（保留已有数据）

    # --- 恢复个股数据：从数据库加载到内存（断点续采） ---
    stock_existing = load_stock_intraday()  # 从数据库加载当天个股分时数据
    if stock_existing and stock_existing["date"] == today and stock_existing.get("stocks"):
        print(f"📂 恢复个股数据: {len(stock_existing['stocks'])} 只股票")
        for s in stock_existing["stocks"]:  # 遍历数据库中的每只股票
            code = s["code"]
            _stock_names[code] = s["name"]  # 恢复股票名称
            _stock_values[code] = list(s["values"])  # 恢复241个数值（保留已有数据）

    # --- 主采集循环：根据实际时间动态定位索引，每分钟采集一次 ---
    _collection_total = 241             # 固定241个点
    # 注意：_collection_idx 已在上面的断点续采逻辑中正确设置，不要重置！
    _last_fetch_time = 0  # 记录上次采集的时间戳，防止重复采集
    print(f"🚀 开始采集循环，起始索引: {_collection_idx}/241")
    while _collection_idx < _collection_total and not _collection_stop.is_set():  # 未采集完且未收到停止信号
        try:
            # --- 等待下一分钟到来（修复版：使用更可靠的等待逻辑） ---
            print("⏳ 进入等待下一分钟...")
            prev_minute = datetime.now().minute  # 记录当前分钟
            wait_start = time.time()  # 记录等待开始时间
            while not _collection_stop.is_set():
                now = datetime.now()
                # 修复：只要分钟变化就立即退出，不再要求秒数>=1
                if now.minute != prev_minute:
                    break
                # 防止无限等待：如果等待超过65秒，强制退出
                if time.time() - wait_start > 65:
                    print(f"⚠️ 等待超时(65秒)，强制继续...")
                    break
                _collection_stop.wait(timeout=0.5)  # 0.5秒检查一次（稍微延长，减少CPU占用）
    
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
            print(f"actual_idx:{actual_idx}, current_hm:{actual_hm}")
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

            # 修复：防止索引跳跃导致漏采（使用线程锁保护）
            with _collection_lock:
                if actual_idx > _collection_idx + 1:
                    # 如果跳跃超过1个点，记录警告但仍然跳跃（避免永久落后）
                    print(f"⚠️ 索引跳跃: {_collection_idx} -> {actual_idx}，跳过 {actual_idx - _collection_idx - 1} 个点")
                    _collection_idx = actual_idx
                elif actual_idx == _collection_idx + 1:
                    # 正常前进
                    _collection_idx = actual_idx
                elif actual_idx < _collection_idx:
                    # 实际时间索引小于当前索引，说明跨天或时钟问题
                    print(f"⚠️ 索引异常: actual_idx={actual_idx} < _collection_idx={_collection_idx}")
                    pass  # 保持当前索引
                # else: actual_idx == _collection_idx，保持不变
        
            label_time = time_points[_collection_idx]  # 标签时间点（应等于实际时间）
            actual_time = now.strftime("%H:%M")        # 真实北京时间
            t_min = now.hour * 60 + now.minute
            is_trading = (now.weekday() < 5 and ((9*60+31 <= t_min <= 11*60+30) or (13*60 <= t_min <= 15*60)))
            trading_tag = "交易时间" if is_trading else "非交易时间"
    
            # --- 执行采集（带超时保护） ---
            print(f"🔍 正在获取{label_time}的资金流向数据...")  # 采集前提示
            
            # 使用线程池实现超时保护
            import concurrent.futures
            snapshot = None
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(_fetch_snapshot)
                    snapshot = future.result(timeout=30)  # 30秒超时
            except concurrent.futures.TimeoutError:
                print(f"⚠️ API请求超时(30秒)，跳过本次采集")
                snapshot = {}
            except Exception as e:
                print(f"⚠️ API请求异常: {e}")
                snapshot = {}
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
                print(f"💾 行业数据已保存到数据库")

                # ---- 个股数据采集（akshare单股明细接口，每只1次请求+2s间隔） ----
                if STOCK_LIST:          # 如果有配置个股
                    print(f"📈 开始采集{len([s for s in STOCK_LIST if s['code']])}只个股数据...")
                    stock_snapshot = _fetch_stock_snapshot()  # 逐只获取，每只间隔2秒
                    if stock_snapshot:
                        # 首次采集时初始化个股数据结构
                        if not _stock_names:
                            for s in STOCK_LIST:
                                if s["code"]:  # 只初始化有效股票
                                    _stock_names[s["code"]] = s["name"]
                                    _stock_values[s["code"]] = [0.0] * 241

                        # 将本次采集的数据填入对应位置（按STOCK_LIST顺序）
                        for i, s in enumerate(STOCK_LIST):
                            if s["code"] and i < len(stock_snapshot):
                                _stock_values[s["code"]][_collection_idx] = stock_snapshot[i]["value"]

                        # 保存个股分时数据到数据库
                        stock_sectors = [{"code": s["code"], "name": s["name"], "values": _stock_values[s["code"]]}
                                         for s in STOCK_LIST if s["code"]]
                        save_stock_intraday(today, time_points, stock_sectors)
                        stock_top3 = sorted([(s["name"], s["value"]) for s in stock_snapshot if s["code"]],
                                           key=lambda x: x[1], reverse=True)[:3]
                        stock_top3_str = ", ".join(f"{n}({v:+.1f})" for n, v in stock_top3)
                        print(f"💾 个股数据已保存 | TOP3: {stock_top3_str}")
            else:
                print(f"❌ 获取失败: [{_collection_idx+1}/{_collection_total}] 标签:{label_time} 实际:{actual_time} [{trading_tag}] API返回空")
    
            with _collection_lock:
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
        # 自动合并行业分时到每日数据库
        print("🔄 自动合并行业分时数据到每日数据库...")
        try:
            result, err = do_merge_intraday_to_daily()
            if err:
                print(f"❌ 行业自动合并失败: {err}")
            else:
                print(f"✅ 行业自动合并成功! 共{len(result.get('time_points', []))}天数据")
        except Exception as e:
            print(f"❌ 行业自动合并异常: {e}")
        # 自动合并个股分时到每日数据库
        if STOCK_LIST:
            print("🔄 自动合并个股分时数据到每日数据库...")
            try:
                stock_result, stock_err = do_merge_stock_intraday_to_daily()
                if stock_err:
                    print(f"❌ 个股自动合并失败: {stock_err}")
                else:
                    print(f"✅ 个股自动合并成功!")
            except Exception as e:
                print(f"❌ 个股自动合并异常: {e}")
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

    # 超过1080个交易日（约3年）则删除最早的
    MAX_DAYS = 1080  # 30 * 36 = 1080个交易日
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
    
    # 标记被删除的日期（数据库中不存在的日期）
    deleted_dates = [d for d in dates if d not in record_by_date]
    
    result = {
        "date": dates[-1],
        "time_points": dates,
        "sectors": sectors_list,
        "deleted_dates": deleted_dates  # 新增字段
    }

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

async def lifespan(app: FastAPI):
    init_db()                           # 初始化行业数据库（创建表）
    init_stock_db()                     # 初始化个股数据库（创建表）
    _init_allstock_db()                 # 初始化全部个股数据库（创建表）
    _load_allstock_from_db()            # 从数据库加载今天已有的全部个股数据
    # 启动时自动开始分时采集
    ok, msg = start_collection()
    if ok:
        print(f"🚀 服务启动，自动开始分时采集: {msg}")
    else:
        print(f"⚠️️  服务启动，自动采集未启动: {msg}")
    # 启动后台监控任务：定期检查采集线程是否存活
    import asyncio as _asyncio
    _asyncio.create_task(_check_collection_alive())
    # 启动后台任务：交易日每天15:00收盘后自动获取全部个股数据
    _asyncio.create_task(_auto_fetch_allstock_loop())
    yield                               # 应用运行中...
    # （应用关闭后的清理代码可以写在这里）

app = FastAPI(lifespan=lifespan)        # 创建FastAPI应用实例
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")  # 挂载静态文件目录

@app.get("/")                           # 根路径：返回前端页面
async def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))  # 返回index.html文件

# --- 每日数据API ---
@app.get("/api/daily/history")
async def daily_history():
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
    
    # 标记被删除的日期（数据库中不存在的日期）
    deleted_dates = [d for d in dates if d not in record_by_date]
    
    return {
        "date": dates[-1],
        "time_points": dates,
        "sectors": sectors_list,
        "deleted_dates": deleted_dates  # 新增字段
    }

# --- 每日数据API（支持自定义天数） ---
@app.get("/api/daily/history/{days}")
async def daily_history_range(days: int):
    """获取指定天数的每日历史数据"""
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
    for i in range(days - 1, -1, -1):
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
    
    deleted_dates = [d for d in dates if d not in record_by_date]
    
    return {
        "date": dates[-1] if dates else "",
        "time_points": dates,
        "sectors": sectors_list,
        "deleted_dates": deleted_dates,
        "days": days
    }

# --- 热力图数据API ---
@app.get("/api/heatmap/{days}")
async def heatmap_data(days: int = 30):
    """
    获取热力图数据：返回指定天数内每个行业每天的净流入值
    返回格式: { dates: [...], sectors: [{name, values: [...]}, ...] }
    """
    conn = get_db(DB_DAILY)
    rows = conn.execute(
        "SELECT date, sectors FROM fund_flow ORDER BY date ASC"
    ).fetchall()
    conn.close()
    
    # 计算日期范围
    today_date = datetime.now().date()
    dates = []
    for i in range(days - 1, -1, -1):
        d = today_date - timedelta(days=i)
        dates.append(d.strftime("%Y-%m-%d"))
    
    # 按日期索引记录
    record_by_date = {}
    for row in rows:
        record_by_date[row["date"]] = json.loads(row["sectors"])
    
    # 获取所有行业名（从最近有数据的记录中获取）
    all_sector_names_set = set()
    for date_str in dates:
        if date_str in record_by_date:
            sectors = record_by_date[date_str]
            if sectors:
                for s in sectors:
                    all_sector_names_set.add(s["name"])
    all_sector_names = sorted(all_sector_names_set)
    
    # 构建每个行业的数据
    sectors_list = []
    for name in all_sector_names:
        values = []
        for date_str in dates:
            if date_str in record_by_date:
                sectors = record_by_date[date_str]
                val = 0
                for s in sectors:
                    if s["name"] == name:
                        val = s["values"][240] if len(s["values"]) > 240 else 0
                        break
                values.append(val)
            else:
                values.append(0)
        sectors_list.append({"name": name, "values": values})
    
    return {
        "dates": dates,
        "sectors": sectors_list
    }
  
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
            # 同时更新240点（15:00点）的数据
            if len(tp) > 241:  # 有241点存在
                for s in existing["sectors"]:
                    s["values"][240] = snapshot.get(s["name"], 0)
        else:
            # 最后一个点已经是动态点 → 更新它（不新增）
            tp[-1] = current_hm
            idx = len(tp) - 1
            # 如果当前有241点，也更新240点
            if len(tp) > 241:
                for s in existing["sectors"]:
                    s["values"][240] = snapshot.get(s["name"], 0)
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
    
@app.get("/api/daily/delete_point")
async def daily_delete_point(date: str = ""):
    if not date:
        return JSONResponse(status_code=400, content={"error": "缺少日期参数"})

    conn = get_db(DB_DAILY)
    
    # 直接删除该日期的数据库记录
    conn.execute("DELETE FROM fund_flow WHERE date LIKE ?", (f"%{date}%",))
    conn.commit()
    
    # 重新读取所有记录
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

    # 构造30天数据返回
    today_date = datetime.now().date()
    dates = []
    for i in range(29, -1, -1):
        d = today_date - timedelta(days=i)
        dates.append(d.strftime("%Y-%m-%d"))

    record_by_date = {rec["date"]: rec for rec in records}

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

    # 返回额外字段：标记哪些日期是被删除的（数据库中不存在的日期）
    deleted_dates = [d for d in dates if d not in record_by_date]

    return {
        "date": dates[-1],
        "time_points": dates,
        "sectors": sectors_list,
        "deleted_dates": deleted_dates  # 新增字段，告诉前端哪些日期被删除了
    }

# ==================== 趋势分析API ====================
@app.get("/api/trend/analysis/{days}")
async def trend_analysis(days: int = 30):
    """
    趋势分析API：基于历史资金流向数据，计算技术指标并给出买入/卖出建议
    
    指标说明：
    1. MA5/MA10/MA20: 5/10/20日移动平均净流入
    2. momentum: 动量指标（短期vs长期资金流向变化率）
    3. trend_strength: 趋势强度（0-100，类似RSI）
    4. volatility: 波动率（资金流向的稳定性）
    5. score: 综合评分（0-100，越高越适合买入）
    6. signal: 买入/卖出/观望信号
    """
    conn = get_db(DB_DAILY)
    rows = conn.execute(
        "SELECT date, sectors FROM fund_flow ORDER BY date ASC"
    ).fetchall()
    conn.close()
    
    # 获取日期范围（需要额外历史数据用于计算MA20）
    today_date = datetime.now().date()
    lookback = max(days, 60)  # 至少需要60天历史数据
    all_dates = []
    for i in range(lookback - 1, -1, -1):
        d = today_date - timedelta(days=i)
        all_dates.append(d.strftime("%Y-%m-%d"))
    
    # 按日期索引记录
    record_by_date = {}
    for row in rows:
        record_by_date[row["date"]] = json.loads(row["sectors"])
    
    # 获取所有行业名
    all_sector_names = set()
    for date_str in all_dates:
        if date_str in record_by_date:
            sectors = record_by_date[date_str]
            if sectors:
                for s in sectors:
                    all_sector_names.add(s["name"])
    all_sector_names = sorted(all_sector_names)
    
    # 构建每个行业的完整时间序列
    sector_data = {}
    for name in all_sector_names:
        values = []
        for date_str in all_dates:
            if date_str in record_by_date:
                sectors = record_by_date[date_str]
                val = 0
                for s in sectors:
                    if s["name"] == name:
                        val = s["values"][240] if len(s["values"]) > 240 else 0
                        break
                values.append(val)
            else:
                values.append(0)
        sector_data[name] = values
    
    # 计算每个行业的技术指标
    analysis_results = []
    
    for name in all_sector_names:
        values = sector_data[name]
        if len(values) < 20:
            continue
        
        # 取最近days天的数据用于分析
        recent_values = values[-days:] if len(values) >= days else values
        
        # 1. 移动平均线
        def calc_ma(data, period):
            if len(data) < period:
                return sum(data) / len(data) if data else 0
            return sum(data[-period:]) / period
        
        ma5 = calc_ma(values, 5)
        ma10 = calc_ma(values, 10)
        ma20 = calc_ma(values, 20)
        
        # 2. 动量指标：短期(5日)均值 vs 长期(20日)均值的百分比变化
        short_avg = calc_ma(values, 5)
        long_avg = calc_ma(values, 20)
        momentum = ((short_avg - long_avg) / abs(long_avg) * 100) if long_avg != 0 else 0
        
        # 3. 趋势强度（类似RSI）
        # 计算上涨日和下跌日的比例
        gains = []
        losses = []
        for i in range(1, len(recent_values)):
            change = recent_values[i] - recent_values[i-1]
            if change > 0:
                gains.append(change)
            else:
                losses.append(abs(change))
        
        avg_gain = sum(gains) / len(gains) if gains else 0
        avg_loss = sum(losses) / len(losses) if losses else 1
        rs = avg_gain / avg_loss if avg_loss != 0 else 100
        trend_strength = 100 - (100 / (1 + rs))  # 0-100，>50偏多，<50偏空
        
        # 4. 波动率（标准差）
        mean_val = sum(recent_values) / len(recent_values)
        variance = sum((x - mean_val) ** 2 for x in recent_values) / len(recent_values)
        volatility = variance ** 0.5
        
        # 5. 累计净流入
        total_inflow = sum(recent_values)
        
        # 6. 最近趋势（最近5天vs之前5天）
        recent_5 = sum(values[-5:]) if len(values) >= 5 else sum(values)
        prev_5 = sum(values[-10:-5]) if len(values) >= 10 else sum(values[:5])
        recent_trend = recent_5 - prev_5
        
        # 7. MA排列信号
        ma_bullish = ma5 > ma10 > ma20  # 多头排列
        ma_bearish = ma5 < ma10 < ma20  # 空头排列
        
        # ===== 新增：长期趋势分析 =====
        # 8. 长期趋势方向（使用全部可用数据，至少30天）
        all_values = values  # 完整历史数据
        long_term_avg = sum(all_values) / len(all_values) if all_values else 0
        # 长期趋势：最近20日均值 vs 全部历史均值
        recent_20_avg = calc_ma(values, 20)
        long_trend_dir = recent_20_avg - long_term_avg  # 正值=高于长期均值，负值=低于
        
        # 9. 趋势斜率（线性回归斜率，反映趋势方向和速度）
        n = len(recent_values)
        if n >= 5:
            x_mean = (n - 1) / 2.0
            y_mean = sum(recent_values) / n
            numerator = sum((i - x_mean) * (recent_values[i] - y_mean) for i in range(n))
            denominator = sum((i - x_mean) ** 2 for i in range(n))
            slope = numerator / denominator if denominator != 0 else 0
        else:
            slope = 0
        
        # 10. 资金流入一致性（正流入天数占比）
        positive_days = sum(1 for v in recent_values if v > 0)
        consistency = positive_days / len(recent_values) * 100 if recent_values else 50
        
        # 11. 趋势衰减/加速（后半段vs前半段）
        half = len(recent_values) // 2
        if half > 0:
            first_half_avg = sum(recent_values[:half]) / half
            second_half_avg = sum(recent_values[half:]) / (len(recent_values) - half)
            trend_accel = second_half_avg - first_half_avg  # 正值=加速流入/减速流出
        else:
            trend_accel = 0
        
        # 12. 当前值相对历史分位（0-100，越高说明当前流入越多）
        sorted_vals = sorted(all_values)
        current_val = values[-1] if values else 0
        percentile = 0
        for i, v in enumerate(sorted_vals):
            if v <= current_val:
                percentile = (i + 1) / len(sorted_vals) * 100
        
        # ===== 综合评分（0-100）=====
        score = 50  # 基础分
        
        # A. 短期动量贡献（-10 ~ +10）降低权重
        score += max(-10, min(10, momentum * 1))
        
        # B. 趋势强度RSI贡献（-10 ~ +10）
        score += (trend_strength - 50) * 0.2
        
        # C. MA排列贡献（-8 ~ +8）
        if ma_bullish:
            score += 8
        elif ma_bearish:
            score -= 8
        
        # D. 最近趋势贡献（-8 ~ +8）
        if recent_trend > 0:
            score += min(8, recent_trend / 10)
        else:
            score += max(-8, recent_trend / 10)
        
        # E. 长期趋势方向贡献（-15 ~ +15）★ 重要：长期下行行业大幅扣分
        if long_term_avg < 0:
            # 长期净流出行业（如房地产）：即使短期反弹也要扣分
            score += max(-15, min(5, long_trend_dir * 1.5))
        else:
            # 长期净流入行业：正常评分
            score += max(-10, min(15, long_trend_dir * 1.5))
        
        # F. 趋势斜率贡献（-10 ~ +10）★ 重要：持续下行趋势扣分
        score += max(-10, min(10, slope * 5))
        
        # G. 资金流入一致性贡献（-8 ~ +8）
        score += (consistency - 50) * 0.16
        
        # H. 趋势加速/减速贡献（-5 ~ +5）
        score += max(-5, min(5, trend_accel * 3))
        
        # I. 累计净流入贡献（-3 ~ +3）降低权重
        score += max(-3, min(3, total_inflow / 80))
        
        # J. 历史分位贡献（-3 ~ +3）
        score += (percentile - 50) * 0.06
        
        # 限制在0-100范围
        score = max(0, min(100, score))
        
        # 9. 生成信号
        if score >= 70:
            signal = "强烈买入"
            signal_color = "#ff1744"  # 红色
        elif score >= 60:
            signal = "建议买入"
            signal_color = "#ff5252"
        elif score >= 45:
            signal = "观望"
            signal_color = "#ffc107"  # 黄色
        elif score >= 35:
            signal = "建议减仓"
            signal_color = "#4caf50"  # 绿色
        else:
            signal = "建议卖出"
            signal_color = "#00c853"
        
        analysis_results.append({
            "name": name,
            "ma5": round(ma5, 2),
            "ma10": round(ma10, 2),
            "ma20": round(ma20, 2),
            "momentum": round(momentum, 2),
            "trend_strength": round(trend_strength, 2),
            "volatility": round(volatility, 2),
            "total_inflow": round(total_inflow, 2),
            "recent_trend": round(recent_trend, 2),
            "ma_bullish": ma_bullish,
            "ma_bearish": ma_bearish,
            "long_term_avg": round(long_term_avg, 2),
            "slope": round(slope, 3),
            "consistency": round(consistency, 1),
            "trend_accel": round(trend_accel, 2),
            "percentile": round(percentile, 1),
            "score": round(score, 1),
            "signal": signal,
            "signal_color": signal_color,
            "recent_values": recent_values[-days:]  # 返回最近N天数据
        })
    
    # 按评分排序
    analysis_results.sort(key=lambda x: x["score"], reverse=True)
    
    return {
        "analysis_date": today_date.strftime("%Y-%m-%d"),
        "period_days": days,
        "sectors": analysis_results,
        "summary": {
            "strong_buy": len([s for s in analysis_results if s["score"] >= 70]),
            "buy": len([s for s in analysis_results if 60 <= s["score"] < 70]),
            "hold": len([s for s in analysis_results if 45 <= s["score"] < 60]),
            "sell": len([s for s in analysis_results if 35 <= s["score"] < 45]),
            "strong_sell": len([s for s in analysis_results if s["score"] < 35])
        }
    }

# ==================== 个股API接口 ====================
@app.get("/api/stock/intraday")
async def stock_intraday():
    """获取当天个股分时数据"""
    d = load_stock_intraday()
    if d:
        return d
    # 无数据时返回空结构
    stocks = [{"code": s["code"], "name": s["name"], "values": [0.0] * 241}
              for s in STOCK_LIST if s["code"]]
    return {"date": datetime.now().strftime("%Y-%m-%d"), "time_points": INTRADAY_POINTS, "stocks": stocks}

@app.get("/api/stock/daily/{days}")
async def stock_daily_history(days: int = 30):
    """获取个股每日历史数据"""
    conn = get_db(DB_STOCK_DAILY)
    rows = conn.execute(
        "SELECT date, time_points, stocks FROM stock_daily ORDER BY date ASC"
    ).fetchall()
    conn.close()

    records = []
    for row in rows:
        records.append({
            "date": row["date"],
            "time_points": json.loads(row["time_points"]),
            "stocks": json.loads(row["stocks"])
        })

    today_date = datetime.now().date()
    dates = []
    for i in range(days - 1, -1, -1):
        d = today_date - timedelta(days=i)
        dates.append(d.strftime("%Y-%m-%d"))

    record_by_date = {rec["date"]: rec for rec in records}

    # 用STOCK_LIST中的有效股票作为基准
    stocks_list = []
    for s in STOCK_LIST:
        if not s["code"]:
            continue
        values = []
        for date_str in dates:
            if date_str in record_by_date:
                rec = record_by_date[date_str]
                val = 0
                for st in rec["stocks"]:
                    if st["code"] == s["code"]:
                        val = st["values"][240] if len(st["values"]) > 240 else 0
                        break
                values.append(val)
            else:
                values.append(0)
        stocks_list.append({"code": s["code"], "name": s["name"], "values": values})

    deleted_dates = [d for d in dates if d not in record_by_date]
    return {
        "date": dates[-1] if dates else "",
        "time_points": dates,
        "stocks": stocks_list,
        "deleted_dates": deleted_dates,
        "days": days
    }

@app.post("/api/stock/refresh")
async def stock_refresh():
    """手动刷新个股分时数据：立即获取一次个股快照并保存到数据库"""
    today = datetime.now().strftime("%Y-%m-%d")
    now = datetime.now()
    current_hm = f"{now.hour:02d}:{now.minute:02d}"

    print(f"📈 手动刷新个股数据...")
    stock_snapshot = _fetch_stock_snapshot()
    if not stock_snapshot or all(s["value"] == 0 for s in stock_snapshot):
        return {"error": "个股API返回空数据，请检查网络或稍后重试"}

    # 加载已有的分时数据
    existing = load_stock_intraday()
    if existing and existing["date"] == today and existing.get("stocks"):
        tp = existing["time_points"]
        # 更新最后一个动态点
        last_tp = tp[-1] if tp else ""
        is_fixed = last_tp in INTRADAY_POINTS
        if is_fixed:
            tp.append(current_hm)
            for s in existing["stocks"]:
                s["values"].append(0.0)
            idx = len(tp) - 1
            if len(tp) > 241:
                for s in existing["stocks"]:
                    s["values"][240] = next((ss["value"] for ss in stock_snapshot if ss["code"] == s["code"]), 0)
        else:
            tp[-1] = current_hm
            idx = len(tp) - 1
            if len(tp) > 241:
                for s in existing["stocks"]:
                    s["values"][240] = next((ss["value"] for ss in stock_snapshot if ss["code"] == s["code"]), 0)
        for s in existing["stocks"]:
            s["values"][idx] = next((ss["value"] for ss in stock_snapshot if ss["code"] == s["code"]), 0)
        save_stock_intraday(today, tp, existing["stocks"])
        stocks_data = existing["stocks"]
    else:
        # 首次创建
        time_points = list(INTRADAY_POINTS)
        stocks_data = []
        for s in STOCK_LIST:
            if not s["code"]:
                continue
            values = [0.0] * 241
            val = next((ss["value"] for ss in stock_snapshot if ss["code"] == s["code"]), 0)
            values[240] = val  # 填入15:00点
            stocks_data.append({"code": s["code"], "name": s["name"], "values": values})
        save_stock_intraday(today, time_points, stocks_data)

    top3 = sorted([(s["name"], s["value"]) for s in stock_snapshot if s["code"]],
                   key=lambda x: x[1], reverse=True)[:3]
    top3_str = ", ".join(f"{n}({v:+.2f})" for n, v in top3)
    print(f"✅ 个股数据已刷新 | TOP3: {top3_str}")

    return {"date": today, "time_points": INTRADAY_POINTS, "stocks": stocks_data}

@app.post("/api/stock/merge_intraday")
async def stock_merge_intraday():
    """将个股分时数据合并到每日数据库"""
    try:
        result, err = do_merge_stock_intraday_to_daily()
        if err:
            return JSONResponse(status_code=400, content={"error": err})
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

def do_merge_stock_intraday_to_daily():
    """将个股分时数据合并到个股每日数据库"""
    intraday = load_stock_intraday()
    if not intraday or not intraday.get("stocks"):
        return None, "无个股分时数据可合并"

    today = datetime.now().strftime("%Y-%m-%d")

    conn = get_db(DB_STOCK_DAILY)
    rows = conn.execute(
        "SELECT date, time_points, stocks FROM stock_daily ORDER BY date ASC"
    ).fetchall()

    records = []
    for row in rows:
        records.append({
            "date": row["date"],
            "time_points": json.loads(row["time_points"]),
            "stocks": json.loads(row["stocks"])
        })

    found = False
    for rec in records:
        if rec["date"] == today:
            rec["time_points"] = intraday["time_points"]
            rec["stocks"] = intraday["stocks"]
            found = True
            break

    if not found:
        records.append({
            "date": today,
            "time_points": intraday["time_points"],
            "stocks": intraday["stocks"]
        })

    MAX_DAYS = 1080
    while len(records) > MAX_DAYS:
        removed = records.pop(0)
        print(f"🗑️ 删除过期个股数据: {removed['date']}")

    conn.execute("DELETE FROM stock_daily")
    for rec in records:
        conn.execute(
            "INSERT INTO stock_daily (date, time_points, stocks) VALUES (?, ?, ?)",
            (rec["date"],
             json.dumps(rec["time_points"], ensure_ascii=False),
             json.dumps(rec["stocks"], ensure_ascii=False))
        )
    conn.commit()
    conn.close()

    # 构造返回数据
    conn = get_db(DB_STOCK_DAILY)
    rows = conn.execute(
        "SELECT date, time_points, stocks FROM stock_daily ORDER BY date ASC"
    ).fetchall()
    conn.close()

    records = []
    for row in rows:
        records.append({
            "date": row["date"],
            "time_points": json.loads(row["time_points"]),
            "stocks": json.loads(row["stocks"])
        })

    today_date = datetime.now().date()
    dates = []
    for i in range(29, -1, -1):
        d = today_date - timedelta(days=i)
        dates.append(d.strftime("%Y-%m-%d"))

    record_by_date = {rec["date"]: rec for rec in records}

    stocks_list = []
    for s in STOCK_LIST:
        if not s["code"]:
            continue
        values = []
        for date_str in dates:
            if date_str in record_by_date:
                rec = record_by_date[date_str]
                val = 0
                for st in rec["stocks"]:
                    if st["code"] == s["code"]:
                        val = st["values"][240] if len(st["values"]) > 240 else 0
                        break
                values.append(val)
            else:
                values.append(0)
        stocks_list.append({"code": s["code"], "name": s["name"], "values": values})

    deleted_dates = [d for d in dates if d not in record_by_date]

    result = {
        "date": dates[-1],
        "time_points": dates,
        "stocks": stocks_list,
        "deleted_dates": deleted_dates
    }

    print(f"✅ 个股分时数据已合并到每日: {len(intraday['stocks'])} 只股票 × 241个时间点 | 共{len(records)}天数据")
    return result, None

# ==================== 全部个股资金流（独立模块，与现有功能解耦） ====================
DB_ALLSTOCK = "allstock.db"             # 全部个股资金流数据库文件名
_all_stock_flow_data = []          # 内存中缓存的全部个股数据列表
_all_stock_flow_status = {
    "running": False,               # 是否正在采集
    "total_pages": 0,               # 总页数
    "current_page": 0,              # 当前已采集到第几页
    "total_stocks": 0,              # 已获取的股票数量
    "last_update": "",              # 最后更新时间
    "error": "",                    # 错误信息
    "is_complete": False,           # 本轮采集是否已全部完成
}
_all_stock_flow_thread = None       # 后台采集线程


def _init_allstock_db():
    """初始化全部个股数据库，创建表结构"""
    conn = sqlite3.connect(DB_ALLSTOCK)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS allstock_data (
            fetch_date TEXT NOT NULL,
            page_num INTEGER NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            price TEXT,
            change_pct TEXT,
            turnover_rate TEXT,
            flow_in TEXT,
            flow_out TEXT,
            net_flow TEXT,
            turnover TEXT,
            PRIMARY KEY (fetch_date, stock_code)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS allstock_progress (
            fetch_date TEXT PRIMARY KEY,
            total_pages INTEGER DEFAULT 0,
            current_page INTEGER DEFAULT 0,
            total_stocks INTEGER DEFAULT 0,
            last_update TEXT DEFAULT '',
            is_complete INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    conn.close()


def _load_allstock_from_db():
    """从数据库加载最近一次的全部个股数据到内存（优先今天，其次最近有数据的日期）"""
    global _all_stock_flow_data, _all_stock_flow_status
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_ALLSTOCK)
    c = conn.cursor()

    # 优先加载今天的数据，如果没有则加载最近有数据的日期
    load_date = today
    c.execute("SELECT total_pages, current_page, total_stocks, last_update, is_complete FROM allstock_progress WHERE fetch_date=?", (today,))
    row = c.fetchone()
    if not row:
        # 今天没有数据，查找最近有数据的日期
        c.execute("SELECT fetch_date, total_pages, current_page, total_stocks, last_update, is_complete FROM allstock_progress ORDER BY fetch_date DESC LIMIT 1")
        row2 = c.fetchone()
        if row2:
            load_date = row2[0]
            row = row2[1:]  # 去掉fetch_date字段

    if row:
        _all_stock_flow_status["total_pages"] = row[0]
        _all_stock_flow_status["current_page"] = row[1]
        _all_stock_flow_status["total_stocks"] = row[2]
        _all_stock_flow_status["last_update"] = row[3]
        _all_stock_flow_status["is_complete"] = bool(row[4])

    # 加载数据
    c.execute("SELECT stock_code, stock_name, price, change_pct, turnover_rate, flow_in, flow_out, net_flow, turnover FROM allstock_data WHERE fetch_date=? ORDER BY CAST(NULLIF(change_pct,'') AS REAL) DESC", (load_date,))
    rows = c.fetchall()
    _all_stock_flow_data = [
        {"股票代码": r[0], "股票简称": r[1], "最新价": r[2], "涨跌幅": r[3],
         "换手率": r[4], "流入资金": r[5], "流出资金": r[6], "净额": r[7], "成交额": r[8]}
        for r in rows
    ]
    conn.close()
    if _all_stock_flow_data:
        print(f"📂 从数据库加载全部个股数据: {len(_all_stock_flow_data)} 只股票 ({load_date}, 第{_all_stock_flow_status['current_page']}/{_all_stock_flow_status['total_pages']}页)")


def _save_allstock_page_to_db(page_num, rows, today):
    """将一页的数据保存到数据库"""
    conn = sqlite3.connect(DB_ALLSTOCK)
    c = conn.cursor()
    for row in rows:
        c.execute("""
            INSERT OR REPLACE INTO allstock_data (fetch_date, page_num, stock_code, stock_name, price, change_pct, turnover_rate, flow_in, flow_out, net_flow, turnover)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (today, page_num, row["股票代码"], row["股票简称"], str(row["最新价"]),
              str(row["涨跌幅"]), str(row["换手率"]), str(row["流入资金"]),
              str(row["流出资金"]), str(row["净额"]), str(row["成交额"])))
    conn.commit()
    conn.close()


def _save_allstock_progress(today):
    """保存采集进度到数据库"""
    conn = sqlite3.connect(DB_ALLSTOCK)
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO allstock_progress (fetch_date, total_pages, current_page, total_stocks, last_update, is_complete)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (today, _all_stock_flow_status["total_pages"], _all_stock_flow_status["current_page"],
          _all_stock_flow_status["total_stocks"], _all_stock_flow_status["last_update"],
          1 if _all_stock_flow_status["is_complete"] else 0))
    conn.commit()
    conn.close()


def _fetch_all_stock_flow_worker(start_page=1):
    """
    后台线程：分页请求同花顺全部个股资金流数据
    - 支持断点续采：从 start_page 开始
    - 每页间隔10秒，均匀获取
    - 每获取一页就实时写入数据库并更新内存缓存
    """
    global _all_stock_flow_data, _all_stock_flow_status

    _all_stock_flow_status["running"] = True
    _all_stock_flow_status["error"] = ""
    today = datetime.now().strftime("%Y-%m-%d")

    if start_page > 1:
        print(f"🔄 断点续采：从第 {start_page} 页继续获取全部个股资金流数据...")
    else:
        print("🚀 开始获取全部个股资金流数据...")

    try:
        # --- 第1步：请求第1页，获取总页数 ---
        v_code = _get_v_code()
        headers = {
            "Accept": "text/html, */*; q=0.01",
            "hexin-v": v_code,
            "Host": "data.10jqka.com.cn",
            "Referer": "http://data.10jqka.com.cn/funds/hyzjl/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "X-Requested-With": "XMLHttpRequest",
        }
        first_url = "http://data.10jqka.com.cn/funds/ggzjl/field/code/order/desc/ajax/1/free/1/"
        r = requests.get(first_url, headers=headers, timeout=15)
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(r.text, features="lxml")
        page_info = soup.find(name="span", attrs={"class": "page_info"})
        if page_info:
            total_pages = int(page_info.text.split("/")[1])
        else:
            total_pages = 1
        _all_stock_flow_status["total_pages"] = total_pages
        print(f"📄 总页数: {total_pages}")

        # --- 第2步：逐页请求，每页间隔10秒 ---
        page_url_tpl = "http://data.10jqka.com.cn/funds/ggzjl/field/zdf/order/desc/page/{}/ajax/1/free/1/"

        for page in range(start_page, total_pages + 1):
            if not _all_stock_flow_status["running"]:
                print(f"⏹️ 全部个股采集被手动停止 (已采集到第 {page-1} 页，数据已保存)")
                break

            # 每页重新生成验证码
            v_code = _get_v_code()
            headers["hexin-v"] = v_code

            try:
                r = requests.get(page_url_tpl.format(page), headers=headers, timeout=15)
                temp_df = pd.read_html(StringIO(r.text))[0]
                # 删除序号列
                if "序号" in temp_df.columns:
                    del temp_df["序号"]
                # 统一列名
                temp_df.columns = ["股票代码", "股票简称", "最新价", "涨跌幅", "换手率", "流入资金", "流出资金", "净额", "成交额"]
                page_rows = [row.to_dict() for _, row in temp_df.iterrows()]

                # 实时写入数据库
                _save_allstock_page_to_db(page_num=page, rows=page_rows, today=today)

                # 实时更新内存缓存（从数据库重新加载，保证排序一致）
                _load_allstock_from_db()

                _all_stock_flow_status["current_page"] = page
                _all_stock_flow_status["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                _all_stock_flow_status["is_complete"] = False
                # 保存进度
                _save_allstock_progress(today)
                print(f"  📥 第 {page}/{total_pages} 页完成，累计 {len(_all_stock_flow_data)} 只股票")

            except Exception as e:
                print(f"  ⚠️ 第 {page} 页请求失败: {e}")
                _all_stock_flow_status["error"] = f"第{page}页失败: {e}"

            # 非最后一页，等待10秒
            if page < total_pages and _all_stock_flow_status["running"]:
                time.sleep(10)

        # --- 第3步：标记完成 ---
        if _all_stock_flow_status["current_page"] >= total_pages:
            _all_stock_flow_status["is_complete"] = True
            _all_stock_flow_status["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            _save_allstock_progress(today)
            print(f"✅ 全部个股数据获取完成: {len(_all_stock_flow_data)} 只股票")

    except Exception as e:
        _all_stock_flow_status["error"] = str(e)
        print(f"❌ 全部个股采集失败: {e}")
    finally:
        _all_stock_flow_status["running"] = False


async def _auto_fetch_allstock_loop():
    """
    后台任务：交易日每天15:00收盘后自动获取全部个股数据(约5210只)，每天仅一次。
    服务启动/盘中只读数据库不拉取全市场；仅在交易日15:00后、且今天未采集完成时自动触发。
    """
    global _all_stock_flow_data, _all_stock_flow_thread, _all_stock_flow_status
    while True:
        try:
            now = datetime.now()
            is_weekday = now.weekday() < 5
            t_min = now.hour * 60 + now.minute
            if is_weekday and t_min >= 15 * 60 and not _all_stock_flow_status["running"]:
                today = now.strftime("%Y-%m-%d")
                # 检查今天是否已完成采集
                conn = sqlite3.connect(DB_ALLSTOCK)
                row = conn.execute(
                    "SELECT is_complete FROM allstock_progress WHERE fetch_date=?", (today,)
                ).fetchone()
                conn.close()
                if not row or not row[0]:
                    # 今天未完成 → 自动启动（全新开始或断点续采）
                    start_page = 1
                    if _all_stock_flow_status["current_page"] > 0 and not _all_stock_flow_status["is_complete"]:
                        start_page = _all_stock_flow_status["current_page"] + 1
                        print(f"🕒 交易日收盘自动获取：从第 {start_page} 页断点续采全部个股数据...")
                    else:
                        # 全新开始：清空今天的旧数据
                        conn2 = sqlite3.connect(DB_ALLSTOCK)
                        c = conn2.cursor()
                        c.execute("DELETE FROM allstock_data WHERE fetch_date=?", (today,))
                        c.execute("DELETE FROM allstock_progress WHERE fetch_date=?", (today,))
                        conn2.commit()
                        conn2.close()
                        _all_stock_flow_data = []
                        _all_stock_flow_status = {
                            "running": False, "total_pages": 0, "current_page": 0,
                            "total_stocks": 0, "last_update": "", "error": "", "is_complete": False,
                        }
                        print("🕒 交易日收盘(15:00)自动获取全部个股数据...")
                    _all_stock_flow_status["running"] = True
                    _all_stock_flow_thread = threading.Thread(
                        target=_fetch_all_stock_flow_worker, args=(start_page,), daemon=True
                    )
                    _all_stock_flow_thread.start()
        except Exception as e:
            print(f"⚠️ 自动获取全部个股数据检查异常: {e}")
        await asyncio.sleep(60)


@app.post("/api/allstock/start")
async def allstock_start():
    """启动全部个股数据采集（支持断点续采）"""
    global _all_stock_flow_thread, _all_stock_flow_status
    if _all_stock_flow_status["running"]:
        return {"ok": False, "msg": "采集已在进行中"}

    today = datetime.now().strftime("%Y-%m-%d")

    # 判断是否需要断点续采
    start_page = 1
    if _all_stock_flow_status["current_page"] > 0 and not _all_stock_flow_status["is_complete"]:
        # 有未完成的进度，从下一页继续
        start_page = _all_stock_flow_status["current_page"] + 1
        msg = f"断点续采：从第 {start_page} 页继续"
    else:
        # 全新开始或上次已完成：清空今天的数据重新采集
        conn = sqlite3.connect(DB_ALLSTOCK)
        c = conn.cursor()
        c.execute("DELETE FROM allstock_data WHERE fetch_date=?", (today,))
        c.execute("DELETE FROM allstock_progress WHERE fetch_date=?", (today,))
        conn.commit()
        conn.close()
        global _all_stock_flow_data
        _all_stock_flow_data = []
        _all_stock_flow_status = {
            "running": False, "total_pages": 0, "current_page": 0,
            "total_stocks": 0, "last_update": "", "error": "", "is_complete": False,
        }
        msg = "采集已启动（全新开始）"

    _all_stock_flow_status["running"] = True
    _all_stock_flow_thread = threading.Thread(target=_fetch_all_stock_flow_worker, args=(start_page,), daemon=True)
    _all_stock_flow_thread.start()
    return {"ok": True, "msg": msg}


@app.post("/api/allstock/stop")
async def allstock_stop():
    """停止全部个股数据采集（已获取的数据保留在数据库中）"""
    global _all_stock_flow_status
    _all_stock_flow_status["running"] = False
    return {"ok": True, "msg": f"采集已停止，已获取 {_all_stock_flow_status['total_stocks']} 只股票数据已保存"}


@app.get("/api/allstock/status")
async def allstock_status():
    """获取采集状态"""
    return _all_stock_flow_status


@app.get("/api/allstock/data")
async def allstock_data():
    """获取全部个股数据（从内存缓存读取，包含实时采集的增量数据）"""
    return {
        "data": _all_stock_flow_data,
        "status": _all_stock_flow_status,
    }


# ==================== 程序入口 ====================
if __name__ == "__main__":              # 直接运行此文件时执行
    print("🚀 启动服务器...")            # 控制台提示
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")
    # 启动uvicorn服务器：
    #   app: FastAPI应用实例
    #   host="0.0.0.0": 监听所有网络接口（允许局域网访问）
    #   port=8000: 端口号8000
    #   log_level="info": 日志级别
