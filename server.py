import asyncio
import requests
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles
from datetime import datetime

app = FastAPI()

# 挂载静态文件，让前端能访问
app.mount("/static", StaticFiles(directory="static"), name="static")

# 东方财富行业板块资金流接口
EM_URL = "https://push2.eastmoney.com/api/qt/clist/get"
PARAMS = {
    "pn": 1,
    "pz": 80,  # 获取80个行业板块
    "po": 1,
    "np": 1,
    "fltt": 2,
    "fid": "f62",
    "fs": "m:90+t:2",  # 行业板块
    "fields": "f12,f14,f62"
}
HEADERS = {"User-Agent": "Mozilla/5.0"}

def is_trading_time():
    """判断是否在交易时间段"""
    now = datetime.now()
    # 周一到周五
    if now.weekday() >= 5:
        return False
    t = now.hour * 100 + now.minute
    # 上午 9:30-11:30，下午 13:00-15:00
    return (930 <= t <= 1129) or (1300 <= t <= 1459)

@app.get("/")
async def root():
    """重定向到首页"""
    return {"message": "请访问 /static/index.html"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("客户端已连接")
    
    while True:
        try:
            if is_trading_time():
                # 拉取东方财富数据
                resp = requests.get(EM_URL, params=PARAMS, headers=HEADERS, timeout=5)
                data = resp.json()
                
                if data.get("data") and data["data"].get("diff"):
                    items = data["data"]["diff"]
                    # 提取板块名称和主力净流入
                    result = []
                    for item in items:
                        name = item.get("f14", "")
                        net = item.get("f62", 0)
                        if name and net is not None:
                            result.append({"name": name, "net": net})
                    
                    # 按净流入从大到小排序
                    result.sort(key=lambda x: x["net"], reverse=True)
                    
                    # 发送给前端
                    await websocket.send_json(result)
                    print(f"已推送 {len(result)} 个板块数据")
            
            # 非交易时间也发一条消息告诉前端
            else:
                await websocket.send_json([])
            
            # 每3秒轮询一次
            await asyncio.sleep(3)
            
        except Exception as e:
            print(f"错误: {e}")
            await asyncio.sleep(3)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)