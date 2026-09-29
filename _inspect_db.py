# -*- coding: utf-8 -*-
import sqlite3, os
base = r'C:\Users\Administrator\Desktop\stock_server\stock_server'
for db in ['allstock.db', 'fund_flow.db', 'stock_daily.db', 'stock_intraday.db', 'fund_flow_intraday.db']:
    p = os.path.join(base, db)
    print('=' * 60)
    print(db, os.path.getsize(p), 'bytes')
    conn = sqlite3.connect(p)
    c = conn.cursor()
    tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    print('tables:', tables)
    for t in tables:
        cnt = c.execute('SELECT COUNT(*) FROM ' + t).fetchone()[0]
        print('  %s: %d rows' % (t, cnt))
        # 列出表结构
        cols = [r[1] for r in c.execute('PRAGMA table_info(' + t + ')').fetchall()]
        print('    cols:', cols)
        # 按日期统计（如果有日期列）
        for dcol in ['fetch_date', 'date', 'trade_date', 'day']:
            if dcol in cols:
                rows = c.execute('SELECT %s, COUNT(*) FROM %s GROUP BY %s ORDER BY %s DESC LIMIT 6' % (dcol, t, dcol, dcol)).fetchall()
                print('    by %s:' % dcol, rows)
                break
    conn.close()
