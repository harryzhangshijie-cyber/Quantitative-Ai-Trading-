import dolphindb as ddb
import pandas as pd
import vectorbt as vbt
from datetime import datetime

# === 1. 配置 ===
DDB_HOST = "127.0.0.1"
DDB_PORT = 8848
DDB_USER = "admin"
DDB_PASS = "123456"  # [!!!] 请改成你设置的DolphinDB密码
DB_PATH = "dfs://okx_db"
TABLE_NAME = "kline_1h"
SYMBOL = "BTC-USDT"

# === 2. 回测参数 ===
FEES_PERCENT = 0.001  # 0.1% 的手续费
SLIPPAGE_PERCENT = 0.0005 # 0.05% 的滑点
INITIAL_CASH = 10000  # 初始模拟资金 10,000 USDT

def main():
    print(f"--- AlphaBot 1 策略回测程序 (MACD Crossover) ---")

    # === 3. 连接 DolphinDB 并拉取数据 ===
    s = ddb.session()
    try:
        s.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PASS)
        print(f"DolphinDB 连接成功。正在拉取所有 {SYMBOL} 数据...")
        
        # [!!! 最终绕过Bug !!!]
        # 我们不 select *
        # 我们在 SQL 中，明确地将 DateTime 转换为 LONG (整数)
        # 这样 API 就不会触发它那个有Bug的 "DATETIME -> datetime64[ns]" 转换
        sql_query = f"""
        select 
            long(DateTime) as DateTime_long, 
            Symbol, Open, High, Low, Close, Volume
        from loadTable('{DB_PATH}', `{TABLE_NAME}) 
        where Symbol='{SYMBOL}' 
        order by DateTime
        """
        
        # 1. 运行 SQL, DDB API 会返回一个 Pandas DataFrame
        #    'DateTime_long' 列现在是 int64 (纳秒)
        df = s.run(sql_query)
        
        if df.empty:
            print("[错误] 从DolphinDB拉取的数据为空！")
            return
            
        # 2. [!!! 关键修正: 时区BUG !!!]
        #    我们现在读取 'DateTime_long' 列
        #    a. 将 int64 (纳秒) 转换为 Pandas 的 Datetime 对象
        df['DateTime'] = pd.to_datetime(df['DateTime_long'], unit='s')
        #    b. 强制本地化为 UTC 时区
        df['DateTime'] = df['DateTime'].dt.tz_localize('UTC')
        
        # 3. 现在再设置索引
        df = df.set_index('DateTime')
            
        price_data = df['Close']
        print(f"成功拉取 {len(price_data)} 条1H K线数据。")

    except Exception as e:
        print(f"[致命错误] DolphinDB 操作失败: {e}")
        return
    finally:
        s.close()
        print("DolphinDB 连接已关闭。")


    # === 4. 手动计算 MACD 指标 ===
    print("正在手动计算 MACD 指标 (12, 26, 9)...")
    
    fast_ema = price_data.ewm(span=12, adjust=False).mean()
    slow_ema = price_data.ewm(span=26, adjust=False).mean()
    fast_line = fast_ema - slow_ema
    slow_line = fast_line.ewm(span=9, adjust=False).mean()
    
    print("MACD 指标计算完成。")


    # === 5. 生成交易信号 ===
    print("正在生成交易信号 (金叉/死叉)...")
    
    entries = fast_line.vbt.crossed_above(slow_line)
    exits = fast_line.vbt.crossed_below(slow_line)
    
    # === 6. 运行回测 ===
    print("正在运行向量化回测 (包含手续费和滑点)...")
    
    pf = vbt.Portfolio.from_signals(
        price_data,
        entries,
        exits,
        init_cash=INITIAL_CASH,
        fees=FEES_PERCENT,
        slippage=SLIPPAGE_PERCENT,
        freq='h' # 修复 'H' 警告
    )

    # === 7. 打印回测报告 ===
    print("\n--- [!!!] 回测报告 (最终绕过Bug版) [!!!] ---")
    print(pf.stats())
    print("\n--- 回测报告结束 ---")

if __name__ == "__main__":
    main()
