import dolphindb as ddb
import pandas as pd
import requests
import time
from datetime import datetime

# === 1. Config ===
DDB_HOST = "127.0.0.1"
DDB_PORT = 8848
DDB_USER = "admin"
DDB_PASS = "123456"  # [!!!] CHANGE THIS TO YOUR PASSWORD
DB_PATH = "dfs://okx_db"
TABLE_NAME = "kline_1h"
SYMBOL = "BTC-USDT"
START_DATE_STR = "2020-01-01T00:00:00Z"
OKX_API_URL = "https://www.okx.com/api/v5/market/history-candles"
LIMIT = 100

# [!!! Using AFTER parameter !!!]
def get_okx_data_after(instrument_id, bar, after_timestamp_ms):
    params = {'instId': instrument_id, 'bar': bar, 'after': after_timestamp_ms, 'limit': LIMIT}
    try:
        response = requests.get(OKX_API_URL, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get('code') == '0' and data.get('data'):
            # OKX returns [oldest -> newest] when using 'after'
            return data['data']
        elif data.get('code') == '51001': print("  -> API 51001: No more data."); return []
        else: print(f"  -> API Error: {data.get('msg')}"); return None
    except requests.exceptions.RequestException as e: print(f"  -> HTTP Error: {e}"); return None

def main():
    print(f"--- AlphaBot 1 Import (AFTER, naive datetime) ---")
    s = ddb.session()
    try:
        s.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PASS); s.run("1+1")
        print(f"DDB Connected (Host: {DDB_HOST}:{DDB_PORT})")
    except Exception as e: print(f"[Fatal] DDB Connect Error: {e}"); return

    # === Find latest timestamp in DB ===
    try:
        result_df = s.run(f"select max(DateTime_long) from loadTable('{DB_PATH}', `{TABLE_NAME}) where Symbol='{SYMBOL}'")
        latest_long_val = None
        if isinstance(result_df, pd.DataFrame) and not result_df.empty: latest_long_val = result_df.iloc[0, 0]
        elif not isinstance(result_df, pd.DataFrame): latest_long_val = result_df

        if latest_long_val is None or pd.isna(latest_long_val):
            print(f"No data for `{SYMBOL}`. Starting from {START_DATE_STR}.")
            start_timestamp_ms = int(datetime.strptime(START_DATE_STR, "%Y-m-%dT%H:%M:%SZ").timestamp() * 1000)
            # OKX 'after' returns data *strictly newer than* the timestamp.
            # So, to include the start date, we need the timestamp just *before* it.
            current_after_ms = start_timestamp_ms - 1
        else:
            latest_timestamp_ns = latest_long_val
            current_after_ms = latest_timestamp_ns // 1_000_000 # Use the latest timestamp as 'after'
            dt_human = pd.to_datetime(current_after_ms, unit='ms')
            print(f"Latest data in DB: {dt_human} (UTC). Fetching data AFTER this.")

    except Exception as e: print(f"Error checking DDB timestamp: {e}"); s.close(); return

    # === Loop Fetch & Write ===
    total_imported = 0
    try:
        while True:
            dt_human = pd.to_datetime(current_after_ms, unit='ms')
            print(f"\nFetching {SYMBOL} AFTER {dt_human} (UTC)...")
            candles = get_okx_data_after(SYMBOL, '1H', current_after_ms)
            if not candles: print("Fetch End."); break
            print(f"  -> Fetched {len(candles)} candles.")

            df_data = []
            for candle in candles:
                timestamp_ms = int(candle[0])
                nano_timestamp = timestamp_ms * 1_000_000
                df_data.append({
                    'Symbol': SYMBOL,
                    # [!!! Key Fix: NAIVE Datetime !!!] Write naive datetime64[ns]
                    'DateTime': pd.to_datetime(timestamp_ms, unit='ms'),
                    'DateTime_long': nano_timestamp, # Write pure int64
                    'Open': float(candle[1]), 'High': float(candle[2]),
                    'Low': float(candle[3]), 'Close': float(candle[4]),
                    'Volume': float(candle[5])
                })

            df = pd.DataFrame(df_data)
            df['DateTime_long'] = df['DateTime_long'].astype('int64')
            # Ensure DateTime is naive before writing
            df['DateTime'] = pd.to_datetime(df['DateTime'])

            s.upload({'df_to_write': df})
            s.run(f"tableInsert(loadTable('{DB_PATH}', `{TABLE_NAME}), df_to_write)")

            total_imported += len(df)
            print(f"  -> Wrote {len(df)} rows to DDB.")
            # Update 'after' to the timestamp of the *newest* candle fetched
            current_after_ms = int(candles[-1][0])
            time.sleep(0.5)

    except Exception as e: print(f"[Error] Fetch/Write loop failed: {e}")
    finally:
        print(f"\n--- Task Complete ---"); print(f"Imported {total_imported} new rows.")
        s.close(); print("DDB Connection Closed.")

if __name__ == "__main__": main()