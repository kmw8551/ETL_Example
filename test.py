import pandas as pd
import time
from Binance import binance
from Bitmex  import bitmex
from Bybit   import bybit
from functools import wraps
from datetime import datetime, timedelta
from ELT import transform
from ELT import load
import psycopg2
from commons.logSet import logSettings



def main(exchange, n):
    logger_obj = logSettings.Loggers()
    record_logs = logger_obj.log('coin', 'C:\\Users\\lucas\\Main\\cryptoquant\\logfiles\\', 'extract.log')
    collect_data = pd.DataFrame(columns=['exch_id', 'id', 'time_KST', 'side', 'qty'])
    i = 0
    tmp = None
    if exchange == "binance":
        binance_data = binance.GetBinanceTrade(record_logs)
        binance_data.endpoint = 'https://fapi.binance.com/fapi/v1/trades'
        binance_data.symbol = 'BTCUSDT'
        binance_data.limit = '1000'
        tmp = binance_data

    elif exchange == "bitmex":
        bitmex_data = bitmex.GetBitmexTrade(record_logs)
        bitmex_data.endpoint = "https://www.bitmex.com/api/v1/trade?"
        bitmex_data.symbol = 'XBTUSD'
        bitmex_data.count = '1000'
        tmp = bitmex_data

    elif exchange == "bybit":
        bybit_data = bybit.GetBybitTrade(record_logs)
        bybit_data.endpoint = "https://api.bybit.com/v2/public/trading-records?"
        bybit_data.symbol = 'BTCUSD'
        bybit_data.limit = '1000'
        tmp = bybit_data
    tf = transform.Transform(record_logs)
    while i < n:
        rs = tmp.getTradeData(tmp.getParameter())
        exch_df = tf.toFrame(exchange, rs)
        tmp_val = tf.makerTaker(exch_df)

        collect_data = pd.concat([collect_data, tmp_val]).drop_duplicates().reset_index(drop=True)
        print(len(collect_data))
        collect_data.sort_values(by=['time_KST'], ascending=False, inplace=True)

        i += 1
        time.sleep(2)
    tv = tf.takerVol_cal(collect_data)
    load_settings = load.SQLloader(record_logs)
    load_settings.conn =psycopg2.connect(database="postgres",
                                         user="postgres",
                                         password="rkdalsdnd1!",
                                         host="127.0.0.1",
                                         port="5432")
    load_settings.execute_mogrify(tv, f'{exchange}_taker_volume')

    return "Done"


if __name__ == "__main__":
    main('bitmex', 5)