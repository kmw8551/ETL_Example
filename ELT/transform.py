import pandas as pd
import re
from datetime import datetime, timedelta
from commons.logSet.logSettings import Loggers


class ExchangeNotSetError(Exception):
    def __init__(self):
        super().__init__('Exchange not set, Require Argument')


class Transform(ExchangeNotSetError):



    def __init__(self, log_object):
        
        self._exchange = None
       
        self._last_endpoint = None
        self._mylog =  log_object

    
    
    def time_pattern(self, x):
        self._mylog.info("change time pattern")
        if self._exchange == "binance":
            time_return = datetime.utcfromtimestamp(int(x) / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
        elif self._exchange == "bitmex" or self._exchange =='bybit':
            if re.match('\d{4}\-\d{2}\-\d{2}T\d{2}\:\d{2}\:\d{2}Z', x):
                time_return = datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
            else:
                time_return = datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
            
        else:
            raise Exception

        return time_return 
    
    def toFrame(self, exchange : str, data):
        self._exchange = exchange
        self._mylog.info("change to pandas dataframe")
        if exchange == "binance":
            df = pd.DataFrame.from_dict(data, orient='columns')
            # df['time'] = df['time'].apply(lambda x: datetime.utcfromtimestamp(int(x) / 1000).strftime('%Y-%m-%d %H:%M:%S'))
            df['time'] = df['time'].apply(lambda x: Transform.time_pattern(self, x))
            df = df.rename(columns={'time': 'time_UTC'})
            df['time_KST'] = df['time_UTC'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + timedelta(hours=9))
            df = df[['id', 'price', 'qty', 'quoteQty', 'isBuyerMaker', 'time_UTC', 'time_KST']]
            

        elif exchange == "bitmex":
            df = pd.DataFrame.from_dict(data, orient='columns')
            
            # df['time'] = df['timestamp'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S"))
            df['time'] = df['timestamp'].apply(lambda x: Transform.time_pattern(self, x))
            df = df.rename(columns={'time': 'time_UTC'})
            df['time_KST'] = df['time_UTC'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + timedelta(hours=9))
            df.drop('timestamp', inplace=True, axis=1)
            
            
        elif exchange == "bybit":
            data = data['result']
            df = pd.DataFrame.from_dict(data, orient='columns')
            # df['time'] = df['time'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S"))
            df['time'] = df['time'].apply(lambda x: Transform.time_pattern(self, x))
            df = df.rename(columns={'time': 'time_UTC'})
            df['time_KST'] = df['time_UTC'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + timedelta(hours=9))
            
        else:
            self._mylog.error("exchange not set")
            raise ExchangeNotSetError

        return df
        
    
    def makerTaker(self, df) :
        '''
        
        '''
        
        if self._exchange == "binance":
          
            df['exch_id'] = '01'
            df = df[['exch_id', 'time_KST', 'isBuyerMaker', 'qty']]
                  
            
        elif self._exchange == "bitmex":
          
            df['exch_id'] = '02'
            df = df.rename(columns={'size': 'qty'})
            df = df[['exch_id', 'time_KST', 'side', 'qty']]
            
        elif self._exchange == "bybit":
        
            df['exch_id'] = '03'
            df = df[['exch_id', 'id', 'time_KST', 'side', 'qty']]
            
        else :

            raise Exception
        
        # return self._df
        return df

  
    def takerVol_cal(self, data):
        '''
        data 인자로 받는 이유 : 인자로 바꿔서 데이터를 받을 경우, 강제하는 효과가 있다.
        이전의 transformed data를 따로 쓸 수도 있기에 self 객체에 할당하기보다는,
        따로 받아서 Micro한 Service가 제공되게 한다.
        
        Taker Buy Volume
        =
        Volume of buy orders filled by takers in perpetual swaps(USD)
         
        Taker Sell Volume
        =
        Volume of sell orders filled by takers in perpetual swaps(USD)
        '''
        
        #data 내 KST 컬럼 : datetime64
        
        def aggr_1m(dataframe, types):
            dataframe['qty'] = dataframe['qty'].astype(float)
            dataframe = dataframe.set_index('time_KST', drop=True)
            agg_1m = dataframe['qty'].resample('1T').sum()
            
            col_name = "taker_%s_volume" % types
            df_aggr_1m = pd.DataFrame({'timestamp':agg_1m.index, 
                                       col_name : agg_1m.values})
            return df_aggr_1m
        
        tmp_buy = None
        tmp_sell = None

        if self._exchange == 'binance':
            tmp_buy = data[data.isBuyerMaker == False]
            tmp_sell = data[data.isBuyerMaker == True]
        
        elif self._exchange == 'bitmex':
            tmp_buy = data[data.side == 'Buy']
            tmp_sell = data[data.side == 'Sell']
        
        
        elif self._exchange == 'bybit':
            tmp_buy = data[data.side == 'Buy']
            tmp_sell = data[data.side == 'Sell']
        
        else:
            self._mylog.error("exchange not set")
            raise Exception
        
        df_aggr_buy = aggr_1m(tmp_buy, 'buy')
        df_aggr_sell = aggr_1m(tmp_sell, 'sell')
        
        # [timestamp, buy,  sell ]
        # [    1,      3,    NaN ]
        # [    1,      3,     5  ]
        # [    1,      NaN,    5 ]  
        
        # outer join을 한 경우 위와 같이 발생 , 
        # 특히 시간이 23:59:59  buy,  24:00:00 sell이 마지막 데이터일 경우
        # 따라서 겹치는 가운데 부분만 추출하여 적재
        # DB 적재에서는 동일 시간대 존재할 경우, created_at의 비교를 통해 더 최신의 데이터를 남겨 놓는다.
        
        df_aggr_total = pd.concat([df_aggr_buy, df_aggr_sell], 
                                      axis=1,
                                      join='inner')

        df_aggr_total = df_aggr_total.loc[:,~df_aggr_total.columns.duplicated()]


        df_aggr_total['id'] = self._exchange

        df_aggr_total['timestamp'] = df_aggr_total['timestamp'].apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))

        df_aggr_total = df_aggr_total[['id', 'timestamp', 'taker_buy_volume', 'taker_sell_volume']]

        return df_aggr_total
        


    

   
# if __name__ == "__main__":
#
#     import  time
#     collect_data = pd.DataFrame(columns=['exch_id','id', 'time_KST', 'side', 'qty'])
#     i = 0
#     binance_data = binance.GetBinanceTrade('coin', 'C:\\Users\\lucas\\Main\\cryptoquant\\logfiles\\', 'extract.log')
#     binance_data.endpoint = 'https://fapi.binance.com/fapi/v1/trades'
#     binance_data.symbol = 'BTCUSDT'
#     binance_data.limit = '1000'
#     tf = Transform()
#     while i < 3:
#
#         rs = binance_data.getTradeData(binance_data.getParameter())
#         binance_df = tf.toFrame('binance', rs)
#         tmp_val = tf.makerTaker(binance_df)
#
#         collect_data= pd.concat([collect_data, tmp_val]).drop_duplicates().reset_index(drop=True)
#         print(len(collect_data))
#         collect_data.sort_values(by=['time_KST'], ascending=False, inplace=True)
#
#         i += 1
#         time.sleep(5)
#     tv = tf.takerVol_cal(collect_data)
#

    
    
    # import  time
    # collect_data = pd.DataFrame(columns=['exch_id',
    #                                      'id', 
    #                                      'time_KST', 
    #                                      'side', 
    #                                      'qty'])
    # i = 0
    # bitmex_data = bitmex.GetBitmexTrade('coin', 'C:\\Users\\lucas\\Main\\cryptoquant\\logfiles\\', 'extract.log')
    # bitmex_data.endpoint = "https://www.bitmex.com/api/v1/trade?"
    # bitmex_data.symbol = 'XBTUSD'
    # bitmex_data.count = '1000'
    # tf = Transform()
    
    # while i < 8:
    #     rs = bitmex_data.getTradeData(bitmex_data.getParameter())
    #     bitmex_df = tf.toFrame('bitmex', rs)
    #     # print(tf.toFrame(rs))
    #     tmp_val = tf.makerTaker(bitmex_df)
    #     print(len(collect_data))
    #     collect_data= pd.concat([collect_data, tmp_val]).drop_duplicates().reset_index(drop=True)
    #     collect_data.sort_values(by=['time_KST'], ascending=False, inplace=True)
        
    #     i += 1 
    #     time.sleep(3)
    # tv = tf.takerVol_cal(collect_data)
    
    ######################################################################################

    # collect_data = pd.DataFrame(columns=['exch_id','id', 'time_KST', 'side', 'qty'])
    # i = 0
    # bybit_data = bybit.GetBybitTrade('coin', 'C:\\Users\\lucas\\Main\\cryptoquant\\logfiles\\', 'extract.log')
    # bybit_data.endpoint = "https://api.bybit.com/v2/public/trading-records?"
    # bybit_data.symbol = 'BTCUSD'
    # bybit_data.limit = '1000'
    # tf = Transform()
    # while i < 8:
            
    #     rs = bybit_data.getTradeData(bybit_data.getParameter())   
        
    #     bybit_df = tf.toFrame('bybit', rs)
    #     tmp_val = tf.makerTaker(bybit_df)
    
       
    #     print(len(collect_data))
    #     collect_data= pd.concat([collect_data, tmp_val]).drop_duplicates().reset_index(drop=True)
    #     collect_data.sort_values(by=['time_KST'], ascending=False, inplace=True)
        
    #     i += 1 
    #     time.sleep(3)
    # tv = tf.takerVol_cal(collect_data)
        
    





# @staticmethod
# def toDataFrame(data):
#     '''
#     make data(list form) to pandas DataFrame
#
#     :param data: list with dict type elements
#     :return: pandas DataFrame Obejct
#     '''
#     df = pd.DataFrame.from_dict(data, orient='columns')
#     df['time'] = df['time'].apply(lambda x: datetime.utcfromtimestamp(int(x) / 1000).strftime('%Y-%m-%d %H:%M:%S'))
#     df = df.rename(columns={'time': 'time_UTC'})
#     df['time_KST'] = df['time_UTC'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + timedelta(hours=9))
#     df = df[['id', 'price', 'qty', 'quoteQty', 'isBuyerMaker', 'time_UTC', 'time_KST']]
#     return df



# def transform_bitmex(df):
#
#     return None
#
# def transform_bybit(df):
#
#     return None
#
# def cal_min(exchange, df):
#     new_df = pd.DataFrame(columns=['timestamp', 'taker_buy_volume', 'taker_sell_volume', 'created_at'])
#     if exchange == 'binance':
#         tmp_df = transform_binance(df)
#
#     elif exchange == 'bitmex':
#         tmp_df = transform_bitmex(df)
#
#     elif exchange == 'bybit':
#         pass
#
#     else:
#         raise ExchangeNotSetError
#
#
# binance_df = binance.GetBinanceTrade()
# binance_df.endpoint = 'https://fapi.binance.com/fapi/v1/trades'
# # binance.endpoint = 'https://api.binance.com/api/v3/trades'
# binance_df.symbol = 'BTCUSDT'
# binance_df.limit = '1000'
# rs = binance_df.getTradeData()
# # print(rs)
# df = binance_df.toDataFrame(rs)
# # print(df)
# rslt = cal_min('binance',  df)
# print(rslt)