from commons.comm_template import  GetData, RangeError

class GetBinanceTrade(GetData, RangeError):

    def __init__(self, logs):
        super().__init__(logs)
        self._from = None

    def getParameter(self):
        parameter = {'symbol' : self._symbol, 'limit' : self._limit, 'from' : self._from}
        return parameter


# if __name__ == "__main__":
#
#     binance_data = GetBinanceTrade('coin', 'C:\\Users\\lucas\\Main\\cryptoquant\\logfiles\\', 'extract.log')
#     binance_data.endpoint = "https://api.binance.com/api/v3/trades"
#     binance_data.symbol = 'BTCUSDT'
#     binance_data.limit = '1000'
#
#     rs = binance_data.getTradeData(binance_data.getParameter())
#     print(rs)

########################################################################################################################
# original Design

# Pilot class

# class GetBinanceTrade(RangeError):
#
#     def __init__(self):
#         self._data : str = None
#         self._endpoint : str = None
#         self._symbol : str = None
#         self._limits : str = None
#
#     @property
#     def endpoint(self):
#         return self._endpoint
#
#     @endpoint.setter
#     def endpoint(self, ep):
#         '''
#         :param ep: String
#         :return: None
#         '''
#         self._endpoint = ep
#
#     @property
#     def symbol(self):
#         return self._symbol
#
#     @endpoint.setter
#     def symbol(self, symbol_name):
#         '''
#         :param symbol_name: String
#         :return: None
#         '''
#         self._symbol = symbol_name
#
#     @property
#     def limits(self):
#         return self._limits
#
#     @endpoint.setter
#     def limits(self, limit_num):
#         try:
#             if  500 <= int(limit_num) <= 1000:
#                 self._params = limit_num
#             else:
#                 raise RangeError
#
#         except RangeError as e:
#             print(e)
#
#         # errors except RangError(custom Error)
#         except Exception as e:
#             print(e)
#
#
#     def getTradeData(self):
#         '''
#         :return: list
#         '''
#         params = {'symbol' : self._symbol, 'limit' : self._limits}
#         result = requests.get(self._endpoint, params)
#         str_result = result.text
#         list_result =  json.loads(str_result)
#         return list_result
#
#     @staticmethod
#     def toDataFrame(data):
#         '''
#         make data(list form) to pandas DataFrame
#
#         :param data: list with dict type elements
#         :return: pandas DataFrame Obejct
#         '''
#         df = pd.DataFrame.from_dict(data, orient='columns')
#         df['time'] = df['time'].apply(lambda x : datetime.utcfromtimestamp(int(x)/1000).strftime('%Y-%m-%d %H:%M:%S'))
#         df = df.rename(columns = {'time' : 'time_UTC'})
#         df['time_KST'] = df['time_UTC'].apply(lambda x : datetime.strptime(x, '%Y-%m-%d %H:%M:%S') +timedelta(hours=9))
#         df = df[['id', 'price', 'qty', 'quoteQty', 'isBuyerMaker',  'time_UTC' , 'time_KST']]
#         return df
#
    


# binance endpoint for recent trades
# binance_ep = 'https://api.binance.com/api/v3/trades'
# params = {'symbol' : 'BTCUSDT', 'limit' : '1000'}
# r2 = requests.get(binance_ep, params=params)
# print(r2.text)

# binance = GetBinanceTrade()
# binance.endpoint = 'https://fapi.binance.com/fapi/v1/trades'
# # binance.endpoint = 'https://api.binance.com/api/v3/trades'
# binance.symbol = 'BTCUSDT'
# binance.limit = '1000'
# rs = binance.getTradeData()
# # print(rs)
# df = binance.toDataFrame(rs)
# print(df)