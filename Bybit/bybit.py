from commons.comm_template import  GetData, RangeError


class GetBybitTrade(GetData, RangeError):

    def __init__(self,logs):
        super().__init__(logs)
        self._from = None

    def getParameter(self):
        parameter = {'symbol' : self._symbol, 'limit' : self._limit}
        return parameter

# if __name__ == "__main__":
#     bybit_data = GetBybitTrade('coin', 'C:\\Users\\lucas\\Main\\cryptoquant\\logfiles\\', 'extract.log')
#     bybit_data.endpoint = "https://api.bybit.com/v2/public/trading-records?"
#     bybit_data.symbol = 'BTCUSD'
#     bybit_data.limits = '1000'
#
#     rs = bybit_data.getTradeData(bybit_data.getParameter())
#     print(rs)