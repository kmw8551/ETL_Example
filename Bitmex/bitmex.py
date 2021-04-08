
from commons.comm_template import  GetData, RangeError



class GetBitmexTrade(GetData, RangeError):

    def __init__(self, logs):
        super().__init__(logs)
        self._filter = None
        self._columns = None
        self._count = None
        self._start = None
        self._reverse = False #default
        self._startTime = None
        self._endTime = None


    @property
    def count(self):
        return self._count

    @count.setter
    def count(self, limit_num):

        try:
            self._mylog.debug("start 'count' url value validation")
            flag: bool = False
            if isinstance(limit_num, str):
                limit_num = limit_num.strip()
                if limit_num.isdigit():
                    flag = True

            elif isinstance(limit_num, int):
                flag = True

            if flag:
                self._mylog.info("limit_num : %s " % limit_num)
                if 500 <= int(limit_num) <= 1000:
                    self._params = limit_num
                    self._mylog.info("validate")
                else:
                    self._mylog.error("wrong range was set")
                    raise RangeError
            else:
                self._mylog.error("wrong type, input str or int object")
                raise Exception

        except RangeError as e:
            self._mylog.error("Error Occurred, %s" % e)


    def getParameter(self):
        total = {'symbol': self._symbol,
                  'filter': self._filter,
                  'columns': self._columns,
                  'count' : self._count,
                  'start' : self._start,
                  'reverse' : self._reverse,
                  'startTime' : self._startTime,
                  'endTime' : self._endTime}

        parameter = {k : v for k, v in total.items() if v is not None}
        return parameter


# if __name__ == "__main__":
#     bitmex_data = GetBitmexTrade('coin', 'C:\\Users\\lucas\\Main\\cryptoquant\\logfiles\\', 'extract.log')
#     bitmex_data.endpoint = "https://www.bitmex.com/api/v1/trade?"
#     bitmex_data.symbol = 'XBTUSD'
#     rs = bitmex_data.getTradeData(bitmex_data.getParameter())
#     print(rs)