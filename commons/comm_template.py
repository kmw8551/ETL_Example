import requests
import validators
import json
from abc import *
from commons.logSet.logSettings import Loggers




class RangeError(Exception):
    def __init__(self):
        super().__init__('Range out of Error')


class GetData(RangeError, Loggers, metaclass= ABCMeta):
    def __init__(self, log_object):

        self._data: str = None
        self._endpoint: str = None
        self._symbol: str = None
        self._limit : str = None
        self._mylog = log_object
        #'C:\\Users\\lucas\\Main\\cryptoquant\\logfiles\\'




    @property
    def endpoint(self):
        return self._endpoint

    @endpoint.setter
    def endpoint(self, ep):
        '''
        :param ep: String
        :return: None
        '''
        try:
            if validators.url(ep):
                self._endpoint = ep
                self._mylog.info("validate endpoint : %s " % ep)
            else:
                raise Exception
        except Exception as e:
            self._mylog.error("Error Occurred, %s" % e)


    @property
    def symbol(self):
        return self._symbol

    @symbol.setter
    def symbol(self, symbol_name):
        '''
        :param symbol_name: String
        :return: None
        '''
        try:
            if isinstance(symbol_name, str):
                self._mylog.info("validate symbol : %s " % symbol_name)
                self._symbol = symbol_name
            else:
                raise Exception
        except Exception as e:
            self._mylog.error("Error Occurred, %s" % e)

    @property
    def limit(self):
        return self._limit

    @limit.setter
    def limit(self, limit_num):

        try:
            flag: bool = False
            if isinstance(limit_num, str):
                if limit_num.isdigit():
                    flag = True

            elif isinstance(limit_num, int):
                flag = True

            if flag :
                self._mylog.info("limit_num : %s " % limit_num)
                if  500 <= int(limit_num) < 1001:
                        self._limit = limit_num
                        self._mylog.info("validate")
                else:
                    raise RangeError
            else:
                raise Exception

        except RangeError as e:
            self._mylog.error("Error Occurred, %s" % e)


        # errors except RangError(custom Error)
        except Exception as e:
            self._mylog.error("Error Occurred, %s" % e)

    @abstractmethod
    def getParameter(self):
        parameter = {}
        return parameter


    def getTradeData(self, pfunc):
        self._mylog.info("get trade data from exchange")
        endpoint, params = self._endpoint, pfunc
        result = requests.get(endpoint, params=params)
        self._mylog.info("result body : %s" % result.text)
        str_result= result.text
        list_result = json.loads(str_result)
        return list_result


