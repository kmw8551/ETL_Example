import psycopg2 as pg2
import json
import datetime
from flask import Flask
from flask_restx import Api, Resource


app = Flask(__name__)  
api = Api(app)  



@api.route('/taker_volume/<string:exchange>/from=<int:_from>/to=<int:_to>', defaults={'limit': 10})
@api.route('/taker_volume/<string:exchange>/from=<int:_from>/to=<int:_to>/<int:limit>')
class bigdataDB(Resource):
    def get(self , exchange, _from, _to, limit):
        '''

        :param exchange: [binance, bitmex, bybit, all_exchange]
        :param _from: int, unixtimestamp
        :param _to: int, unixtimestamp
        :param limit: int or str
        :return:
        '''
        if limit  < 10 or limit > 100 :
            print("wrong limit, range between 10 to 100")
            print("Set limit to 10")
            limit = '10'

        flag =False
        if isinstance(exchange, str) :
            if exchange in ['binance', 'bitmex', 'bybit', 'all_exchange']:
                flag = True
            else:
                print('Wrong Value, exchage lists are [binance, bitmex, bybit, all_exchange]')
                return "Nothing to Return"
        else:
            print('Wrong Format, type must be string')
            raise Exception

        if isinstance(_from, int) :
            flag = True

        else:
            print('Wrong Format, type must be int')
            raise Exception

        if isinstance(_to, int):
            flag =True
        else:
            print('Wrong Format, type must be int')
            raise Exception

        if flag:
            conn = pg2.connect(database="postgres",
                             user="postgres",
                             password="rkdalsdnd1!",
                             host="127.0.0.1",
                             port="5432")
            #
            cursor = conn.cursor()

            table_name = ""
            all_table = ""

            if exchange == 'binance':
                table_name = "binance_taker_volume"
            elif exchange == 'bitmex':
                table_name = "bitmex_taker_volume"
            elif exchange == 'bybit':
                table_name = "bybit_taker_volume"
            elif exchange == 'all_exchange':
                all_table = "all"

            _from = datetime.datetime.fromtimestamp(int(_from)).strftime('%Y-%m-%d %H:%M:%S')
            _to = datetime.datetime.fromtimestamp(int(_to)).strftime('%Y-%m-%d %H:%M:%S')
            indiv_sql = f"""
                    select *
                    from {table_name}
                    where timestamp between '{_from}' and '{_to}' 
                    order by timestamp desc
                    """
            indiv_sql = indiv_sql + ('limit %s' % limit) if limit is not None else indiv_sql

            total_sql = f"""
                        with tb as(
                        select *
                        from binance_taker_volume
                        union
                        select *
                        from bitmex_taker_volume
                        )
                        select *
                        from tb
                        union
                        select *
                        from bybit_taker_volume
                        where timestamp between '{_from}' and '{_to}'
                        order by timestamp desc
                        limit {limit};
                        """

            if exchange == 'all_exchange' :
                cursor.execute(total_sql)
            else :
                cursor.execute(indiv_sql)

            result = cursor.fetchall()

            def json_default(value):
                if isinstance(value, datetime.datetime):
                    return value.strftime('%Y-%m-%d %H:%M:%S.%f')
                raise TypeError('not JSON serializable')

            print(result)
            data = json.dumps(result, default=json_default)
            return data
   


if __name__ == "__main__":
    app.run()