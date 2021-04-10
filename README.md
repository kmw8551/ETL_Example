# Cryptoquant Task

작성자: Min Woong Kang

---

과제 링크 : [Realtime Data ETL & Serving via REST API](https://www.notion.so/Realtime-Data-ETL-Serving-via-REST-API-fd8850b7fec64e73b56839d627bfe099) 

### 1. 개요

- 암호화폐 거래소에서 제공하는 데이터를 추출하고 분 단위 가공하여 데이터베이스에 저장한다.
- 데이터베이스에 저장된 데이터를 Restful API를 통해 제공한다

### 2. 구성

- cryptoquant ( 최상위 레벨 / 패키지 이름)
    - Binance ( Binance API GET 관련 모듈)
    - Bitmex ( Bitmex API GET 관련 모듈)
    - Bybit ( Bybit API GET 관련 모듈)
    - commons ( 거래소 모듈 공통 템플릿)
    - doc (개발 시 발생하는 문서 관련 폴더, **클래스 다이어그램**, **데이터베이스 스키마**, **플로우차트** )
    - ELT ( Transformation, Load 관련)
    - logfiles (logging구현 및 로그 파일)
    - init.py
    - [app.py](http://app.py) (Flask / Swagger UI 기반 taker_volume API 제공 코드, 서버 실행 관련)
    - README.MD
    - [test.py](http://test.py) (패키지 실행 예시 코드 관련, main 함수 실행)
- doc
    - diagram_cryptoquant.png (클래스 다이어그램)
    - database_schema.png (데이터베이스 스키마)
    - flowchart_ETL.png (플로우차트)  

### 3. 개발 고려 사항 및 특징

1. 개발 요건 충족

	Python 3.8.9  
	Flask 1.1.2
	![Untitled](https://user-images.githubusercontent.com/34782356/114054214-9d037f80-98ca-11eb-9d60-950020419f9b.png)
    

	Postgresql
	![Untitled 1](https://user-images.githubusercontent.com/34782356/114054306-b73d5d80-98ca-11eb-8035-89997f09a01f.png)


    


2. 특징

    1. 객체 지향 코드 구현
    - 공통 코드 템플릿(commons) 를 통한 코드 재사용성
    - 상속 및 오버라이딩 (ex : getParameter 함수) 
    - property, setter를 이용한 객체 무결성 고려 (외부의 이상한 변수 설정 금지) 
    2. 기능의 모듈화를 이용, 확장 가능성 고려
    3. logging 모듈을 이용한 로그 구현 
    4. transform.py, load.py
    - 집계 테이블 내, taker_buy_volume 컬럼과 taker_sell_volume 컬럼의 무결성 유지
    - Null 값이 존재하는 row 제거
    - drop_duplicates를 통한 중복 제거
    - Database Insert 후, DB Table 내 중복 데이터 제거 
    (가장 최근 시간에 생성된 row를 남겨 놓는 방식)
    5.  Swagger UI 가 지원되는 flask_restx 패키지를 통한 웹 UI 개선
    6. 'all_exchange' 사용 시 데이터 간 구별이 쉽지 않아 거래소의 명칭을 추가
    7. database_schema는 요구사항 3개 테이블  + dim_exch ( 거래소 코드 및 이름 관련 디멘션 테이블)로 구성 

3. 기타 고려사항(to)
   - 거래소 데이터 가공 후 최종  api 거래소별 데이터 제공 확인여부를 파악하기 위해 부득이하게 exchange_name을 FK로 설정하여 3개 테이블을 구성하였습니다
   - 원래 구상은 각 테이블 별 (exchange_id, timestamp, taker_buy_volume, taker_sell_volume, created_at)으로 구상하고 디멘션 테이블과 JOIN 연산을 통해 거래소 명을 파악하는 형태였습니다.
   - 요구 사항은 맞췄습니다.

### 4. 작동 방법

**[데이터 수집]**

```python
# test.py

def main(exchange, n):
    logger_obj = logSettings.Loggers()
		# 최초 로그 파일 객체 명 및 로그파일 경로, 로그파일 명 설정
    record_logs = logger_obj.log('coin', 'C:\\Users\\lucas\\Main\\cryptoquant\\logfiles\\', 'extract.log')
    
		# 빈 데이터프레임에 차곡 차곡 저장
		collect_data = pd.DataFrame(columns=['exch_id', 'id', 'time_KST', 'side', 'qty'])
    i = 0
    tmp = None
		# 거래소 별 enpoint, symbol, 주요 파라미터 입력
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

		# postgresql 접속 사항 입력 
    load_settings.conn =psycopg2.connect(database="postgres",
                                         user="postgres",
                                         password="rkdalsdnd1!",
                                         host="127.0.0.1",
                                         port="5432")
    load_settings.execute_mogrify(tv, f'{exchange}_taker_volume')

if __name__ == "__main__":
    # main('bitmex', 5)
		main('거래소 명' , 시도 횟수)

'''
Kafka, Kinesis와 같은 Realtime 구현 상황을 위해 서버에 매 3초 마다 
데이터를 요청하는 상황을 구현
(i번째 에서 종료, 무한 루프 방지)
'''

```

**[서버]**

```python
app = Flask(__name__)  
api = Api(app)  

# get에 2가지 url 방식 등록, limit의 경우, 생략 시 기본 10 적용

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

				# 변수 검증 부분
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
						#변수 검증과 관련하여 다 통과 시 flag = True가 되어 동작
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
						
						#쿼리 결과 값 json 제공
            def json_default(value):
                if isinstance(value, datetime.datetime):
                    return value.strftime('%Y-%m-%d %H:%M:%S.%f')
                raise TypeError('not JSON serializable')

            #print('result : {}'.format(bool(result)))
            data = json.dumps(result, default=json_default)
            return data
   

if __name__ == "__main__":
    app.run()
```
