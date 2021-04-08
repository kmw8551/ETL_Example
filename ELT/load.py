
import psycopg2
import psycopg2.extras as extras
from datetime import datetime
import pytz


class SQLloader:
    def __init__(self, logs):
        self._conn = None
        self.record_logs = logs

    @property
    def conn(self):
        return self._conn

    @conn.setter
    def conn(self, connections):
        self._conn = connections

    # 여기서부터 작업하기
    def execute_mogrify(self, df, table):
        """
        Using cursor.mogrify() to build the bulk insert query
        then cursor.execute() to execute the query
        """
        # Create a list of tupples from the dataframe values
        dt_now = datetime.now(pytz.timezone('Asia/Seoul')).isoformat()
        array = [list(x) + [dt_now] for x in df.to_numpy()]
        # Comma-separated dataframe columns
        cols = 'exchange_name,' \
               'timestamp,' \
               'taker_buy_volume,' \
               'taker_sell_volume,' \
               'created_at'

        # SQL quert to execute

        cursor = self._conn.cursor()

        values = [cursor.mogrify("(%s,%s,%s,%s,%s)", tuple(map(str,arr))).decode('utf8') for arr in array]
        query = "INSERT INTO %s(%s) VALUES " % (table, cols) + ",".join(values)

        try:
            self.record_logs.info("cursor object executed query")
            cursor.execute(query, array)
            self._conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:

            self.record_logs.error("Error: %s" % error)
            self._conn.rollback()
            cursor.close()
            return 1
        # if duplicates remain latest one (timestamp가 가장 마지막(최신)인 것만 남게 처리
        # 따라서 timestamp가 unique해진다.

        dupl_remove_query =     f'''with dup as(
                                    select timestamp, count(*)
                                    from {table}
                                    group by timestamp
                                    having count(*) > 1
                                    ),
                                    mid as (
                                    select *
                                    from {table} as v,
                                         dup as d
                                    where v.timestamp = d.timestamp
                                    ),
                                    max_val as(
                                    select timestamp, max(created_at) as max_created
                                     from {table}
                                     where timestamp in (
                                                    select timestamp
                                                    from {table}
                                                    group by timestamp
                                                    having count(*) > 1
                                                )
                                         group by timestamp
                                    ),
                                    rm_target as (
                                    select distinct itst.timestamp, itst.created_at
                                    from (select btv.timestamp, btv.created_at
                                          from {table} btv, dup d
                                          where btv.timestamp = d.timestamp
                                         ) itst, max_val as mv
                                    where itst.created_at != mv.max_created
                                    )
                                    delete from {table} origin
                                    where exists (select 1 from rm_target rm where origin.created_at = rm.created_at) ;
                                     '''
        # cursor.execute(dupl_remove_query)

        self.record_logs.info("execute_mogrify() done")

        cursor.close()
        self.record_logs.info("Job Success!")
        print("All Jobs are Successfully Done!!!!")


