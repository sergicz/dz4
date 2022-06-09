# [START import_module]
from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['sergicz@yandex.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

with DAG(
    'b24',
    default_args=default_args,
    description='b24',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    tags=['b24'],
) as dag:
    def imp_b24():
        #pg_hook = PostgresHook(postgres_conn_id='mypostgres')
        ch_hook = ClickHouseHook(clickhouse_conn_id='curs')
        t_insert = """INSERT INTO b24 (ID, PORTAL_USER_ID, PHONE_NUMBER,CALL_DURATION,CALL_START_DATE,CALL_FAILED_CODE,CALL_FAILED_REASON,CALL_TYPE) VALUES (%(a1)s, %(a2)s, %(a3)s, %(a4)s, %(a5)s, %(a6)s, %(a7)s, %(a8)s);"""
        api_hook = HttpHook(http_conn_id='myb24', method='GET')
        cNext=ch_hook.run("""select next from next;""")
        iNext = cNext[0][0]
#        iNext=-382200  #для тестирования - защита от лишних записей
        iLastID=iNext #последний номер записи
        while iNext>=0:
            print('Читаем следующую партию: '+str(iNext))
            resp = api_hook.run(data='start='+str(iNext))
            profile = json.loads(resp.content.decode('utf-8'))
            cRes=profile['result']
            if 'next' in profile:
               iNext=profile['next']
            else:
               iNext=-999
            for cEl in cRes:
               ch_hook.run(t_insert, {'a1':cEl['ID'], 'a2':cEl['PORTAL_USER_ID'], 'a3':cEl['PHONE_NUMBER'], 'a4':cEl['CALL_DURATION'], 'a5':cEl['CALL_START_DATE'], 'a6':cEl['CALL_FAILED_CODE'], 'a7':cEl['CALL_FAILED_REASON'], 'a8':cEl['CALL_TYPE']})
               iLastID+=1 #cEl['ID']
            print(cRes)
            print(iNext)
        ch_hook.run("""alter table next delete where 1=1""")
        ch_hook.run("""insert INTO next (next) VALUES (%(a)s);""",{'a':iLastID})
#        ch_hook.run("""alter table next update next=%(a)s where 1=1;""",{'a':iLastID})  #CH отказывается апдейтить записи
#        t_select="select * from t"
#        ch_hook.run(t_select)
        return ('ok')

    t1 = PythonOperator(
        task_id='imp_b24',
        python_callable=imp_b24,
    )
    t1.doc_md = dedent(
    """\
    ###TaskDoc
    """
    )
    t1