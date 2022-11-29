import pandas as pd

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
 
from datetime import datetime

date = datetime.today().strftime("%Y_%m_%d")

  
def _process_review():
    processed_review = pd.read_csv(f'C:/Users/hufs0/OneDrive/바탕 화면/apache-airflow/mnt/csv파일/{date}.csv')
    processed_review.to_csv('/tmp/processed_user.csv', index=None, header=False)
 
def _store_review():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY uncorrect_review FROM stdin WITH DELIMITER as ','",
        filename='/tmp/uncorrect_review.csv'
    )
 
with DAG('schedule', start_date=datetime(2022, 11, 1), 
        schedule_interval='@daily', catchup=False) as dag:
 
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS uncorrect_review (
                taste TEXT NOT NULL,
                amount TEXT NOT NULL,
                delivery TEXT NOT NULL,
                review TEXT NOT NULL
            );
        '''
    )
    
    is_review_currencies_file_available = FileSensor(
        task_id="is_review_currencies_file_available",
        fs_conn_id="forex_path",
        filepath=date+".csv",
        poke_interval=5,
        timeout=20
    )
    
    saving_review = BashOperator(
        task_id="saving_review",
        bash_command=f"""
            hdfs dfs -mkdir -p /schedule && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/{date}.csv /schedule
        """
    )

    creating_reviewtable = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                taste STRING,
                amount DATE,
                delivery DOUBLE,
                review DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    review_processing = SparkSubmitOperator(
        task_id="forex_processing",
        application="/opt/scripts/forex_processing.py",
        conn_id="spark_conn",
        verbose=False
    )
 
    process_review = PythonOperator(
        task_id='process_review',
        python_callable=_process_review
    )
 
    store_review = PythonOperator(
        task_id='store_review',
        python_callable=_store_review
    )
 
    create_table  >> is_review_currencies_file_available >> process_review >> store_review >> saving_review 
    saving_review >> creating_reviewtable >> review_processing
