from datetime import datetime
from airflow import DAG
import sys
sys.path.append('/home/himanshu/Himanshu/MongoQueue/')
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from db.insert_records import insertRecords


dag = DAG('insertion_DAG', description='DAG for inserting values to db',
          schedule_interval='*/1 * * * *',
          start_date=datetime(2019, 10, 24), catchup=False)

with dag:
    current_minute = datetime.now().minute
    ins_operator = PythonOperator(task_id='insert_records', python_callable=insertRecords, 
                                        op_args=[current_minute], dag=dag)
    ins_operator