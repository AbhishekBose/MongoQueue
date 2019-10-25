#%%
import sys
sys.path.append('db/')
from mongoqueue import mongoQueue
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

#%%

def fetch_data_to_process():
    q = mongoQueue('fetch_list','statuses')
    vehicle_detector_status = q.get_vehicle_detector_status()
    for i in vehicle_detector_status:
        x = i
    vehicle_detector_status= x['Working']
    if vehicle_detector_status:
        data_to_process = q.Dequeue()
        return data_to_process
    else:
        return 'Vehicle Detector processing videos'
# %%

dag = DAG('read_from_db', description='Read not processed video list from DB',
          schedule_interval='*/1 * * * *',
          start_date=datetime(2019, 10, 25), catchup=False)
# %%
hello_operator = PythonOperator(task_id='read_from_db_to_process', python_callable=fetch_data_to_process, dag=dag)

# %%
