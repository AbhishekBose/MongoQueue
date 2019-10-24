from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from file_writing import filewriting


def checkCond(ds, *args):
    print(ds)
    if int(ds) %2 == 0:
        print("Im here")
        return 'writing_files'
    else:
        return 'sample_id'

def printDummy():
    ds = 5
    return ds

dag = DAG('inferenceReq_DAG', description='DAG for writing txt files',
          schedule_interval='*/1 * * * *',
          start_date=datetime(2019, 10, 23), catchup=False)

with dag:
    current_minute = datetime.now().minute
    branch_op = BranchPythonOperator(task_id='branch_task', python_callable=checkCond, 
                                        op_args=[current_minute],dag=dag)
    file_operator = PythonOperator(task_id='writing_files', python_callable=filewriting, 
                                        op_args=[current_minute], dag=dag)
    sample_operator = PythonOperator(task_id='sample_id', python_callable=printDummy, 
                                        dag=dag)
    
    branch_op >> [file_operator, sample_operator]
