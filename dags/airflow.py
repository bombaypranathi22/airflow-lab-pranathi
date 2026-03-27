from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow import configuration as conf
from src.lab import load_data, data_preprocessing, build_save_model, load_model_elbow

conf.set('core', 'enable_xcom_pickling', 'True')

default_args = {
    'owner': 'Pranathi',
    'start_date': datetime(2026, 3, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pranathi_airflow_kmeans_lab',
    default_args=default_args,
    description='Airflow KMeans clustering lab by Pranathi',
    schedule_interval=None,
    catchup=False,
)

load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    dag=dag,
)

data_preprocessing_task = PythonOperator(
    task_id='data_preprocessing_task',
    python_callable=data_preprocessing,
    op_args=[load_data_task.output],
    dag=dag,
)

build_save_model_task = PythonOperator(
    task_id='build_save_model_task',
    python_callable=build_save_model,
    op_args=[data_preprocessing_task.output, "pranathi_model.sav"],
    dag=dag,
)

load_model_task = PythonOperator(
    task_id='load_model_task',
    python_callable=load_model_elbow,
    op_args=["pranathi_model.sav", build_save_model_task.output],
    dag=dag,
)

load_data_task >> data_preprocessing_task >> build_save_model_task >> load_model_task