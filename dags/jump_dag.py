from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from clean_by_spark import clean_by_spark
from upload_to_s3 import upload_to_s3
from s3_to_redshift import s3_to_redshfit

default_args = {
    "owner": "tanawat",
    "depends_on_past": False,
    "start_date": datetime(2022, 5, 25)
}

dag = DAG("jump", default_args=default_args, schedule_interval=timedelta(1))

t1 = PythonOperator(
    task_id='clean-by-spark',
    python_callable=clean_by_spark,
    dag=dag
)

t2 = PythonOperator(
    task_id='upload-to-s3',
    python_callable=upload_to_s3,
    dag=dag
)

t3 = PythonOperator(
    task_id='s3-to-redshift',
    python_callable=s3_to_redshfit,
    dag=dag
)

t1 >> t2 >> t3