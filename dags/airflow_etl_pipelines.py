from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import pendulum
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")


def task3():
    return print("No new data found")

commands_task1 = """
    cd ./etl_spark_job;
    spark-submit check_new_data.py;
    """
commands_task2 = """
    cd ./etl_spark_job;
    spark-submit export_CS_to_MySQL.py;
    """

# Create DAG
dag = DAG(
    'my_dag',
    description='DAG to trigger pySpark job',
    schedule_interval= '0 6 * * *',
    start_date= datetime(2023, 8, 2, 0, 0, tzinfo=local_tz)
)

task1 = BashOperator(
    task_id='task1',
    bash_command=commands_task1,
    dag=dag,
)

task2 = BashOperator(
    task_id='task2',
    bash_command=commands_task2,
    dag=dag,
)

task3 = PythonOperator(
    task_id='task3',
    python_callable=task3,
    dag=dag,
)

# Define dependencies between tasks in DAG
task1 >> [task2, task3]