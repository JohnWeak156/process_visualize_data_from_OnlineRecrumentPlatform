from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

def task3():
    return print("No new data found")

commands_task1 = """
    cd .\The_system_processes_and_analyzes_log_data_from_the_Online_Recruitment_Platform\ETL_spark_job;
    spark-submit --packages com.datastax.spark:spark-cassandra-connector-assembly_2.12-3.3.0 check_new_data.py;
    """
commands_task2 = """
    cd .\The_system_processes_and_analyzes_log_data_from_the_Online_Recruitment_Platform\ETL_spark_job;
    spark-submit --packages com.datastax.spark:spark-cassandra-connector-assembly_2.12-3.3.0 export_CS_to_MySQL.py;
    """

# Create DAG
dag = DAG(
    'my_dag',
    description='DAG to trigger pySpark job',
    schedule_interval= '0 6 * * *',
    start_date= datetime(2023, 7, 30)
)

task1 = BashOperator(
    task_id='task1',
    bash_command=commands_task1,
    xcom_push=True,
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