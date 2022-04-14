from datetime import datetime

from airflow import DAG
from airflow.decorators import task

with DAG(
    'passing_xcom_values',
    default_args={'retries': 2},
    description='Demonstrates 2 methods of passing xcoms',
    schedule_interval=None,
    start_date=datetime(2022, 4, 14),
    catchup=False,
    tags=['best'],
) as dag:

    # implicit xcom_push
    @task(task_id="A", retries=1)
    def get_time():
        x = datetime.now()
        return [x.hour, x.minute] # pushes to default 'return_value' key 
    time_task = get_time()

    # pull entire object from return_value key
    @task(task_id="B", retries=1)
    def print_time(**kwargs):
        ti = kwargs["ti"]
        time_obj = ti.xcom_pull(task_ids="A") # pulling object, no need to specify key
        print(f"It's currently {time_obj[0]}:{time_obj[1]}!")
    print_time_task = print_time()

    # explicit xcom_push
    @task(task_id="c", retries=1)
    def get_time_v2(**kwargs):
        ti = kwargs['ti']
        x = datetime.now()

        # explicitly push and set key names
        ti.xcom_push('month', x.month)
        ti.xcom_push('year', x.year)
    time_task_v2 = get_time_v2()

    # pull individual properties from keys
    @task(task_id="d", retries=1)
    def print_time_v2(**kwargs):
        ti = kwargs["ti"]

        # explicitly pull each key
        month = ti.xcom_pull(task_ids="c", key="month")
        year = ti.xcom_pull(task_ids="c", key="year")

        print(f"It's currently {month}:{year}!")
    print_time_task_v2 = print_time_v2()


    time_task >> print_time_task >> time_task_v2 >> print_time_task_v2
