import shutil
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

with DAG(
    'python_variations',
    default_args={'retries': 2},
    description='Demonstrates multiple ways to run Python scripts',
    schedule_interval=None,
    start_date=datetime(2022, 4, 14),
    catchup=False,
    tags=['best'],
) as dag:

    # task definition method 1
    # (PythonOperator)
    def hello_world():
        print("Hello World!")

    hello_task = PythonOperator (
        task_id="hello",
        python_callable=hello_world,
        retries=1
    )

    # task definition method 2
    # (_PythonDecoratedOperator)
    # this became available in Airflow 2.0
    # requires from airflow.decorators import task
    @task(task_id="goodbye", retries=1)
    def goodbye_world():
        print("Goodbye World!")
    goodbye_task = goodbye_world()

    # task definition method 3
    # (_PythonDecoratedOperator)
    # this became available in Airflow 2.0
    # does NOT require from airflow.decorators import task
    @dag.task(task_id="hello_again", retries=1)
    def hello_again():
        print("Hello a Second Time!")
    hello_again_task = hello_again()

    # task definition method 4
    # (_PythonVirtualenvDecoratedOperator)
    # alternatively use PythonVirtualenvOperator directly
    # this became available in Airflow 2.9
    # requires from airflow.decorators import task
    if not shutil.which("virtualenv"):
        print("The virtalenv_python task requires virtualenv, please install it.")
    else:
        @task.virtualenv(
            task_id="thumbs_up",
            retries=1,
            requirements=["colorama==0.4.0", "emoji"]
            )
        def thumbs_up():
            from emoji import emojize
            print(emojize(":thumbs_up:"))
        thumbs_up_task = thumbs_up()


    hello_task >> [goodbye_task, hello_again_task] >> thumbs_up_task
