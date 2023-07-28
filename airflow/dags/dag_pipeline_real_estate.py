import os
from pathlib import Path
import pendulum
import pipelines.rentals as rentals
import pipelines.sales as sales
import pipelines.datasets as ds
import pipelines.database as db

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

main_dir = str(Path(os.path.dirname(__file__)).parent.parent)
scrapy_dir = main_dir+"/scrapy"    

# Dag scheduled to be executed every day at 12 AM
dag = DAG(
    dag_id="dag_real_estate_data_pipeline",
    schedule="0 0 * * *",
    start_date=pendulum.now().subtract(days=2),
    catchup=False
)

with dag:
    start = EmptyOperator(task_id="start_dag")

    with TaskGroup("scrap", tooltip="Scrap data for rentals and sales") as task_group_scrap:
        # Scrap data from website https://www.dfimoveis.com.br/aluguel/df/todos/apartamento
        # The output file is defined in the scope of scraping project (/airflow/scraping/scraping/pipelines.py)
        task_scrap_rentals = BashOperator(
            task_id="scrap_rentals",
            bash_command="cd "+scrapy_dir+" && scrapy crawl DFImoveis -a transaction_type=rentals",
        )

        # Scrap data from website https://www.dfimoveis.com.br/venda/df/todos/apartamento
        # The output file is defined in the scope of scraping project (/airflow/scraping/scraping/pipelines.py)
        task_scrap_sales = BashOperator(
            task_id="scrap_sales",
            bash_command="cd "+scrapy_dir+" && scrapy crawl DFImoveis -a transaction_type=sales",
        )        

        task_scrap_rentals >> task_scrap_sales

    with TaskGroup("transform", tooltip="Adjustments and load dataset into a CSV file") as task_group_transform:
        # Adjustments and load dataset into a CSV file
        task_transform_rentals = PythonOperator(
            task_id="transform_rentals", python_callable=rentals.transform
        )

        # Adjustments and load dataset into a CSV file
        task_transform_sales = PythonOperator(
            task_id="transform_sales", python_callable=sales.transform
        )            

        # Join the datasets into 1 CSV file
        task_join_datasets = PythonOperator(task_id="join_datasets", python_callable=ds.join)

        task_transform_rentals >> task_transform_sales >> task_join_datasets

    with TaskGroup("load", tooltip="Reset MySQL database and load data into the tables") as task_group_load:
        # Reset MySQL database
        task_reset_db = PythonOperator(task_id="reset_database", python_callable=db.reset)
        task_load_db = PythonOperator(task_id="load_database", python_callable=db.load)
        
        task_reset_db >> task_load_db
    
    end = EmptyOperator(task_id="end_dag")

    start >> task_group_scrap >> task_group_transform >> task_group_load >> end