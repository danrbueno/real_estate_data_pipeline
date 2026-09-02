"""
DAG for Real Estate Data Pipeline using AI Scraper (OpenAI-powered)

This DAG replaces the original Scrapy-based scraping with AI agents from OpenAI.
Tasks:
1. Scrap data from DFImoveis using AI agents
2. Transform and clean the data
3. Load into MySQL database
"""

import os
import sys
from pathlib import Path
import pendulum

# Add project root to path
main_dir = str(Path(os.path.dirname(__file__)).parent.parent)
sys.path.insert(0, main_dir)

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Import pipeline modules
from pipelines import rentals, sales, datasets, database

# Import AI Scraper
from ai_scraper import AIScraper


# DAG scheduled to be executed every day at 12 AM
dag = DAG(
    dag_id="dag_real_estate_data_pipeline_ai",
    schedule="0 0 * * *",
    start_date=pendulum.now().subtract(days=2),
    catchup=False,
    tags=["real-estate", "ai-scraper"],
    description="Real estate data pipeline using AI agents for scraping"
)


# Scraping functions
def scrap_rentals():
    """Scrape rental data using AI agents"""
    scraper = AIScraper()
    try:
        properties = scraper.scrape_transaction_type("rentals")
        return {
            "status": "success",
            "transaction_type": "rentals",
            "count": len(properties)
        }
    finally:
        scraper.close()


def scrap_sales():
    """Scrape sales data using AI agents"""
    scraper = AIScraper()
    try:
        properties = scraper.scrape_transaction_type("sales")
        return {
            "status": "success",
            "transaction_type": "sales",
            "count": len(properties)
        }
    finally:
        scraper.close()


with dag:
    start = EmptyOperator(task_id="start_dag")

    with TaskGroup("scrap", tooltip="Scrap data using AI agents for rentals and sales") as task_group_scrap:
        # Scrap rental data from DFImoveis using OpenAI AI agents
        task_scrap_rentals = PythonOperator(
            task_id="scrap_rentals",
            python_callable=scrap_rentals,
            retries=2,
            retry_delay=300,  # 5 minutes
        )

        # Scrap sales data from DFImoveis using OpenAI AI agents
        task_scrap_sales = PythonOperator(
            task_id="scrap_sales",
            python_callable=scrap_sales,
            retries=2,
            retry_delay=300,  # 5 minutes
        )

        # Run them in parallel
        [task_scrap_rentals, task_scrap_sales]

    with TaskGroup("transform", tooltip="Adjustments and load dataset into a CSV file") as task_group_transform:
        # Adjustments and load rental dataset into a CSV file
        task_transform_rentals = PythonOperator(
            task_id="transform_rentals",
            python_callable=rentals.transform
        )

        # Adjustments and load sales dataset into a CSV file
        task_transform_sales = PythonOperator(
            task_id="transform_sales",
            python_callable=sales.transform
        )

        # Join the datasets into 1 CSV file
        task_join_datasets = PythonOperator(
            task_id="join_datasets",
            python_callable=datasets.join
        )

        task_transform_rentals >> task_transform_sales >> task_join_datasets

    with TaskGroup("load", tooltip="Reset MySQL database and load data into the tables") as task_group_load:
        # Reset MySQL database
        task_reset_db = PythonOperator(
            task_id="reset_database",
            python_callable=database.reset
        )

        # Load data into database
        task_load_db = PythonOperator(
            task_id="load_database",
            python_callable=database.load
        )

        task_reset_db >> task_load_db

    end = EmptyOperator(task_id="end_dag")

    # Define dependencies
    start >> task_group_scrap >> task_group_transform >> task_group_load >> end
