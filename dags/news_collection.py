from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from plugins.functions.news_fetchers import fetch_devto, fetch_hackernews

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='news_collection',
    default_args=default_args,
    description='Collect tech news from Hacker News and Dev.to',
    schedule='@hourly',  # Каждый час
    start_date=datetime(2025, 1, 1, tzinfo=UTC),
    catchup=False,
    tags={'news', 'collection'},
) as dag:
    fetch_hn = PythonOperator(
        task_id='fetch_hackernews',
        python_callable=fetch_hackernews,
    )

    fetch_dt = PythonOperator(
        task_id='fetch_devto',
        python_callable=fetch_devto,
    )
