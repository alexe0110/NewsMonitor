from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from plugins.functions.processing_utils import find_unprocessed_files
from plugins.operators import HuggingFaceClassifierOperator, TextPreprocessingOperator

default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='classification_pipeline',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags={'classification', 'ml'},
    default_args=default_args,
) as dag:
    find_unprocessed = PythonOperator(
        task_id='find_unprocessed_files',
        python_callable=find_unprocessed_files,
    )

    preprocess = TextPreprocessingOperator(
        task_id='preprocess_news',
        source_task_id='find_unprocessed_files',
    )

    classify = HuggingFaceClassifierOperator(
        task_id='classify_news',
        source_task_id='preprocess_news',
    )

    find_unprocessed >> preprocess >> classify
