
import os
import sys

sys.path.append("goodreads")

from datetime import datetime, timedelta
import os
#from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.bigquery import ( 
                            BigQueryValueCheckOperator,
                            BigQueryCreateEmptyDatasetOperator,
                            BigQueryCreateEmptyTableOperator,
                            BigQueryInsertJobOperator,
                            BigQueryCheckOperator)

# from airflow.operators.goodreads_plugin import DataQualityOperator
# from airflow.operators.goodreads_plugin import LoadAnalyticsOperator
from goodreads.plugins.helpers.analytics_queries import *

#config = configparser.ConfigParser()
#config.read_file(open(f"{Path(__file__).parents[0]}/emr_config.cfg"))


default_args = {
    'owner': 'goodreads',
    'depends_on_past': False,
    'start_date' : datetime(2022, 12, 31, 0, 0, 0, 0),
    'end_date' : datetime(2023, 12, 31, 0, 0, 0, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=15),
    'catchup': True
}


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/mnt/c/data-growth-poc-289687863392.json"
dag_name = 'goodreads_pipeline'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and Transform data from landing zone to processed zone. Populate data from Processed zone to goodreads Warehouse.',
          schedule_interval=None,
          #schedule_interval='*/10 * * * *',
          max_active_runs = 1,
          start_date = datetime(2022, 12, 31, 0, 0, 0, 0)
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

CLUSTER_NAME = 'cluster-31d0'
REGION='us-east4'
BQ_LOCATION="US"
DATASET_NAME="goodreads_analytics"
PROJECT_ID='data-growth-poc'
PYSPARK_URI='gs://training_raw/goodreads/goodreads_driver.py'
PYSPARK_ZIP_URI='gs://training_raw/goodreads/goodreads_etl.zip'

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI, 
        "python_file_uris": [
            PYSPARK_ZIP_URI,
            "gs://training_raw/goodreads/config.cfg",
            "gs://training_raw/goodreads/warehouse/warehouse_config.cfg",
            "gs://training_raw/goodreads/data-growth-poc-289687863392.json",
            "gs://training_raw/goodreads/logging.ini"
            ]},
}

pySparkjobOperator = DataprocSubmitJobOperator(
        task_id="GoodReadsETLJob", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

authors_data_quality = BigQueryCheckOperator(
    task_id="authors_check_value",
    sql=f"SELECT count(*) FROM test_prod_training.authors",
    use_legacy_sql=False,
    location=BQ_LOCATION,
)

reviews_data_quality = BigQueryCheckOperator(
    task_id="reviews_check_value",
    sql=f"SELECT count(*) FROM test_prod_training.reviews",
    use_legacy_sql=False,
    location=BQ_LOCATION,
)

books_data_quality = BigQueryCheckOperator(
    task_id="boks_check_value",
    sql=f"SELECT count(*) FROM test_prod_training.books",
    use_legacy_sql=False,
    location=BQ_LOCATION,
)

users_data_quality = BigQueryCheckOperator(
    task_id="users_check_value",
    sql=f"SELECT count(*) FROM test_prod_training.users",
    use_legacy_sql=False,
    location=BQ_LOCATION,
)

create_analytics_schema = BigQueryCreateEmptyDatasetOperator(
    task_id="Create_analytics_schema", 
    dataset_id=DATASET_NAME)

create_author_analytics_table_1 = BigQueryCreateEmptyTableOperator(
    task_id="Create_author_analytics_table_1",
    dataset_id=DATASET_NAME,
    table_id="popular_authors_review_count",
    schema_fields=[
        {"name": "author_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "review_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "role", "type": "STRING", "mode": "NULLABLE"},
        {"name": "profile_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "average_rating", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "rating_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "text_review_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "record_create_timestamp", "type": "DATETIME", "mode": "NULLABLE"},
    ],
)

create_author_analytics_table_2 = BigQueryCreateEmptyTableOperator(
    task_id="Create_author_analytics_table_2",
    dataset_id=DATASET_NAME,
    table_id="popular_authors_average_rating",
    schema_fields=[
        {"name": "author_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "average_review_rating", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "role", "type": "STRING", "mode": "NULLABLE"},
        {"name": "profile_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "average_rating", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "rating_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "text_review_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "record_create_timestamp", "type": "DATETIME", "mode": "NULLABLE"},
    ],
)

create_author_analytics_table_3 = BigQueryCreateEmptyTableOperator(
    task_id="Create_author_analytics_table_3",
    dataset_id=DATASET_NAME,
    table_id="best_authors",
    schema_fields=[
        {"name": "author_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "review_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "average_review_rating", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "role", "type": "STRING", "mode": "NULLABLE"},
        {"name": "profile_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "average_rating", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "rating_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "text_review_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "record_create_timestamp", "type": "DATETIME", "mode": "NULLABLE"},
    ],
)

create_book_analytics_table_1 = BigQueryCreateEmptyTableOperator(
    task_id="create_book_analytics_table_1",
    dataset_id=DATASET_NAME,
    table_id="popular_books_review_count",
    schema_fields=[
        {"name": "book_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "review_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        {"name": "title_without_series", "type": "STRING", "mode": "NULLABLE"},
        {"name": "image_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "book_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "num_pages", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "format", "type": "STRING", "mode": "NULLABLE"},
        {"name": "edition_information", "type": "STRING", "mode": "NULLABLE"},
        {"name": "publisher", "type": "STRING", "mode": "NULLABLE"},
        {"name": "average_rating", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "ratings_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "authors", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "record_create_timestamp", "type": "DATETIME", "mode": "NULLABLE"},
    ],
)

create_book_analytics_table_2 = BigQueryCreateEmptyTableOperator(
    task_id="create_book_analytics_table_2",
    dataset_id=DATASET_NAME,
    table_id="popular_books_average_rating",
    schema_fields=[
        {"name": "book_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "average_reviews_rating", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        {"name": "title_without_series", "type": "STRING", "mode": "NULLABLE"},
        {"name": "image_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "book_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "num_pages", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "format", "type": "STRING", "mode": "NULLABLE"},
        {"name": "edition_information", "type": "STRING", "mode": "NULLABLE"},
        {"name": "publisher", "type": "STRING", "mode": "NULLABLE"},
        {"name": "average_rating", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "ratings_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "authors", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "record_create_timestamp", "type": "DATETIME", "mode": "NULLABLE"},
    ],
)

create_book_analytics_table_3 = BigQueryCreateEmptyTableOperator(
    task_id="create_book_analytics_table_3",
    dataset_id=DATASET_NAME,
    table_id="best_books",
    schema_fields=[
        {"name": "book_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "average_reviews_rating", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "review_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        {"name": "title_without_series", "type": "STRING", "mode": "NULLABLE"},
        {"name": "image_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "book_url", "type": "STRING", "mode": "NULLABLE"},
        {"name": "num_pages", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "format", "type": "STRING", "mode": "NULLABLE"},
        {"name": "edition_information", "type": "STRING", "mode": "NULLABLE"},
        {"name": "publisher", "type": "STRING", "mode": "NULLABLE"},
        {"name": "average_rating", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "ratings_count", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
        {"name": "authors", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "record_create_timestamp", "type": "DATETIME", "mode": "NULLABLE"},
    ],
)

load_author_table_reviews = BigQueryInsertJobOperator(
    task_id="Load_author_table_reviews",
    configuration={
        "query": {
            "query": AnalyticsQueries.populate_authors_reviews.format('2020-02-01 00:00:00.000000', '2020-02-28 00:00:00.000000'),
            "useLegacySql": False,
        }
    },
    location=BQ_LOCATION,
)

load_author_table_ratings = BigQueryInsertJobOperator(
    task_id="Load_author_table_ratings",
    configuration={
        "query": {
            "query": AnalyticsQueries.populate_authors_ratings.format('2020-02-01 00:00:00.000000', '2020-02-28 00:00:00.000000'),
            "useLegacySql": False,
        }
    },
    location=BQ_LOCATION,
)

load_best_author  = BigQueryInsertJobOperator(
    task_id="Load_best_author",
    configuration={
        "query": {
            "query": AnalyticsQueries.populate_best_authors,
            "useLegacySql": False,
        }
    },
    location=BQ_LOCATION,
)

load_book_table_reviews = BigQueryInsertJobOperator(
    task_id="Load_book_table_reviews",
    configuration={
        "query": {
            "query": AnalyticsQueries.populate_books_reviews.format('2020-02-01 00:00:00.000000', '2020-02-28 00:00:00.000000'),
            "useLegacySql": False,
        }
    },
    location=BQ_LOCATION,
)

load_book_table_ratings  = BigQueryInsertJobOperator(
    task_id="Load_book_table_ratings",
    configuration={
        "query": {
            "query": AnalyticsQueries.populate_books_ratings.format('2020-02-01 00:00:00.000000', '2020-02-28 00:00:00.000000'),
            "useLegacySql": False,
        }
    },
    location=BQ_LOCATION,
)

load_best_books  = BigQueryInsertJobOperator(
    task_id="Load_best_books",
    configuration={
        "query": {
            "query": AnalyticsQueries.populate_best_books,
            "useLegacySql": False,
        }
    },
    location=BQ_LOCATION,
)

authors_data_quality_checks_1 = BigQueryCheckOperator(
    task_id="Authors_data_quality_checks_1",
    sql=f"SELECT count(*) FROM goodreads_analytics.popular_authors_average_rating",
    use_legacy_sql=False,
    location=BQ_LOCATION,
)

authors_data_quality_checks_2 = BigQueryCheckOperator(
    task_id="Authors_data_quality_checks_2",
    sql=f"SELECT count(*) FROM goodreads_analytics.popular_authors_average_rating",
    use_legacy_sql=False,
    location=BQ_LOCATION,
)

books_data_quality_checks_1 = BigQueryCheckOperator(
    task_id="Books_data_quality_checks_1",
    sql=f"SELECT count(*) FROM goodreads_analytics.popular_books_average_rating",
    use_legacy_sql=False,
    location=BQ_LOCATION,
)

books_data_quality_checks_2 = BigQueryCheckOperator(
    task_id="Books_data_quality_checks_2",
    sql=f"SELECT count(*) FROM goodreads_analytics.popular_books_review_count",
    use_legacy_sql=False,
    location=BQ_LOCATION,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# start_operator >> jobOperator >> warehouse_data_quality_checks >> create_analytics_schema
# create_analytics_schema >> [create_author_analytics_table, create_book_analytics_table]
# create_author_analytics_table >> [load_author_table_reviews, load_author_table_ratings, load_best_author] >> authors_data_quality_checks
# create_book_analytics_table >> [load_book_table_reviews, load_book_table_ratings, load_best_book] >> books_data_quality_checks
# [authors_data_quality_checks, books_data_quality_checks] >> end_operator

start_operator >> pySparkjobOperator >> [authors_data_quality, reviews_data_quality, books_data_quality, users_data_quality] >> create_analytics_schema
create_analytics_schema >> [create_author_analytics_table_1,create_author_analytics_table_2,create_author_analytics_table_3,create_book_analytics_table_1,create_book_analytics_table_2,create_book_analytics_table_3] 
create_author_analytics_table_1 >> load_author_table_reviews >> authors_data_quality_checks_1
create_author_analytics_table_2 >> load_author_table_ratings >> authors_data_quality_checks_2
create_author_analytics_table_3 >> load_best_author 
create_book_analytics_table_1 >> load_book_table_reviews >> books_data_quality_checks_1
create_book_analytics_table_2 >> load_book_table_ratings >> books_data_quality_checks_2
create_book_analytics_table_3 >> load_best_books
[authors_data_quality_checks_1,authors_data_quality_checks_2,books_data_quality_checks_1,books_data_quality_checks_2]>>end_operator