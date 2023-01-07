import configparser
from pathlib import Path

config = configparser.ConfigParser()

config.read_file(open(f"warehouse_config.cfg"))

# Setup configs
staging_schema = config.get('STAGING', 'SCHEMA')
gs_processed_zone = 'gs://' + config.get('BUCKET', 'PROCESSED_ZONE')

# Setup Staging Schema
create_staging_schema = "CREATE SCHEMA IF NOT EXISTS {};".format(staging_schema)

# Setup Drop table queries
drop_authors_table = "DROP TABLE IF EXISTS {}.authors;".format(staging_schema)
drop_reviews_table = "DROP TABLE IF EXISTS {}.reviews;".format(staging_schema)
drop_books_table = "DROP TABLE IF EXISTS {}.books;".format(staging_schema)
drop_users_table = "DROP TABLE IF EXISTS {}.users;".format(staging_schema)

create_authors_table = """
CREATE TABLE IF NOT EXISTS {}.authors
(
    author_id INT64 NOT NULL,
    name STRING,
    role STRING,
    profile_url STRING,
    average_rating FLOAT64,
    rating_count INT64,
    text_review_count INT64,
    record_create_timestamp DATETIME
);
""".format(staging_schema)

create_reviews_table = """
CREATE TABLE IF NOT EXISTS {}.reviews
(
    review_id INT64 NOT NULL,
    user_id INT64,
    book_id INT64,
    author_id INT64,
    review_text STRING,
    review_rating FLOAT64,
    review_votes INT64,
    spoiler_flag BOOL,
    spoiler_state STRING,
    review_added_date DATETIME,
    review_updated_date DATETIME,
    review_read_count INT64,
    comments_count INT64,
    review_url STRING,
    record_create_timestamp DATETIME
);
""".format(staging_schema)

create_books_table = """
CREATE TABLE IF NOT EXISTS {}.books
(
    book_id INT64 NOT NULL,
    title STRING,
    title_without_series STRING,
    image_url STRING,
    book_url STRING,
    num_pages INT64,
    format STRING,
    edition_information STRING,
    publisher STRING,
    publication_day INT64,
    publication_year INT64,
    publication_month INT64,
    average_rating FLOAT64,
    ratings_count INT64,
    description STRING,
    authors INT64,
    published INT64,
    record_create_timestamp DATETIME
);
""".format(staging_schema)

create_users_table = """
CREATE TABLE IF NOT EXISTS {}.users
(
    user_id INT64 NOT NULL,
    user_name STRING,
    user_display_name STRING,
    location STRING,
    profile_link STRING,
    uri STRING,
    user_image_url STRING,
    small_image_url STRING,
    has_image BOOL,
    record_create_timestamp DATETIME
);
""".format(staging_schema)

copy_authors_table=""" 
LOAD DATA OVERWRITE {}.authors
FROM FILES (
  format = 'CSV',
  uris = ['{}/authors/*.csv']);
""".format(staging_schema,gs_processed_zone)

copy_reviews_table="""
LOAD DATA OVERWRITE {}.reviews
FROM FILES (
  format = 'CSV',
  uris = ['{}/reviews/*.csv']);
""".format(staging_schema,gs_processed_zone)

copy_books_table="""
LOAD DATA OVERWRITE {}.books
FROM FILES (
  format = 'CSV',
  uris = ['{}/books/*.csv']);
""".format(staging_schema,gs_processed_zone)

copy_users_table="""
LOAD DATA OVERWRITE {}.users
FROM FILES (
  format = 'CSV',
  uris = ['{}/users/*.csv']);
""".format(staging_schema,gs_processed_zone)

drop_staging_tables = [drop_authors_table, drop_reviews_table, drop_books_table, drop_users_table]
create_staging_tables = [create_authors_table, create_reviews_table, create_books_table, create_users_table]
copy_staging_tables = [copy_authors_table, copy_reviews_table, copy_books_table, copy_users_table]
