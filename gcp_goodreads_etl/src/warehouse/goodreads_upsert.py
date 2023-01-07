import configparser
from pathlib import Path

config = configparser.ConfigParser()
config.read_file(open(f"warehouse_config.cfg"))
#config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

staging_schema = config.get('STAGING', 'SCHEMA')
warehouse_schema = config.get('WAREHOUSE', 'SCHEMA')


# ==============================AUTHORS==========================================

upsert_authors = """
BEGIN TRANSACTION;
MERGE INTO {1}.authors USING {0}.authors  AS authors_0
ON authors.author_id = authors_0.author_id
   WHEN MATCHED THEN DELETE 
;
INSERT INTO {1}.authors
  SELECT
    author_id ,
    name,
    role,
    profile_url,
    average_rating,
    rating_count ,
    text_review_count,
    cast(record_create_timestamp as DATETIME)
    FROM
      {0}.authors
;
COMMIT TRANSACTION;
""".format(staging_schema, warehouse_schema)

# =============================REVIEWS==============================================

upsert_reviews = """
BEGIN TRANSACTION;
MERGE INTO {1}.reviews USING {0}.reviews AS reviews_0 
ON reviews.review_id = reviews_0.review_id
   WHEN MATCHED THEN DELETE 
;
INSERT INTO {1}.reviews
  SELECT
    review_id ,
    user_id,
    book_id ,
    author_id,
    review_text,
    review_rating,
    review_votes ,
    spoiler_flag ,
    spoiler_state,
    cast(review_added_date as DATETIME),
    cast(review_updated_date as DATETIME),
    review_read_count INT64,
    comments_count INT64,
    review_url STRING,
    cast(record_create_timestamp as DATETIME)
    FROM
      {0}.reviews
;
COMMIT TRANSACTION;
""".format(staging_schema, warehouse_schema)

# ===============================BOOKS=============================================

upsert_books = """
BEGIN TRANSACTION;
MERGE INTO {1}.books USING {0}.books AS books_0 
ON books.book_id = books_0.book_id
   WHEN MATCHED THEN DELETE 
;
INSERT INTO {1}.books
  SELECT
      book_id,
    title,
    title_without_series,
    image_url,
    book_url ,
    num_pages ,
    `format`,
    edition_information,
    publisher,
    publication_day,
    publication_year ,
    publication_month,
    average_rating ,
    ratings_count,
    description,
    authors,
    published,
    cast(record_create_timestamp as DATETIME)
    FROM
      {0}.books
;
COMMIT TRANSACTION;
""".format(staging_schema, warehouse_schema)

# ===============================USERS=============================================

upsert_users = """
BEGIN TRANSACTION;
MERGE INTO {1}.users USING {0}.users AS users_0 
ON users.user_id = users_0.user_id
   WHEN MATCHED THEN DELETE 
;
INSERT INTO {1}.users
  SELECT
    user_id,
    user_name,
    user_display_name,
    location,
    profile_link,
    uri,
    user_image_url,
    small_image_url,
    has_image,
    cast(record_create_timestamp AS DATETIME)
    FROM
      {0}.users
;
COMMIT TRANSACTION;
""".format(staging_schema, warehouse_schema)

# ======================================================================================

upsert_queries = [upsert_authors, upsert_reviews, upsert_books, upsert_users]