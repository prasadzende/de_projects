# Data Engineering Projects

![Data Engineering Projects](https://i.morioh.com/210519/70badb5b.webp)

## [Project 1: GoodReads Data Pipeline Using GCP](https://github.com/prasadzende/de_projects/tree/main/gcp_goodreads_etl)

The data collected from the goodreads API is stored on local disk and is timely moved to the Landing Bucket on Cloud Storage. ETL jobs are written in spark and scheduled in airflow to run every 10 minutes.