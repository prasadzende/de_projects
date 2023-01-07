from pyspark.sql import SparkSession
from goodreads_transform import GoodreadsTransform
from storage_module import GoodReadsStorageModule
from pathlib import Path
import logging
import logging.config
import configparser
from warehouse.goodreads_warehouse_driver import GoodReadsWarehouseDriver
import time

# Setting configurations. Look config.cfg for more details
config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

# Setting up logger, Logger properties are defined in logging.ini file
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

def create_sparksession():
    """
    Initialize a spark session
    """
    return SparkSession.builder.master('yarn').appName('gooodreads').getOrCreate()

def main():
    """
    This method performs below tasks:
    1: Check for data in Landing Zone, if new files are present move them to Working Zone
    2: Transform data present in working zone and save the transformed data to Processed Zone
    3: Run Data Warehouse functionality by setting up Staging tables, then loading staging tables, performing upsert operations on warehouse.
    """
    logging.debug("\n\nSetting up Spark Session...")
    spark = create_sparksession()
    grt = GoodreadsTransform(spark)

    # Modules in the project
    modules = {
        "author.csv": grt.transform_author_dataset,
        "book.csv" : grt.transform_books_dataset,
        "reviews.csv" : grt.transform_reviews_dataset,
        "user.csv" : grt.tranform_users_dataset
    }

    logging.debug("\n\nCopying data from storage landing zone to ...")
    gds3 = GoodReadsStorageModule()
    gds3.storage_move_data(source_bucket= config.get('BUCKET','LANDING_ZONE'), target_bucket= config.get('BUCKET', 'WORKING_ZONE'))

    files_in_working_zone = gds3.get_files(config.get('BUCKET', 'WORKING_ZONE'))

    # Cleanup processed zone if files available in working zone
    if len([set(modules.keys()) & set(files_in_working_zone)]) > 0:
        logging.info("Cleaning up processed zone.")
        gds3.clean_bucket(config.get('BUCKET', 'PROCESSED_ZONE'))

    # for file in files_in_working_zone:
    #     if file in modules.keys():
    #         modules[file]()
    grt.transform_author_dataset()
    grt.transform_books_dataset()
    grt.transform_reviews_dataset()
    grt.tranform_users_dataset()

    logging.debug("Waiting before setting up Warehouse")
    time.sleep(5)

    # Starting warehouse functionality
    grwarehouse = GoodReadsWarehouseDriver()
    logging.debug("Setting up staging tables")
    grwarehouse.setup_staging_tables()
    logging.debug("Populating staging tables")
    grwarehouse.load_staging_tables()
    logging.debug("Setting up Warehouse tables")
    grwarehouse.setup_warehouse_tables()
    logging.debug("Performing UPSERT")
    grwarehouse.perform_upsert()

# Entry point for the pipeline
if __name__ == "__main__":
    main()