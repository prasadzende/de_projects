import configparser
import logging
from pathlib import Path
from google.oauth2 import service_account
from google.cloud import storage

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read_file(open(f"config.cfg"))
#config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))

class GoodReadsStorageModule:

    def __init__(self):
        self._credentials = service_account.Credentials.from_service_account_file(config.get('GCP','KEY_FILE'))
        self._storage = storage.Client(credentials=self._credentials)
        self._files = []
        self._landing_zone = config.get('BUCKET','LANDING_ZONE')
        self._working_zone = config.get('BUCKET','WORKING_ZONE')
        self._processed_zone = config.get('BUCKET','PROCESSED_ZONE')
        
    def storage_move_data(self, source_bucket = None, target_bucket= None):
        """
        Detect files in source bucket and move those files to target bucket
        :param source_bucket: name of source bucket
        :param target_bucket: name of target bucket
        """
        
        # If no argument passed default to the project related landing zone and working zone
        if source_bucket is None:
            source_bucket = self._landing_zone
        if target_bucket is None:
            target_bucket = self._working_zone

        logging.debug(f"Inside storage_move_data : Source bucket set is : {source_bucket}\n Target bucket set is : {target_bucket}")

        # cleanup target bucket
        self.clean_bucket(target_bucket)

        # Move files to working zone
        for key in self.get_files(source_bucket):
            if key.name in config.get('FILES','NAME').split(","):
                logging.debug(f"Copying file {key} from {source_bucket} to {target_bucket}")
                self._storage.bucket(source_bucket).copy_blob(key, self._storage.bucket(target_bucket), key.name)

        # cleanup source bucket,
        # Cleaning bucket part is commented to avoid uploading files to storage again and again when testing heavy loads on ETL.

        #self.clean_bucket(source_bucket)
        
    def get_files(self, bucket_name):
        """
        Get all the files present in the provided bucket
        :param bucket_name: bucket to search
        :return: keys or files present in the bucket
        """
        logging.debug(f"Inspecting bucket : {bucket_name} for files present")
        return [bucket_object for bucket_object in self._storage.bucket(bucket_name).list_blobs()]

    def clean_bucket(self, bucket_name):
        """
        Clean the bucket, delete all files
        :param bucket_name: bucket name, bucket to clean
        """
        logging.debug(f"Cleaning bucket : {bucket_name}")
        self._storage.bucket(bucket_name).delete_blobs(blobs=list(self._storage.bucket(bucket_name).list_blobs()))