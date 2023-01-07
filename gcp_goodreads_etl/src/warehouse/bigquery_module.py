import configparser
import logging
from pathlib import Path
from google.oauth2 import service_account
from google.cloud import storage,bigquery

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/warehouse_config.cfg"))

class GoodReadsBigqueryModule:
    
    def __init__(self):
        self._credentials = service_account.Credentials.from_service_account_file(f"{Path(__file__).parents[2]}/"+config.get('GCP','KEY_FILE'))
        self._bigquery = bigquery.Client(credentials=self._credentials)
        self._processed_zone = config.get('BUCKET','PROCESSED_ZONE')
        
    def create_dataset(self,dataset_name : str ="test-schema"):
        dataset_id= f"{}.{}".format(dataset_name)
        dataset = self._bigquery.Dataset(dataset_id)
        dataset.location = "US"
        logging.debug(f"Creating dataset : {dataset_id}")
        dataset = self._bigquery.create_dataset(dataset, timeout=30,exists_ok=True) 
        
    def create_table(self):
        pass 
    
    
        