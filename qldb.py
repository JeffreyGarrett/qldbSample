from logging import basicConfig, getLogger, INFO
from botocore.exceptions import ClientError
from pyqldb.driver.pooled_qldb_driver import PooledQldbDriver

logger = getLogger(__name__)
basicConfig(level=INFO)


class QLDB:
   
    pooled_qldb_driver = None
    qldb_session = None

    def __create_qldb_driver(self, ledger_name="testLedger", region_name=None, endpoint_url=None, boto3_session=None):
        #creates the authentication using IAM with application information 
        qldb_driver = PooledQldbDriver(ledger_name=ledger_name, region_name=region_name, endpoint_url=endpoint_url,
                                   boto3_session=boto3_session)
        self.pooled_qldb_driver = qldb_driver

    def __create_qldb_session(self):
        self.qldb_session = self.pooled_qldb_driver.get_session()
        

    def __init__(self):
        self.__create_qldb_driver()
        self.__create_qldb_session()

    def list_tables(self):
        try:
            logger.info('Listing table names ')
            for table in self.qldb_session.list_tables():
                logger.info(table)
        except ClientError:
            logger.exception('Unable to create session.')
        