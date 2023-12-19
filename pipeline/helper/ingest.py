import logging.config
from pipeline.utils import sparkcreate




class Ingest:
    def __init__(self, spark):
        self.spark = spark

    def read_json(self, input_location):
        logger = logging.getLogger("Ingest")
        try:
            logger.info('Ingesting json files from the source directory')
            df_json = self.spark.read.json(input_location)
            logger.info('Reading json file successful')
            # df_json.show()
            return df_json
        except Exception as exp:
            logger.error('An error occurred while reading json file > ' + str(exp))
            raise OSError('Source directory does not exist')

    def read_excel(self, input_location, header=True):
        logger = logging.getLogger("Ingest")
        try:
            logger.info('Ingesting excel files from the source directory')
            df_excel = self.spark.read.format('excel').option('inferSchema', True).option('header', header).load(
                input_location)
            logger.info('Reading excel file successful')
            return df_excel
        except Exception as exp:
            logger.error('An error occurred while reading excel file > ' + str(exp))
            raise OSError('Source directory does not exist')

    def read_csv(self, input_location, header=True, delimiter=','):
        logger = logging.getLogger("Ingest-csv")
        try:
            logger.info('Ingesting excel files from the source directory')
            df_csv = self.spark.read.format('csv').option('inferSchema', True).option('header', header).option(
                'delimiter', delimiter).load(input_location)
            logger.info('Reading excel file successful')
            return df_csv
        except Exception as exp:
            logger.error('An error occurred while reading excel file > ' + str(exp))
            raise OSError('Source directory does not exist')

    def read_db(self, input_location):
        pass


