import logging
import logging.config


class Persist:
    def __init__(self, spark):
        self.spark = spark

    def persist_data(self, df, saveformat, savelocation):
        """

        :param df:
        :param saveformat:
        :param savelocation:
        :return:
        """
        logger = logging.getLogger("persist")
        try:
            logger = logging.getLogger("Persist")
            logger.info('Persisting started')
            df.coalesce(1).write.mode('overwrite').format(saveformat).option('header', 'true').save(savelocation)
            logger.info("Persisting completed")
        except Exception as exp:
            logger.error("An error occured while persisiting data >" + str(exp))
            raise Exception("Directory already exists")
