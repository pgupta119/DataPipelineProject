import logging
import logging.config
import sys
import os
from pipeline.helper import ingest, transform, persist
from pipeline.utils import configreader, sparkcreate
import argparse
from resources import configs
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--loggingconf', type=str, required=True)
parser.add_argument('--pipelineini', type=str, required=True)
parser.add_argument('--input', type=str, required=True)
parser.add_argument('--output', type=str, required=True)
args = parser.parse_args()


class Pipeline:
    def __init__(self):
        logging.config.fileConfig(args.loggingconf)
        # if os.getenv('env') == 'dev':
        self.conf_read = configreader.ConfigReader()
        self.params = self.conf_read.readconfig(args.pipelineini)

    def run_pipeline(self):
        try:
            sc = sparkcreate.SparkCreateSession()
            spark = sc.create_spark_session()
            logging.info("Spark Session created")

            logging.info("Run Pipeline method started")

            ingest_process = ingest.Ingest(spark)
            df_recipes = None
            if Path(args.input).suffix == '.json':
                df_recipes = ingest_process.read_json(args.input)
            elif Path(args.input).suffix == '.xlsx':
                df_recipes = ingest_process.read_excel(args.input)
            elif Path(args.input).suffix in ['csv', '.txt', '.dat', '.out']:
                df_recipes = ingest_process.read_csv(args.input)
            logging.info("Transforming")
            transform_process = transform.Transform(spark)
            transformed_df = transform_process.transform_data(df_recipes, self.params['recipe_filter'])

            logging.info("Transformation completed")

            logging.info("Persisting")
            persist_process = persist.Persist(spark)
            persist_process.persist_data(transformed_df, self.params['save_output_file_format'], args.output)
            logging.info("Persisting completed")

            logging.info("Run Pipeline method ended")

        except Exception as exp:
            logging.error("Error while running the pipeline > " + str(exp))
            raise exp
        return


if __name__ == '__main__':
    logging.info("Application started")
    pipeline = Pipeline()
    pipeline.run_pipeline()
    logging.info("Pipeline executed")
