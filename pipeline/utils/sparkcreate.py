from pyspark.sql import SparkSession


class SparkCreateSession:
    def create_spark_session(self):
        spark = SparkSession.builder \
            .master("local") \
            .appName("Data Engineering Spark App").getOrCreate()
        return spark
