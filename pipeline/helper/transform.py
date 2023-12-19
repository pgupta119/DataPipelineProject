from pyspark.sql.types import FloatType
from pyspark.sql.functions import when, lit, col, ceil
from pyspark.sql import functions as F
from pipeline.helper.convert_duration import get_mins
from pyspark.sql import SparkSession
import logging
import logging.config



class Transform:
    def __init__(self, spark):
        self.spark = spark

    def apply_rule1(self, df):
        pass

    def apply_rule2(self, df):
        pass

    def transform_data(self, df, filter_param):
        logger = logging.getLogger("Transform")
        try:
            logger.info("Transforming")
            # df.filter(col('dsada'))
            """
            filter the data based on the filter_param
            """
            beef_df = df.filter(F.lower(df.ingredients).contains(filter_param))
            """
            replace the empty string with PT in cookTime and prepTime columns
            """
            beef_df = beef_df.withColumn("cookTime", when(F.col("cookTime") == "", "PT").otherwise(col("cookTime")))
            beef_df = beef_df.withColumn("prepTime", when(F.col("prepTime") == "", "PT").otherwise(col("prepTime")))

            convertduration = F.udf(lambda duration: get_mins(duration), FloatType())

            """
            convert the cookTime and prepTime to minutes
            """

            beef_df_mins = beef_df.withColumn("cookTime_mins", convertduration(col("cookTime"))) \
                .withColumn("prepTime_mins", convertduration(col("prepTime")))

            beef_total_cook_time = beef_df_mins.withColumn("total_cook_time",
                                                           col("prepTime_mins") + col("cookTime_mins"))

            beef_difficulty = beef_total_cook_time.withColumn("difficulty", \
                                                              when((beef_total_cook_time.total_cook_time < 30),
                                                                   lit("easy")) \
                                                              .when((beef_total_cook_time.total_cook_time >= 30) \
                                                                    & (beef_total_cook_time.total_cook_time <= 60), \
                                                                    lit("medium")) \
                                                              .otherwise(lit("hard")))
            beef_difficulty_avg_level = beef_difficulty.groupBy("difficulty").avg("total_cook_time").withColumnRenamed(
                'avg(total_cook_time)',
                'avg_total_cooking_time')

            beef_difficulty_avg_level = beef_difficulty_avg_level.select('difficulty',
                                                                         ceil('avg_total_cooking_time').alias(
                                                                             'avg_total_cooking_time'))

            logger.info("Transformation completed")
            beef_difficulty_avg_level.show()
            return beef_difficulty_avg_level
        except Exception as exp:
            logger.error("Error occured while transforming data > " + str(exp))



