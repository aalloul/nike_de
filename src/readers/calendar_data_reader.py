from input_data_reader import InputDataReader
from config import Config
from datetime import date
from pyspark.sql import functions as F, types as T, SparkSession, dataframe
from typing import Type


class CalendarDataReader(InputDataReader):
    """
    This class encapsulates the calendar dataset. It defines a few transformations on the input dataset to allow the
    extraction of the week number in the year.
    The main method is `read`. It returns a `pyspark.sql.dataframe` with the columns `dateId, weekNumber`.
    """

    # we could use smaller data types here but according to
    # https://stackoverflow.com/questions/14531235/in-java-is-it-more-efficient-to-use-byte-or-short-instead-of-int-and-float-inst
    # this wouldn't necessarily be a good idea.
    schema = T.StructType() \
        .add("datekey", T.IntegerType()) \
        .add("datecalendarday", T.IntegerType()) \
        .add("datecalendaryear", T.IntegerType()) \
        .add("weeknumberofseason", T.IntegerType())

    header = True

    def __init__(self, spark_context: SparkSession, config: Type[Config]):
        super().__init__(spark_context, config)
        # This is used later to determine the date for each row
        self.year_start_date = date(Config.reporting_year, 1, 1)

    def read(self) -> dataframe:
        df = self._read(self.config.input_calendar).withColumnRenamed("datekey", "dateId")
        first_date_id = self.min_date_id(df)

        # the column `datekey` (in the original dataset) is an integer that seems to increase by `+1` for every day.
        # As the dataset only has the year and calendar day (which resets at the end of each calendar month), we assume
        # that the date is simply the year + (current_dateId - first_date_id)
        # Based on this assumption, we can then extract the weekofyear
        df = df \
            .filter(F.col("datecalendaryear") == Config.reporting_year) \
            .withColumn("first_date", F.lit(first_date_id)) \
            .withColumn("daysSinceStart", F.col("dateId") - F.col("first_date")) \
            .withColumn("date", F.lit(self.year_start_date) + F.col("daysSinceStart")) \
            .withColumn("weekNumber", F.weekofyear(F.col("date")))

        # Filter to accommodate for the reporting of a single week
        if Config.reporting_week is None:
            return df
        else:
            return df.filter(F.col("weekNumber") == Config.reporting_week)

    @staticmethod
    def min_date_id(df: dataframe) -> int:
        """
        This method extracts the first `dateId` (or `dateKey` in the original dataset).
        :param df: Input calendar dataframe
        :return: The minimum `dateId` in the data
        """
        return df.agg(F.min("dateId").alias("minDateId")).collect()[0].minDateId
