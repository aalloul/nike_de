from pyspark.sql import SparkSession
from src.config import Config
from typing import Type

class InputDataReader(object):
    schema = None
    header = True
    file_format = "csv"

    def __init__(self, spark_context: SparkSession, config: Type[Config]):
        self.spark_context = spark_context
        self.config = config

    def _read(self, location: str):
        return self.spark_context.read.format("csv") \
            .option("header", self.header) \
            .schema(self.schema) \
            .load(location)
