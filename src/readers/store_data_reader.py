from input_data_reader import InputDataReader
from config import Config
from pyspark.sql import types as T, dataframe, SparkSession
from typing import Type


class StoreDataReader(InputDataReader):
    schema = T.StructType() \
        .add("storeid", T.IntegerType()) \
        .add("channel", T.StringType()) \
        .add("country", T.StringType())

    header = True

    def __init__(self, spark_context: SparkSession, config: Type[Config]):
        super().__init__(spark_context, config)

    def read(self) -> dataframe:
        return self._read(self.config.input_store) \
            .withColumnRenamed("storeid", "storeId")
