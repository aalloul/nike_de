from src.readers.input_data_reader import InputDataReader
from src.config import Config
from pyspark.sql import types as T, dataframe, SparkSession
from typing import Type


class StoreDataReader(InputDataReader):
    """
    This class encapsulates the read function to load the Store dataset
    """
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
