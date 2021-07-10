from input_data_reader import InputDataReader
from config import Config
from pyspark.sql import SparkSession, types as T
from typing import Type


class ProductDataReader(InputDataReader):
    schema = T.StructType() \
        .add("productid", T.StringType()) \
        .add("division", T.StringType()) \
        .add("gender", T.StringType()) \
        .add("category", T.StringType())
    header = True

    def __init__(self, spark_context: SparkSession, config: Type[Config]):
        super().__init__(spark_context, config)

    def read(self):
        return self._read(self.config.input_product).withColumnRenamed("productid", "productId")
