from input_data_reader import InputDataReader
from config import Config
from pyspark.sql import types as T, SparkSession, dataframe
from typing import Type


class SalesDataReader(InputDataReader):
    schema = T.StructType() \
        .add("saleId", T.IntegerType()) \
        .add("netSales", T.DoubleType()) \
        .add("salesUnits", T.IntegerType()) \
        .add("storeId", T.IntegerType()) \
        .add("dateId", T.IntegerType()) \
        .add("productId", T.StringType())

    header = True

    def __init__(self, spark_context: SparkSession, config: Type[Config]):
        super(SalesDataReader, self).__init__(spark_context, config)

    def read(self) -> dataframe:
        return self._read(self.config.input_sales)
