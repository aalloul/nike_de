from calendar_data_reader import CalendarDataReader
from product_data_reader import ProductDataReader
from sales_data_reader import SalesDataReader
from store_data_reader import StoreDataReader
from config import Config
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName(Config.app_name) \
        .getOrCreate()

    calendar_df = CalendarDataReader(spark, Config).read()
    sales_df = SalesDataReader(spark, Config).read()
    product_df = ProductDataReader(spark, Config).read()
    store_df = StoreDataReader(spark, Config).read()
