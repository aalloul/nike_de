from src.readers.calendar_data_reader import CalendarDataReader
from src.readers.product_data_reader import ProductDataReader
from src.readers.sales_data_reader import SalesDataReader
from src.readers.store_data_reader import StoreDataReader
from src.aggregators.weekly_aggregator import WeeklyAggregator
from src.writers.json_writer import JsonWriter
from src.config import Config
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName(Config.app_name) \
        .getOrCreate()

    calendar_df = CalendarDataReader(spark, Config).read()
    sales_df = SalesDataReader(spark, Config).read()
    product_df = ProductDataReader(spark, Config).read()
    store_df = StoreDataReader(spark, Config).read()

    aggregated_sales = WeeklyAggregator(calendar_df, sales_df, product_df, store_df).aggregate_per_week()

    JsonWriter(aggregated_sales, Config).write()
