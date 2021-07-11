from typing import Type
from pyspark.sql import dataframe, functions as F, types as T
from config import Config


class JsonWriter(object):
    format_return_type = T.StructType() \
        .add("rowId", T.StringType()) \
        .add("dataRow", T.MapType(T.StringType(), T.DoubleType()), False)

    expected_schema = T.StructType()\
        .add("division", T.StringType())\
        .add("gender", T.StringType())\
        .add("category", T.StringType())\
        .add("channel", T.StringType())\
        .add("year", T.IntegerType())\
        .add("weekNumber", T.StringType())\
        .add("totalNetSales", T.DoubleType()) \
        .add("totalSalesUnits", T.DoubleType()) \
        .add("dataRow", T.ArrayType(T.MapType(T.StringType(), T.DoubleType())))

    def __init__(self, input_data: dataframe, config: Type[Config]):
        self.df = input_data
        self.config = config

    @staticmethod
    def _get_json(pdf, column_name):
        return dict(zip(pdf["weekNumber"].values, pdf[column_name]))

    def collect_rows(self, pdf):
        net_sales = self._get_json(pdf, "totalNetSales")
        unit_sales = self._get_json(pdf, "totalSalesUnits")
        return pdf.assign(dataRow=[net_sales, unit_sales])

    @F.udf(format_return_type)
    def _format(self):
        # uniqueKey, division, gender, category, channel, year, weekNumber, totalNetSales, totalSalesUnits
        return self.df \
            .withColumn("weekNumber", F.lit("W") + F.col("weekNumber")) \
            .groupBy("division", "gender", "category", "year") \
            .applyInPandas(self.collect_rows, schema=self.expected_schema)\
            .withColumn("year", F.lit("RY") + F.date_format("year", "yy").cast(T.StringType()))\
            .drop("totalNetSales", "totalSalesUnits")\
            .withColumn("uniqueKey", F.concat_ws("_", "year", "channel", "division", "gender", "category"))\
            .partitionBy("uniqueKey")

    def write(self):
        self._format()\
            .write\
            .parition("uniqueKey")\
            .json(self.config.reporting_output_file)
