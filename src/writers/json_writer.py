from typing import Type
from pyspark.sql import dataframe, types as T
from config import Config


class JsonWriter(object):
    expected_schema = T.StructType() \
        .add("division", T.StringType()) \
        .add("gender", T.StringType()) \
        .add("category", T.StringType()) \
        .add("channel", T.StringType()) \
        .add("year", T.StringType()) \
        .add("weekNumber", T.StringType()) \
        .add("totalNetSales", T.DoubleType()) \
        .add("totalSalesUnits", T.DoubleType()) \
        .add("dataRows", T.StringType()) \
        .add("uniqueKey", T.StringType())

    def __init__(self, input_data: dataframe, config: Type[Config]):
        self.df = input_data
        self.config = config

    @staticmethod
    def _get_json(pdf, column_name):
        return dict(zip(pdf["weekNumber"].values, pdf[column_name]))


    def collect_rows(self, pdf):
        net_sales = {
            "rowId": "Net Sales",
            "dataRow": self._get_json(pdf, "totalNetSales")
        }
        unit_sales = {
            "rowId": "Sales Units",
            "dataRow": self._get_json(pdf, "totalSalesUnits")
        }
        out = pdf.assign(dataRows=T.json.dumps([net_sales, unit_sales]))
        return out

    def _format(self):
        return self.df \
            .groupBy("uniqueKey") \
            .applyInPandas(self.collect_rows, schema=self.expected_schema)\
            .drop("totalNetSales", "totalSalesUnits")\

    def write(self):
        self._format()\
            .write\
            .parition("uniqueKey")\
            .json(self.config.reporting_output_file)
