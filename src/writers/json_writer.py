from typing import Type
from pyspark.sql import dataframe, types as T
from src.config import Config


class JsonWriter(object):
    """
    This class encapsulates the logic to write data to JSON. It is responsible
    for formatting the input dataframe into the correct format and write it to
    JSON.
    """

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
    def _get_json(pdf: dataframe, column_name: str) -> dict:
        """
        This method simply craetes a dict where the `weekNumber` is
        the key and
        the aggregated value is the value.
        :param pdf: the dataframe containing the data we need
        :param column_name: name of the column where the aggregation is stored
        :return: Dictionary
        """

        return dict(zip(pdf["weekNumber"].values, pdf[column_name]))

    def collect_rows(self, pdf: dataframe) -> dataframe:
        """
        This method is responsible for building the dataRows column
        :param pdf: dataframe coming frmo the applyInPandas used in
        `self._format`
        :return: A dataframe with the `dataRows` column added
        """

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

    def _format(self) -> dataframe:
        """
        This method is responsible for building the final dataframe we need to
        write.
        :return: A dataframe with the columns we need for writing.
        """
        return self.df \
            .groupBy("uniqueKey") \
            .applyInPandas(self.collect_rows, schema=self.expected_schema) \
            .drop("totalNetSales", "totalSalesUnits")

    def write(self):
        """
        This method formats and writes the dataframe in JSON format.
        """

        self._format() \
            .write \
            .parition("uniqueKey") \
            .json(self.config.reporting_output_file)
