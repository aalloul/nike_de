from pyspark.sql import functions as F, dataframe


class WeeklyAggregator(object):
    def __init__(self, calendar: dataframe, sales: dataframe,
                 product: dataframe, store: dataframe):
        self.calendar = calendar
        self.sales = sales
        self.product = product
        self.store = store

    def _build_full_df(self) -> dataframe:
        """
        This method builds a `pyspark.sql.dataframe` that contains all
        the possible sales on the website by creating a`crossJoin` between
        `product`, `calendar` and `store` dataframes
        :return: A dataframe.
        """

        return self.product.crossJoin(self.calendar).crossJoin(self.store)

    def aggregate_per_week(self) -> dataframe:
        """
        This method builds the weekly aggregation based on the input sales data.
        :return:
        """

        all_possible_sales = self._build_full_df()

        # `fillna(0)` is chosen because a `np.nan` simply means that there was
        # no sales for the tuple
        # `dateId, store_id, productId`
        actual_sales = all_possible_sales \
            .join(self.sales, ["dateId", "storeId", "productId"], how="left") \
            .fillna(0)

        key_cols = ["division", "gender", "category", "channel", "year",
                    "weekNumber"]
        return actual_sales \
            .groupBy(*key_cols) \
            .agg(F.sum("netSales").alias("totalNetSales"),
                 F.sum("salesUnits").alias("totalSalesUnits")) \
            .withColumn("uniqueKey",
                        F.concat_ws("_", "year", "channel", "division",
                                    "gender", "category"))
