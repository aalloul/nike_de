# Data engineer assignment 

# Deliverable description
In this assignment, I tried to provide a working solution for that matches 
the expected output. This being said, a few things are missing as I'm not 
able to spend more time on the code:
  - No unit tests are present. I know this is best practice but I lost too 
    much time trying to setup my own Spark environment. Please see below.
    
  - The output file is not exactly as requested for the following reason. I 
    used `partitionBy("uniqueKey")` when writing. I believe this is good 
    practice to avoid bringing the whole dataset into local RAM for example 
    but Spark in that case decides not to write the column into the data 
    itself. On the other hand, if we know the data size will never grow 
    (fixed carinality of `uniqueKey`) then we could decide to have more 
    control by doing the writing bit in `normal` Python.
    
# Spark on Docker
I tried for 4 hours to get a local setup of Spark (1 master, 2 worker nodes) 
but got bitten by mainly 2 issues:
  1. PyArrow requires installation from source of Arrow + building of the 
     wheel file for PyArrow. I was not aware of this as I struggled to get 
     the right libraries installed on the base image. Another blocker was 
     that I was using a Debian base image but turns out Alpine is better for 
     building Arrow. FYI, PyArrow was required for the `udf` functions I 
     defined.
     
  2. I did succeed in getting PyArrow to work but for some reason the worker 
     nodes were using Python 2 while the master node was on Python 3. This 
     doesn't make sense to me as they're both using the same image but I 
     can't spend more time debugging this issue.
     
# Questions in the assignment
## Technology choices I made
  1. Programming language: I'm much more comfortable with Python than any 
   other language, hence my choice.
     
  2. I chose to use the DataFrame API of Spark as it provides a layer of 
     optimisations for the DAG. This way I don't have to deal with the RDD 
     and optimisations. Plus, the PySpark DataFrame API follows rather 
     closely the Pandas DataFrame API. Using `pyspark,sql.functions.
     pandas_udf` and `applyInPandas` also returns actual Pandas DataFrames 
     to work on which is handy.
     
How to run the code:
  1. All configuration is in the `Config` class. This doesn't take command 
     line and if I had more time, I'd definitely use environment variables 
     instead in a Dockerfile.
     
  2. Following the above, please edit the configuration file and edit the file 
     paths and spark master IP.
     
  3. Package the the `src` directory in a zip 
     
  4. SubmitTING with `spark-submit --master <MASTER_IP>  main.PY --py-files src.
     zip` should do the job.
     
# Technical questions
Coding wise, I spent 5 hours in total working on this code. As said above, I 
probably spent the same amount of time trying to make Spark work but failed. 
Had I been able to use these 5 hours coding, I would have:
  - covered my code with unit tests
  - packaged my code in a docker file
  - spent more time on the output so it looks exactly as required

The most useful feature of PySpark I found is the DataFrame API and the 
possibility of defining functions that can act of Pandas DataFrames. This 
can be seen in the `json_writer` for example:
```python
...
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
            .drop("totalNetSales", "totalSalesUnits", "weekNumber")
```

To track performance on production, I'd probably try to write into some DB 
(Elasticsearch for example) metrics that are of interest to me. For example 
RAM usage, time spent per run, time spent per function. This way we can have 
dashboards that tell us exactly the current performance. If we see anything 
RAM usage increasing or the code takes too long, then we can dive into the 
specific function that's causing this and try to improve it. 

If any bugs are detected in production, this should raise an alarm for a 
quick response. I would thus work on setting up such alarms.

I have not had to do such tasks with data in the terabyte range but I 
believe the simple principles outlined above should help in case of issues.