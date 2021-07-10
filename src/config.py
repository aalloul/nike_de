class Config(object):
    app_name = "Sales weekly aggregator"

    input_calendar = "s3://s3-nl-prd-semrb-emr-datascience/adam/calendar.csv"
    input_product = "s3://s3-nl-prd-semrb-emr-datascience/adam/product.csv"
    input_sales = "s3://s3-nl-prd-semrb-emr-datascience/adam/sales.csv"
    input_store = "s3://s3-nl-prd-semrb-emr-datascience/adam/store.csv"

    reporting_year = 2018
    reporting_week = None

    reporting_output_file = "s3://s3-nl-prd-semrb-emr-datascience/adam/consumption"
