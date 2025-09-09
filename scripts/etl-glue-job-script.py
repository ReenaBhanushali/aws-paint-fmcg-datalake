import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load Data from Glue Catalog
sales = glueContext.create_dynamic_frame.from_catalog(database="paint_fmcg_db", table_name="sales_orders")
inventory = glueContext.create_dynamic_frame.from_catalog(database="paint_fmcg_db", table_name="inventory")
production = glueContext.create_dynamic_frame.from_catalog(database="paint_fmcg_db", table_name="production_plan")

# Convert to DataFrames
df_sales = sales.toDF()
df_inventory = inventory.toDF()
df_production = production.toDF()

#alias
df_sales_a = df_sales.alias("a")
df_inventory_b = df_inventory.alias("b")
df_production_c = df_production.alias("c")

# Join datasets
df_joined = df_sales_a.join(df_inventory_b, col("a.product_id") == col("b.product_id"), "left") \
                    .join(df_production_c,col("a.product_id") == col("c.product_id"), "left")

# Aggregate demand by region & product
df_demand = df_joined.groupBy("a.region","a.product_name").sum("a.quantity")

# Save back to S3 (processed zone)
df_demand.write.mode("overwrite").parquet("s3://paint-fmcg-datalake/processed/demand_summary/")

job.commit()
