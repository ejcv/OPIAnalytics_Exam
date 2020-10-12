import pyspark.sql.types as st
from pyspark.sql import udf, Window, SparkSession
from pyspark.sql import functions as f
import os

spark = SparkSession.builder.appName('OPI_Exam').getOrCreate()

RAW_DATA_PATH = "./teinvento_inc/ventas_reportadas_mercado_tamales/mx/20200801/"

# TeInvento Inc data
FACT_DATA_PATH = "{0}fact_table/".format(RAW_DATA_PATH)
PRODUCT_DIM_DATA_PATH = "{0}product_dim/".format(RAW_DATA_PATH)
REGION_DIM_DATA_PATH = "{0}region_dim/".format(RAW_DATA_PATH)

fact_data_schema = st.StructType([
    st.StructField('year', st.StringType(), True),
    st.StructField('month', st.StringType(), True),
    st.StructField('sales', st.DecimalType(10, 2), True),
    st.StructField('region_id', st.StringType(), True),
    st.StructField('product_id', st.StringType(), True)

])

region_data_schema = st.StructType([
    st.StructField('region_id', st.StringType(), True),
    st.StructField('country', st.StringType(), True),
    st.StructField('location', st.StringType(), True)

])

product_data_schema = st.StructType([
    st.StructField('product_id', st.StringType(), True),
    st.StructField('type', st.StringType(), True),
    st.StructField('vendor', st.StringType(), True),
    st.StructField('flavor', st.StringType(), True),
    st.StructField('manufacturer', st.StringType(), True)

])

# Read raw data

fact_table_df = spark.read.schema(fact_data_schema).csv(FACT_DATA_PATH)
product_dim_table_df = spark.read.schema(product_data_schema).csv(PRODUCT_DIM_DATA_PATH)
region_dim_table_df = spark.read.schema(region_data_schema).csv(REGION_DIM_DATA_PATH)

fact_table_df = fact_table_df.join(product_dim_table_df, on=['product_id'], how='inner') \
    .join(region_dim_table_df, on=['region_id'], how='inner')
# product_dim_table_df.show()

fact_table_df = fact_table_df.filter((fact_table_df.manufacturer == 'Tamales Inc'))

monthly_sales_df = fact_table_df.select(['year', 'month', 'sales']).groupBy(['year', 'month']).sum()\
    .withColumn('monthNumber', f.date_format(f.to_date(f.col('month'), 'MMM'), 'MM').cast('int'))\
    .select('year', 'monthNumber', 'sum(sales)').sort('year', 'monthNumber')

# total_sales = monthly_sales_df.select('sum(sales)').agg(f.sum('sum(sales)')).collect()[0][0]

partition = Window.partitionBy("year") \
    .orderBy("year", "monthNumber") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

partition_2 = Window.partitionBy().orderBy("year", "monthNumber")

monthly_sales_df = monthly_sales_df.withColumn("sales_yearly_cum_sum", f.sum(f.col('sum(sales)')).over(partition))\
    .withColumn('monthly_perc_change', ((f.col('sum(sales)') / (f.lag(f.col('sum(sales)')).over(partition_2)) - 1) * 100)
                .cast(st.DecimalType(10, 2)))
monthly_sales_df.show()
