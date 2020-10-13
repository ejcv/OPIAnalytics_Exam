import pyspark.sql.types as st
from datetime import date
from pyspark.sql import  SparkSession
import useful_functions
import os
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder.appName('OPI_Exam').getOrCreate()

db_path = os.environ.get("DATABASE_PATH")
output_path = os.environ.get("OUTPUT_PATH")

RAW_DATA_PATH = f"{db_path}/teinvento_inc/ventas_reportadas_mercado_tamales/mx/20200801/"

today = date.today().strftime("%Y%m%d")

# TeInvento Inc data
FACT_DATA_PATH = f"{RAW_DATA_PATH}fact_table/"
PRODUCT_DIM_DATA_PATH = f"{RAW_DATA_PATH}product_dim/"
REGION_DIM_DATA_PATH = f"{RAW_DATA_PATH}region_dim/"

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

fact_table_df.write.format('csv').option('header', True).mode('overwrite').option('sep', ',')\
    .save(f'{output_path}/crudo/generador/TeInvento/{today}/')

monthly_sales_df = useful_functions.analysis(fact_table_df)

monthly_sales_df.write.format('csv').option('header', True).mode('overwrite').option('sep', ',')\
    .save(f'{output_path}/procesado/generador/TeInvento/{today}/')
