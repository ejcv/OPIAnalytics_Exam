import pyspark.sql.types as st
from pyspark.sql import SparkSession
from datetime import date
import os
import useful_functions
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder.appName('OPI_Exam').getOrCreate()

db_path = os.environ.get("DATABASE_PATH")
output_path = os.environ.get("OUTPUT_PATH")

RAW_DATA_PATH = f"{db_path}/tamales_inc/ventas_mensuales_tamales_inc/"

today = date.today().strftime("%Y%m%d")

tamales_schema = st.StructType([
    st.StructField('year', st.StringType(), True),
    st.StructField('month', st.StringType(), True),
    st.StructField('country', st.StringType(), True),
    st.StructField('type', st.StringType(), True),
    st.StructField('flavor', st.StringType(), True),
    st.StructField('location', st.StringType(), True),
    st.StructField('unknown', st.StringType(), True),
    st.StructField('masa', st.StringType(), True),
    st.StructField('sales', st.DecimalType(10, 2), True),

])

csv_files = [os.path.join(root, name)
             for root, dirs, files in os.walk(RAW_DATA_PATH)
             for name in files
             if name.endswith((".csv"))]

# Read raw data

df = spark.read.schema(tamales_schema).csv(csv_files)
df.write.format('csv').option('header', True).mode('overwrite').option('sep', ',')\
    .save(f'{output_path}/crudo/generador/TamalesInc/{today}/')

monthly_sales_df = useful_functions.analysis(df)

monthly_sales_df.write.format('csv').option('header', True).mode('overwrite').option('sep', ',')\
    .save(f'{output_path}/procesado/generador/TamalesInc/{today}/')