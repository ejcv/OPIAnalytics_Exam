import pyspark.sql.types as st
from pyspark.sql import udf, Window, SparkSession
from pyspark.sql import functions as F


def analysis(df):
    monthly_sales_df = df.select(['year', 'month', 'sales']).groupBy(['year', 'month']).sum() \
        .withColumn('monthNumber', F.date_format(F.to_date(F.col('month'), 'MMM'), 'MM').cast('int')) \
        .select('year', 'monthNumber', 'month', 'sum(sales)').sort('year', 'monthNumber')

    partition = Window.partitionBy("year") \
        .orderBy("year", "monthNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    partition_2 = Window.partitionBy().orderBy("year", "monthNumber")

    monthly_sales_df = monthly_sales_df.withColumn("sales_yearly_cum_sum", F.sum(F.col('sum(sales)')).over(partition)) \
        .withColumn('monthly_perc_change',
                    ((F.col('sum(sales)') / (F.lag(F.col('sum(sales)')).over(partition_2)) - 1) * 100)
                    .cast(st.DecimalType(10, 2)))
    return monthly_sales_df.drop('monthNumber')
