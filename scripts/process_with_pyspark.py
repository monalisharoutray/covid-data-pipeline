from pyspark.sql import *
from pyspark import *
from datetime import datetime
from pyspark.sql.functions import *

spark=SparkSession.builder \
    .appName("processCovidData") \
    .getOrCreate()

today = datetime.today().strftime('%Y-%m-%d')
input_path = f"../data/raw/owid_covid_data_{today}.csv"
df=spark.read \
    .format("csv") \
    .option("header","true") \
    .option ("inferschema","true") \
    .load(input_path)
#df.show()

df_India=df.where((col("location")=="India") & (col("total_cases")!=0))
df_Summary=df_India.withColumn("date",to_date(col("date"))) \
        .groupby("date") \
        .agg(sum("total_cases").alias("total_cases"),sum("total_deaths").alias("total_deaths")) \
        .withColumn("death_percentage", round((col("total_deaths") / col("total_cases")) * 100, 2)) \
        .orderBy("date")
outputpath=f"../data/processed/india_summary_{today}.csv"
df_Summary.coalesce(1).write \
        .mode("overwrite") \
        .option("header","true") \
        .format("csv") \
        .save(outputpath)
print(f"Processed data saved to: {outputpath}")


spark.stop()
# Group by date, sum total_cases and total_deaths




