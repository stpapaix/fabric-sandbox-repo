# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b236d441-d5db-44b7-8d6d-a5abff5c2edb",
# META       "default_lakehouse_name": "my_sandbox_lakehouse",
# META       "default_lakehouse_workspace_id": "23aba859-6448-484f-8fe2-2d174c50adc5",
# META       "known_lakehouses": [
# META         {
# META           "id": "b236d441-d5db-44b7-8d6d-a5abff5c2edb"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import year,month,days,col,to_timestamp,date_format
from pyspark.sql.functions import to_date
from notebookutils import mssparkutils
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.read.format("csv") \
    .option("header","true") \
    .load([
        "Files/bronze-sandbox/yellow_tripdata_2016-01.csv",
        "Files/bronze-sandbox/yellow_tripdata_2016-02.csv",
        "Files/bronze-sandbox/yellow_tripdata_2016-03.csv"
    ])
# df now is a Spark DataFrame containing CSV data from "Files/bronze-sandbox/yellow_tripdata_2016-0?.csv".
print(df2.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv") \
    .option("header","true") \
    .load("Files/bronze-sandbox/yellow_tripdata_201?-0?.csv")
# df now is a Spark DataFrame containing CSV data from "Files/bronze-sandbox/yellow_tripdata_201?-0?.csv".

print(df.count())
# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# adding new columns to the dataframe - year, month, day, hour, and minute
df_output = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
        .withColumn("day", date_format(col("tpep_pickup_datetime"), "dd")) \
        .withColumn("year", date_format(col("tpep_pickup_datetime"), "yyyy")) \
        .withColumn("month", date_format(col("tpep_pickup_datetime"), "MM"))

# displaying selected columns
print(df_output.count())
df_output.select('tpep_pickup_datetime', 'day', 'month', 'year').show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Setup output folder in datalake
output_folder = 'yellow_trip_data_partitioned'
output_path = 'abfss://23aba859-6448-484f-8fe2-2d174c50adc5@onelake.dfs.fabric.microsoft.com/b236d441-d5db-44b7-8d6d-a5abff5c2edb/Files/bronze-sandbox/'+output_folder

#write data into lakehouse using partitioning 
df_output.write.mode("overwrite").partitionBy('year','month','day').parquet(output_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Analyze value distribution for the 'year', 'month', 'day' columns
for col_name in ['year', 'month', 'day']:
    print(f"\nValue distribution for column: {col_name}")
    df_output.groupBy(col(col_name)).count().orderBy(col("count").desc()).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the path to the partitioned data
partitioned_data_path = 'abfss://23aba859-6448-484f-8fe2-2d174c50adc5@onelake.dfs.fabric.microsoft.com/b236d441-d5db-44b7-8d6d-a5abff5c2edb/Files/bronze-sandbox/yellow_trip_data_partitioned'

# Load the partitioned data into a DataFrame
df_partitioned = spark.read.parquet(partitioned_data_path)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_output.count())
print(df_partitioned.count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# test sql pyspark request with group by on partioned data

from pyspark.sql.functions import col

# Group by 'day' and count the number of rows for each day
day_counts = df_partitioned.groupBy(col('day')).count().orderBy(col("count").desc())

# Display the result
day_counts.show()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

# Define the path in OneLake (your Fabric Data Lake)
output_path  = "abfss://23aba859-6448-484f-8fe2-2d174c50adc5@onelake.dfs.fabric.microsoft.com/b236d441-d5db-44b7-8d6d-a5abff5c2edb/Files/bronze-sandbox/log/mylog.txt"

# Log message
log_message = f"Test completed successfully at {datetime.now()}\n"

# Convert the message to an RDD
log_rdd = spark.sparkContext.parallelize([log_message])

# Save the log message as a text file
log_rdd.coalesce(1).saveAsTextFile(output_path)

print("Log message saved successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# test data creation with partition and mesure execution time for a sql pyspark request with groupBy on NON partioned data

import time
from pyspark.sql.functions import year,month,days,col,to_timestamp,date_format
from pyspark.sql.functions import to_date

print("Start Process")


# LOAD NON PARTITIONED DATA FROM csv
# **********************************

print("Load NON Partitioned data ongoing...")
start_time = time.time()

# load data from csv file
df = spark.read.format("csv") \
    .option("header","true") \
    .load("Files/bronze-sandbox/yellow_tripdata_201?-0?.csv")

# adding new columns to the dataframe - year, month, day, hour, and minute
df1 = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"))) \
        .withColumn("day", date_format(col("tpep_pickup_datetime"), "dd")) \
        .withColumn("year", date_format(col("tpep_pickup_datetime"), "yyyy")) \
        .withColumn("month", date_format(col("tpep_pickup_datetime"), "MM"))
df_nonpart_count = df1.count()
end_time = time.time()

print(f"Row number for NON partitioned dataframe : {df_nonpart_count} / Time to load data {end_time - start_time} secondes") 


# LOAD PARTITIONED DATA FROM csv
# **********************************

print("Load Partitioned data ongoing...")
start_time = time.time()

# Define the path to the partitioned data
partitioned_data_path = 'abfss://23aba859-6448-484f-8fe2-2d174c50adc5@onelake.dfs.fabric.microsoft.com/b236d441-d5db-44b7-8d6d-a5abff5c2edb/Files/bronze-sandbox/yellow_trip_data_partitioned'

# Load the partitioned data into a DataFrame
df2 = spark.read.parquet(partitioned_data_path)
df_part_count = df2.count()
end_time = time.time()

print(f"Row number for NON partitioned dataframe : {df_part_count} / Time to load data {end_time - start_time} secondes") 


# Executio pyspark SQL with groupBy on day and compare execution time beetwen Partitioned and non Partitioned
# ***********************************************************************************************************

print("Pyspark SQL execution ongoing...")

start_time1 = time.time()
# Group by 'day' and count the number of rows for each day
day_counts1 = df1.groupBy(col('day')).count().orderBy(col("count").desc())
day_counts1.count()
end_time1 = time.time()

start_time2 = time.time()
# Group by 'day' and count the number of rows for each day
day_counts2 = df2.groupBy(col('day')).count().orderBy(col("count").desc())
day_counts2.count()
end_time2 = time.time()

print(f"Execution duration on NON Partitionned data : {end_time1 - start_time1} secondes")
print(f"Execution duration on Partitionned data : {end_time2 - start_time2} secondes")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

