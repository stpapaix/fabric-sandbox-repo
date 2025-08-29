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

df = spark.read.format("csv") \
    .option("header","true") \
    .load("Files/bronze-sandbox/yellow_tripdata_201?-0?.csv")
# df now is a Spark DataFrame containing CSV data from "Files/bronze-sandbox/yellow_tripdata_201?-0?.csv".

# print(df.count())
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
# print(df_output.count())
# df_output.select('tpep_pickup_datetime', 'day', 'month', 'year').show()

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

# MARKDOWN ********************

