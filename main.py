from pyspark.sql.functions import *
from pyspark.sql import SparkSession

"""make an instance of a SparkSession called 'spark'."""
spark = SparkSession.builder.master('local').getOrCreate()

"""SET UP DATA"""
# set our path
data_file = 'data/coffee.csv'
# set schema
customSchema = 'date date, open float, high float, low float, close float, volume float, currency string'
# read our csv into a spark df
sdf = spark.read.csv(data_file, schema=customSchema, header=True)

"""COLUMNS FROM AGGREGATE FUNCTIONS"""
# Add a column to the DataFrame where the values are the difference between 'Open' and 'Close'.
sdf = sdf.withColumn('open2close',
                     (sdf.open - sdf.close))
# Add a column to the DataFrame where the values are the difference between 'High' and 'Low'.
sdf = sdf.withColumn('high2low',
                     (sdf.high - sdf.close))
# Add a column to the DataFrame where the values are 'True' if the volume for that day was 100 or above, and otherwise 'False'.
sdf = sdf.withColumn('did_well',
                     when((sdf.volume > 99), lit(True))
                     .otherwise(lit(False)))
# add another column that contains the absolute values of the numbers in that column.
sdf = sdf.withColumn('day_abs', abs(sdf.open2close))
# Compute a column called net_sales which is the average of opening, high, low, and closing cost times the volume
net_sales = mean(sdf.open + sdf.high + sdf.low + sdf.close) * sdf.volume
# print(net_sales)

"""STATS"""
# Find the average of the values in the column that has the absolute values of the difference between 'Open' and 'Close'.
sdf.select(avg(sdf.day_abs)).show()
# Get the count of values where the 'Volume' was less than 100.
print(sdf.filter(sdf.volume == False).count())
# Find the average 'Open' value.
sdf.select(avg(sdf.open)).show()
# Get the highest 'High' value.
sdf.select(max(sdf.high)).show()

"""WRITE FILE"""
# Write the code to save the DataFrame with the four new columns created above as a Parquet file in the /data directory
sdf.write.parquet('data/coffequet.parquet')

