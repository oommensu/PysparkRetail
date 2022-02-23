# Sample spark-streaming.py where you will write the Spark Streaming code for the project

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import from_json


#Utility function to sum the quantity 
def get_tot_items(items):
    tot = 0
    for item in items:
        tot = tot + item['quantity']
    return tot
 
#Utility function to calculate the total cost 
def get_tot_sale_volume(items, otypes):
    tot = 0.0
    for item in items:
        if otypes == "ORDER":
            tot = tot + (item['quantity'] * item['unit_price'])
        else:
            tot = tot - (item['quantity'] * item['unit_price'])
    return tot


#Utility function to get the total order 
def get_is_order(otypes):
    tot = 0
    if otypes == "ORDER":
        tot = tot + 1
    return tot

#Utility function to calculate the total order's returned 
def get_is_return(otypes):
    tot = 0
    if otypes == "RETURN":
        tot = tot + 1
    return tot
        

# Input the Kafka host from Command line
host = sys.argv[1]
port = sys.argv[2]
topic = sys.argv[3]

spark = SparkSession \
	.builder \
	.appName("PysparkU") \
	.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')	
	
#Define the order schema
mySchema = StructType() \
    .add("invoice_no", LongType()) \
    .add("country",StringType()) \
    .add("timestamp",TimestampType()) \
    .add("type", StringType()) \
    .add("items", ArrayType(StructType([StructField("SKU", StringType()), \
                                        StructField("title", StringType()), \
                                        StructField("unit_price", DoubleType()), \
                                        StructField("quantity", IntegerType()) \
        ]))) 
   

#Fetch data from Kafka server
lines = spark.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", host + ":" + port) \
	.option("startingOffsets","latest") \
	.option("subscribe", topic) \
	.load()


kafkaStream1 = lines.select(from_json(col("value").cast("String"), mySchema).alias("data")).select("data.*")

# To calculate additional data using UDFs
add_tot_items = udf(lambda x : get_tot_items(x), IntegerType())

add_tot_sale_volume = udf(lambda x,y : get_tot_sale_volume(x,y), DoubleType())

is_order = udf(lambda x : get_is_order(x), IntegerType())

is_return = udf(lambda x : get_is_return(x), IntegerType())

kafka_tot_Stream = kafkaStream1 \
               	    .withColumn("total_cost", add_tot_sale_volume(kafkaStream1.items, kafkaStream1.type)) \
                    .withColumn("total_items", add_tot_items(kafkaStream1.items)) \
                    .withColumn("is_order", is_order(kafkaStream1.type)) \
                    .withColumn("is_return", is_return(kafkaStream1.type)) 

#Total input values to the Console
query = kafka_tot_Stream \
    	.select ("invoice_no","country", "timestamp", "total_cost", "total_items", "is_order", "is_return") \
		.writeStream \
		.outputMode("append") \
		.format("console") \
    	.option("truncate", "false") \
    	.trigger(processingTime = "1 minute") \
		.start()
	
    
#Calculate the time- based KPIs with tumbling window of one minute
aggTime = kafka_tot_Stream \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window("timestamp", "1 minute", "1 minute")) \
        .agg( sum("total_cost").alias("total_sale_volume"), \
            count("invoice_no").alias("OPM"), \
            (count("is_return")/count("invoice_no")).alias("rate_of_return"), \
            (sum("total_cost")/count("invoice_no")).alias("average_transaction_size")) \
        .select("window", "OPM", "total_sale_volume", "average_transaction_size", "rate_of_return") 
		
	    
#Calculate the country and time- based KPIs with tumbling window of one minute
aggTimeCountry = kafka_tot_Stream \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window("timestamp", "1 minute", "1 minute"), "country") \
        .agg( sum("total_cost").alias("total_sale_volume"), \
            count("invoice_no").alias("OPM"), \
            (count("is_return")/count("invoice_no")).alias("rate_of_return")) \
        .select("window", "country", "OPM", "total_sale_volume", "rate_of_return")


# Write the KPIs calculated above to HDFS in json format
queryTimeCountry = aggTimeCountry.writeStream \
	.outputMode("append") \
	.format("json") \
    	.option("truncate", "false") \
    	.option("path", "/prosuby4/data2") \
    	.option("checkpointLocation", "/prosuby4/cp2") \
    	.trigger(processingTime = "1 minute") \
		.start() 

queryTime = aggTime.writeStream \
	.outputMode("append") \
	.format("json") \
    	.option("truncate", "false") \
    	.option("path", "/prosuby4/data1") \
    	.option("checkpointLocation", "/prosuby4/cp1") \
    	.trigger(processingTime = "1 minute") \
		.start() \
        .awaitTermination()



