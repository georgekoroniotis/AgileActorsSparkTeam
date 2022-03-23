from numpy import double
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f

spark=SparkSession.builder.appName("AgileActorsSparkTeam").getOrCreate()

# Paths
orderItems_input_path = r"C:\Users\user\Documents\AgileActorsSparkTeams\AgileActorsSparkTeam\input_files\orderItemsParts_nested.json"
orderItems_output_path = r"C:\Users\user\Documents\AgileActorsSparkTeams\AgileActorsSparkTeam\output\OrderItemParts"

# Read JSON file
orderItems_df = spark.read.json(orderItems_input_path)              
orderItems_df.printSchema()
orderItems_df.show(5)

# ORDER TABLE - Select only the columns I want in order to create a 3NF table
order_df = orderItems_df.select("o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment")
                     
# LINEITMES TABLE - Select only the columns I want in order to create a 3NF table                      
lineItems_df = orderItems_df.withColumn("o_lineitems", f.explode(f.col("o_lineitems"))).select("o_lineitems.l_orderkey"                                                                                              
                                                                                              ,"o_lineitems.l_suppkey"
                                                                                              ,"o_lineitems.l_linenumber"
                                                                                              ,"o_lineitems.l_quantity"
                                                                                              ,"o_lineitems.l_extendedprice"
                                                                                              ,"o_lineitems.l_discount"
                                                                                              ,"o_lineitems.l_tax"
                                                                                              ,"o_lineitems.l_returnflag"
                                                                                              ,"o_lineitems.l_linestatus"
                                                                                              ,"o_lineitems.l_shipdate"
                                                                                              ,"o_lineitems.l_commitdate"
                                                                                              ,"o_lineitems.l_receiptdate"
                                                                                              ,"o_lineitems.l_shipinstruct"
                                                                                              ,"o_lineitems.l_shipmode"
                                                                                              ,"o_lineitems.l_comment"
                                                                                              ,"o_lineitems.l_parts"
                                                                                              )\
                            .withColumn("l_parts",f.explode(f.col("l_parts"))).select("l_orderkey"
                                                                                     , f.col("l_parts.p_partkey").alias('l_partkey')
                                                                                     ,"l_suppkey"
                                                                                     ,"l_linenumber"
                                                                                     ,"l_quantity"
                                                                                     ,"l_extendedprice"
                                                                                     ,"l_discount"
                                                                                     ,"l_tax"
                                                                                     ,"l_returnflag"
                                                                                     ,"l_linestatus"
                                                                                     ,"l_shipdate"
                                                                                     ,"l_commitdate"
                                                                                     ,"l_receiptdate"
                                                                                     ,"l_shipinstruct"
                                                                                     ,"l_shipmode"
                                                                                     ,"l_comment"
                                                                                     ,"l_parts", )
lineItems_df.printSchema()

# PARTS TABLE - Select only the columns I want in order to create a 3NF table                      
parts_df = lineItems_df.withColumn("l_parts", f.col("l_parts")).select("l_parts.p_partkey"
                                                                      ,"l_parts.p_name"
                                                                      ,"l_parts.p_mfgr"
                                                                      ,"l_parts.p_brand"
                                                                      ,"l_parts.p_type"
                                                                      ,"l_parts.p_size"
                                                                      ,"l_parts.p_container"
                                                                      ,"l_parts.p_retailprice"
                                                                      ,"l_parts.p_comment"
                                                                                 )
parts_df.printSchema()
#parts_df.show(5)

# Drop unnecessary fields
lineItems_df = lineItems_df.drop("l_parts")

# Show my tables
order_df.show(5, truncate=False)
lineItems_df.show(5,truncate=False)
parts_df.show(5,truncate=False)

# write to parquet the 3 tables schemas
order_df.write.mode("overwrite").parquet(orderItems_output_path+r'\orders')
lineItems_df.write.mode("overwrite").parquet(orderItems_output_path+r'\lineItems')
parts_df.write.mode("overwrite").parquet(orderItems_output_path+r'\parts')

# write the initial json file to parquet file
orderItems_df.write.mode("overwrite").parquet(orderItems_output_path+r'\orderItemsJSON')

