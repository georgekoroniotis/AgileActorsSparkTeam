from numpy import double
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f

spark=SparkSession.builder.appName("AgileActorsSparkTeam").getOrCreate()


orderItems_input_path = r"C:\Users\user\Documents\AgileActorsSparkTeams\AgileActorsSparkTeam\input_files\orderItemsParts_nested.json"
orderItems_output_path = r"C:\Users\user\Documents\AgileActorsSparkTeams\AgileActorsSparkTeam\output\OrderItemParts"


orderItems_schema = StructType([StructField("CLERK",StringType(),True),
                                StructField("COMMENT",StringType(),True),
                                StructField("CUSTKEY",IntegerType(),True),
                                StructField("LINEITEMS",StructType([
                                    StructField("COMMENT",StringType(),True),
                                    StructField("COMMITDATE",StringType(),True),
                                    StructField("DISCOUNT",DoubleType(),True),
                                    StructField("EXTENTEDPRICE",DoubleType(),True),
                                    StructField("LINENUMBER",IntegerType(),True),
                                    StructField("LINESTATUS",StringType(),True),
                                    StructField("ORDERKEY",IntegerType(),True),
                                    StructField("PARTS",StructType([
                                        StructField("BRAND",StringType(),True),
                                        StructField("COMMENT",StringType(),True),
                                        StructField("CONTAINER",StringType(),True),
                                        StructField("MFGR",StringType(),True),
                                        StructField("NAME",StringType(),True),
                                        StructField("PARTKEY",IntegerType(),True),
                                        StructField("RETAILPRICE",DoubleType(),True),
                                        StructField("SIZE",LongType(),True),
                                        StructField("TYPE",DoubleType(),True)
                                        ]),True),
                                    StructField("QUANTITY",DoubleType(),True),
                                    StructField("RECEIPTDATE",StringType(),True),
                                    StructField("RETURNFLAG",StringType(),True),
                                    StructField("SHIPDATE",StringType(),True),
                                    StructField("SHIPINSTRUCT",StringType(),True),
                                    StructField("SHIPMODE",StringType(),True),
                                    StructField("SUPPKEY",LongType(),True),
                                    StructField("TAX",DoubleType(),True),
                                    ]),True),
                                StructField("ORDERDATE",StringType(),True),
                                StructField("ORDERKEY",LongType(),True),
                                StructField("ORDERPRIORITY",StringType(),True),
                                StructField("ORDERSTATUS",StringType(),True),
                                StructField("SHIPPRIORITY",LongType(),True),
                                StructField("TOTALPRICE",DoubleType(),True)
                            ])


orderItems_df = spark.read.json(orderItems_input_path)
               #.schema(orderItems_schema)\
               
orderItems_df.printSchema()
orderItems_df.show(5)


orders_schema = StructType([
    StructField("ORDERKEY",IntegerType(),True),
    StructField("CUSTKEY",IntegerType(),True),
    StructField("ORDERSTATUS",StringType(),True),
    StructField("TOTALPRICE",DoubleType(),True),
    StructField("ORDERDATE",StringType(),True),
    StructField("ORDER-PRIORITY",StringType(),True),
    StructField("CLERK",StringType(),True),
    StructField("SHIP-PRIORITY",IntegerType(),True),
    StructField("COMMENT",StringType(),True),
])

line_item_schema = StructType([
    StructField("ORDERKEY",IntegerType(),True),
    StructField("PARTKEY",IntegerType(),True),
    StructField("SUPPKEY",IntegerType(),True),
    StructField("LINENUMBER",IntegerType(),True),
    StructField("QUANTITY",IntegerType(),True),
    StructField("EXTENTEDPRICE",DoubleType(),True),
    StructField("DISCOUNT",DoubleType(),True),
    StructField("TAX",IntegerType(),True),
    StructField("RETURNFLAG",IntegerType(),True),
    StructField("LINESTATUS",StringType(),True),
    StructField("SHIPDATE",IntegerType(),True),
    StructField("COMMITDATE",StringType(),True),
    StructField("RECEIPTDATE",IntegerType(),True),
    StructField("SHIPINSTRUCT",IntegerType(),True),
    StructField("SHIPMODE",IntegerType(),True),
    StructField("COMMENT",StringType(),True),
    ])

part_schema = StructType([
    StructField("PARTKEY",IntegerType(),True),
    StructField("NAME",IntegerType(),True),
    StructField("MFGR",IntegerType(),True),
    StructField("BRAND",IntegerType(),True),
    StructField("TYPE",IntegerType(),True),
    StructField("SIZE",IntegerType(),True),
    StructField("CONTAINER",IntegerType(),True),
    StructField("RETAILPRICE",IntegerType(),True),
    StructField("COMMENT",IntegerType(),True),
    ])

#order_df = orderItems_df.select("o_orderkey","o_custkey","o_orderstatus","o_totalprice","o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority", "o_comment")
# line_item_df = orderItems_df.select(f.array(f.expr("o_lineitems.*")))
# line_item_df = orderItems_df.select("o_lineitmes.element.l_orderkey"
#                                   , "o_lineitmes.element.l_parts.element.p_partkey"
#                                   , "o_lineitmes.element.l_suppkey"
#                                   , "o_lineitmes.element.l_linenumber"
#                                   , "o_lineitmes.element.l_quantity"
#                                   , "o_lineitmes.element.l_extendedprice"
#                                   , "o_lineitmes.element.l_discount"
#                                   , "o_lineitmes.element.l_tax"
#                                   , "o_lineitmes.element.l_returnflag"
#                                   , "o_lineitmes.element.l_linestatus"
#                                   , "o_lineitmes.element.l_shipdate"
#                                   , "o_lineitmes.element.l_commitdate"
#                                   , "o_lineitmes.element.l_receiptdate"
#                                   , "o_lineitmes.element.l_shipinstruct"
#                                   , "o_lineitmes.element.l_shipmode"
#                                   , "o_lineitmes.element.l_comment"
#                                   )
# part_df = orderItems_df.select("o_lineitems.element.l_parts.element.p_partkey", 
#                                 "o_lineitems.element.l_parts.element.p_name", 
#                                 "o_lineitems.element.l_parts.element.p_mfgr", 
#                                 "o_lineitems.element.l_parts.element.p_brand:", 
#                                 "o_lineitems.element.l_parts.element.p_type", 
#                                 "o_lineitems.element.l_parts.element.p_size", 
#                                 "o_lineitems.element.l_parts.element.p_container", 
#                                 "o_lineitems.element.l_parts.element.p_retailprice", 
#                                 "o_lineitems.element.l_parts.element.p_comment"
#                               )                              

#order_df.show(5)
#lineItems_df = orderItems_df.select(orderItems_df.o_orderkey, f.explode(orderItems_df.o_lineitems).alias('lineItems'))
lineItems_df = orderItems_df.withColumn("o_lineitems", f.explode(f.col("o_lineitems"))).select("o_lineitems.*")
lineItems_df.printSchema()
lineItems_df.show(5)
#part_df.show(5)


parts_df = lineItems_df.withColumn("l_parts", f.explode(f.col("l_parts"))).select("l_parts.*")
parts_df.printSchema()
parts_df.show(5)

