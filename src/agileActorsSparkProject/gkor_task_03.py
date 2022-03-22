from numpy import double
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f

spark=SparkSession\
        .builder\
            .appName("AgileActorsSparkTeam")\
                .getOrCreate()


input_path = r"C:\\Users\\user\\Documents\\AgileActorsSparkTeams\\AgileActorsSparkTeam\\output\\OrderItemParts\\"

orderItems_df = spark.read.parquet(input_path+'orderItemsJSON')
lineItems_df = spark.read.parquet(input_path+'lineItems')
orders_df = spark.read.parquet(input_path+'orders')
parts_df = spark.read.parquet(input_path+'parts')


# After tasks 1 and 3 you will have the same dataset in json , 
# parquet nested and parquet simple files . Now we will run 
# queries over each different file format. We want to run the 
# same queries but with the syntax adjusted to the nested or flat file formats.

# Please select any query from here (eg query 10) and adapt it to run for nested and flat files. 
# Ideally it should run over the 3NF parquet files as is. 
# If some entity in a query is missing in our data just ignore it and remove it from the query. 
# Make sure you run the query as both SQL and PySpark api.

# Run QUERY 1 using flat file using SQL API
lineItems_df.createOrReplaceTempView('lineitem')

query_01 = spark.sql(""" select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= '1998-09-16'
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus;
    """)
query_01.show()    

# Run QUERY 1 using flat file using PySpark Api

lineItems_df.filter(lineItems_df['l_shipdate'] <= '1998-09-16')\
                .groupBy('l_returnflag','l_linestatus')\
                    .agg((sum("l_quantity").alias("sum_qty"),\
                          sum("l_extendedprice").alias("sum_base_price"),\
                          sum("l_extendedprice" * (1 - "l_discount")).alias("sum_disc_price"),\
                          sum("l_extendedprice" * (1 - "l_discount") * (1 + "l_tax")).alias("sum_charge"),\
                          avg("l_quantity").alias('avg_qty'),\
                          avg("l_extendedprice").alias('avg_price'),\
                          avg("l_discount").alias("avg_disc"),
                          count("*").alias("count_order")\    
                        ))).show()



