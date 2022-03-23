
#import requests
import pyspark

from pyspark.sql import SparkSession

from pyspark.sql.types import *

#from pyspark.sql import SparkSession


spark=SparkSession.builder.appName("AgileActorsSparkTeam").getOrCreate()

# spark = SparkSession.builder()\
#       .master("local[1]")\
#       .appName("AgileActorsSparkTeam")\
#       .getOrCreate()  

#


# Open and parse a file with data using DataFrames and spark
my_path = r"C:\Users\user\Documents\AgileActorsSparkTeams\AgileActorsSparkTeam\input_files\dataset\orders.tbl"

# 2. Create a Spark StructType to define the file’s schema , enforce the schema to the DataFrame , show the contents in the console
schema = StructType() \
      .add("ORDERKEY",IntegerType(),True) \
      .add("CUSTKEY",IntegerType(),True) \
      .add("ORDERSTATUS",StringType(),True) \
      .add("TOTALPRICE",DoubleType(),True) \
      .add("ORDERDATE",StringType(),True) \
      .add("ORDER-PRIORITY",StringType(),True) \
      .add("CLERK",StringType(),True) \
      .add("SHIP-PRIORITY",StringType(),True) \
      .add("COMMENT",StringType(),True) \

df_orders = spark.read \
      .options(delimiter='|') \
      .schema(schema) \
      .csv(my_path)      
      
df_orders.show()

# 3. Save the Dataframe as a Parquet file
df_orders.write.mode("overwrite").parquet(r"C:\Users\user\Documents\AgileActorsSparkTeams\AgileActorsSparkTeam\output\orders.parquet")

# 4. Select a query (any query) from https://github.com/hortonworks/hive-testbench/tree/hdp3/sample-queries-tpcds . 
# Repeat in a single file all the steps above for the tables (files ) mentioned in the Query . 
# Check the different join methods and if any aggregate functions are used.

store_returns_path = r"C:\Users\user\Documents\AgileActorsSparkTeams\AgileActorsSparkTeam\input_files\tpcds_1gb\store_returns.dat"
date_dim_path = r"C:\Users\user\Documents\AgileActorsSparkTeams\AgileActorsSparkTeam\input_files\tpcds_1gb\date_dim.dat"
store_path = r"C:\Users\user\Documents\AgileActorsSparkTeams\AgileActorsSparkTeam\input_files\tpcds_1gb\store.dat"
customer_path = r"C:\Users\user\Documents\AgileActorsSparkTeams\AgileActorsSparkTeam\input_files\tpcds_1gb\customer.dat"

store_returns_schema = StructType() \
      .add("sr_returned_date_sk",IntegerType(),True) \
      .add("sr_return_time_sk",IntegerType(),True) \
      .add("sr_item_sk",IntegerType(),True) \
      .add("sr_customer_sk",IntegerType(),True) \
      .add("sr_cdemo_sk",IntegerType(),True) \
      .add("sr_hdemo_sk",IntegerType(),True) \
      .add("sr_addr_sk",IntegerType(),True) \
      .add("sr_store_sk",IntegerType(),True) \
      .add("sr_reason_sk",IntegerType(),True) \
      .add("sr_ticket_number",IntegerType(),True) \
      .add("sr_return_quantity",IntegerType(),True) \
      .add("sr_return_amt",DoubleType(),True) \
      .add("sr_return_tax",DoubleType(),True) \
      .add("sr_return_amt_inc_tax",DoubleType(),True) \
      .add("sr_fee",DoubleType(),True) \
      .add("sr_return_ship_cost",DoubleType(),True) \
      .add("sr_refunded_cash",DoubleType(),True) \
      .add("sr_reversed_charge",DoubleType(),True) \
      .add("sr_store_credit",DoubleType(),True) \
      .add("sr_net_loss",DoubleType(),True) 

date_dim_schema = StructType() \
      .add("d_date_sk",IntegerType(),True) \
      .add("d_date_id",StringType(),True) \
      .add("d_date",StringType(),True) \
      .add("d_month_seq",IntegerType(),True) \
      .add("d_week_seq",IntegerType(),True) \
      .add("d_quarter_seq",IntegerType(),True) \
      .add("d_year",IntegerType(),True) \
      .add("d_dow",IntegerType(),True) \
      .add("d_moy",IntegerType(),True) \
      .add("d_dom",IntegerType(),True) \
      .add("d_qoy",IntegerType(),True) \
      .add("d_fy_year",IntegerType(),True) \
      .add("d_fy_quarter_seq",IntegerType(),True) \
      .add("d_fy_week_seq",IntegerType(),True) \
      .add("d_day_name",StringType(),True) \
      .add("d_quarter_name",StringType(),True) \
      .add("d_holiday",StringType(),True) \
      .add("d_weekend",StringType(),True) \
      .add("d_following_holiday",StringType(),True) \
      .add("d_first_dom",IntegerType(),True) \
      .add("d_last_dom",IntegerType(),True) \
      .add("d_same_day_ly",IntegerType(),True) \
      .add("d_same_day_lq",IntegerType(),True) \
      .add("d_current_day",StringType(),True) \
      .add("d_current_week",StringType(),True) \
      .add("d_current_month",StringType(),True) \
      .add("d_current_quarter",StringType(),True) \
      .add("d_current_year",StringType(),True) 

store_schema = StructType() \
      .add("s_store_sk",IntegerType(),True) \
      .add("s_store_id",StringType(),True) \
      .add("s_rec_start_date",StringType(),True) \
      .add("s_rec_end_date",StringType(),True) \
      .add("s_closed_date_sk",StringType(),True) \
      .add("s_store_name",StringType(),True) \
      .add("s_number_employees",IntegerType(),True) \
      .add("s_floor_space",IntegerType(),True) \
      .add("s_hours",StringType(),True) \
      .add("S_manager",StringType(),True) \
      .add("S_market_id",IntegerType(),True) \
      .add("S_geography_class",StringType(),True) \
      .add("S_market_desc",StringType(),True) \
      .add("s_market_manager",StringType(),True) \
      .add("s_division_id",IntegerType(),True) \
      .add("s_division_name",StringType(),True) \
      .add("s_company_id",IntegerType(),True) \
      .add("s_company_name",StringType(),True) \
      .add("s_street_number",StringType(),True) \
      .add("s_street_name",StringType(),True) \
      .add("s_street_type",StringType(),True) \
      .add("s_suite_number",StringType(),True) \
      .add("s_city",StringType(),True) \
      .add("s_county",StringType(),True) \
      .add("s_state",StringType(),True) \
      .add("s_zip",StringType(),True) \
      .add("s_country",StringType(),True) \
      .add("s_gmt_offset",DoubleType(),True) \
      .add("s_tax_percentage",DoubleType(),True) 

customer_schema = StructType() \
      .add("c_customer_sk",IntegerType(),True) \
      .add("c_customer_id",StringType(),True) \
      .add("c_current_cdemo_sk",IntegerType(),True) \
      .add("c_current_hdemo_sk",IntegerType(),True) \
      .add("c_current_addr_sk",IntegerType(),True) \
      .add("c_first_shipto_date_sk",IntegerType(),True) \
      .add("c_first_sales_date_sk",IntegerType(),True) \
      .add("c_salutation",StringType(),True) \
      .add("c_first_name",StringType(),True) \
      .add("c_last_name",StringType(),True) \
      .add("c_preferred_cust_flag",StringType(),True) \
      .add("c_birth_day",IntegerType(),True) \
      .add("c_birth_month",IntegerType(),True) \
      .add("c_birth_year",IntegerType(),True) \
      .add("c_birth_country",StringType(),True) \
      .add("c_login",StringType(),True) \
      .add("c_email_address",StringType(),True) \
      .add("c_last_review_date_sk",IntegerType(),True)


df_store_returns = spark.read \
      .options(delimiter='|') \
      .schema(store_returns_schema) \
      .csv(store_returns_path)        
     
df_date_dim = spark.read \
      .options(delimiter='|') \
      .schema(date_dim_schema) \
      .csv(date_dim_path)   

df_store = spark.read \
      .options(delimiter='|') \
      .schema(store_schema) \
      .csv(store_path)        
     
df_customer = spark.read \
      .options(delimiter='|') \
      .schema(customer_schema) \
      .csv(customer_path)   



df_store_returns.createOrReplaceTempView("store_returns")
df_date_dim.createOrReplaceTempView("date_dim")
df_store.createOrReplaceTempView("store")
df_customer.createOrReplaceTempView("customer")

# 5. Run the selected query using spark.sql(“”) method , print some rows using show()
query_01 = spark.sql("""with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select  c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'NM'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100; """)



# 6. Rewrite the selected query using PySpark api only and run it
query_01.show()

# 7. Save the query results in a parquet file
query_01.write.mode("overwrite").parquet(r"C:\Users\user\Documents\AgileActorsSparkTeams\AgileActorsSparkTeam\output\query_01.parquet")