# Databricks notebook source
# DBTITLE 1,FILE LOACTION OF BOTH THE TABLES SALES AND MENU
/FileStore/tables/sales_csv-1.txt

/FileStore/tables/menu_csv-1.txt

# COMMAND ----------

# DBTITLE 1,CREATING SALES DATAFRAME
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

schema = StructType([
    StructField("product_id",IntegerType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("location",StringType(),True),
    StructField("source_order",StringType(),True)
])

# reading the csv file 

sales_df = spark.read.format("csv").option("inferschema","True").schema(schema).load("/FileStore/tables/sales_csv-1.txt")

# display the csv file 
display(sales_df)

# COMMAND ----------

# DBTITLE 1,DERIVING THREE NEW COLUMN FOR YEAR , MONTH , QUARTER
from pyspark.sql.functions import month,quarter,year

sales_df = sales_df.withColumn("order_year",year(sales_df.order_date))
display(sales_df)

# COMMAND ----------

from pyspark.sql.functions import month,quarter,year

sales_df = sales_df.withColumn("order_month",month(sales_df.order_date))
sales_df = sales_df.withColumn("order_quarter",quarter(sales_df.order_date))
display(sales_df)

# COMMAND ----------

from pyspark.sql.functions import month,quarter,year

sales_df = sales_df.withColumn("order_month",month(sales_df.order_date))
sales_df = sales_df.withColumn("order_quarter",quarter(sales_df.order_date))
display(sales_df)

# COMMAND ----------

# DBTITLE 1,CREATING MENU DATAFRAME
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

schema = StructType([
    StructField("product_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("price",StringType(),True)
])

menu_df = spark.read.format("csv").option("inferschema","True").schema(schema).load("/FileStore/tables/menu_csv-1.txt")
display(menu_df)

# COMMAND ----------

# DBTITLE 1,Total amount spent by each customer
total_amount_spent = (sales_df.join(menu_df,"product_id").groupBy("customer_id").agg({"price":"sum"}).orderBy("customer_id"))

display(total_amount_spent)

# COMMAND ----------

# DBTITLE 1,Total amount spent by each food category
total_amount = (sales_df.join(menu_df,"product_id").groupBy("product_name").agg({"price":"sum"}).orderBy("product_name"))
display(total_amount)

# COMMAND ----------

# DBTITLE 1,Total amount of sales in each month 
total_sales_monthly= (sales_df.join(menu_df,"product_id").groupBy("order_month").agg({"price":"sum"}).orderBy("order_month"))
display(total_sales_monthly)

# COMMAND ----------

# DBTITLE 1,Yearly sales 
total_sales= (sales_df.join(menu_df,"product_id").groupBy("order_year").agg({"price":"sum"}).orderBy("order_year"))
display(total_sales)

# COMMAND ----------

# DBTITLE 1,quarterly sales
total_sales= (sales_df.join(menu_df,"product_id").groupBy("order_quarter").agg({"price":"sum"}).orderBy("order_quarter"))
display(total_sales)

# COMMAND ----------

# DBTITLE 1,How many times each product purchased
from pyspark.sql.functions import count

product_count= (sales_df.join(menu_df,"product_id").groupBy("product_id","product_name").agg(count("product_id").alias("product_count")).orderBy("product_count",ascending =0))
                
display(product_count)

# COMMAND ----------

# DBTITLE 1,Top 5 ordered item 
from pyspark.sql.functions import count

product_count= (sales_df.join(menu_df,"product_id").groupBy("product_id","product_name").agg(count("product_id").alias("product_count")).orderBy("product_count",ascending=0)).drop("product_id").limit(5)
                
display(product_count)

# COMMAND ----------

# DBTITLE 1,Top ordered item 
from pyspark.sql.functions import count

product_count= (sales_df.join(menu_df,"product_id").groupBy("product_id","product_name").agg(count("product_id").alias("product_count")).orderBy("product_count",ascending=0)).limit(1)
                
display(product_count)

# COMMAND ----------

# DBTITLE 1,Total sales by each country
total_sales= (sales_df.join(menu_df,"product_id").groupBy("location").agg({"price":"sum"}))
                
display(total_sales)

# COMMAND ----------

# DBTITLE 1,Total sales by order source
total_sales= (sales_df.join(menu_df,"product_id").groupBy("source_order").agg({"price":"sum"}))
                
display(total_sales)
