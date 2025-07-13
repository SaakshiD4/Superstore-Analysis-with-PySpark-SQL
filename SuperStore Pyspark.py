# Databricks notebook source
df = spark.table("default.superstore")
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

df_null=df.dropna()
display(df_null)

# COMMAND ----------

df.createOrReplaceTempView("sample")

# COMMAND ----------

# MAGIC %sql
# MAGIC select*from sample;

# COMMAND ----------

# DBTITLE 1,Total number of customer
# MAGIC %sql
# MAGIC select count(distinct customer_id) from sample;

# COMMAND ----------

df.show(5)

# COMMAND ----------

from pyspark.sql.functions import countDistinct

df_order = df.agg(countDistinct("Order_id").alias("unique_orders"))
df_order.show()


# COMMAND ----------

# ✅ 1. Total Sales and Profit by Category

from pyspark.sql.functions import sum, col

df_tot= df.groupBy("Category").agg(sum("sales").alias("total_Sales"),sum("Profit").alias("total_profit")).orderBy(col("total_Sales").desc())

df_tot.show()

# COMMAND ----------

df.count()


# COMMAND ----------

# ✅ 2. State with Highest Number of Orders
from pyspark.sql.functions import count
df_state=df.groupBy("state").agg(count("order_id").alias("total_orders")).orderBy(col("total_orders").desc())

df_state.show()

# COMMAND ----------

# DBTITLE 1,Find average discount given in each sub-category.  python Copy Edit
# Find average discount given in each sub-category.
from pyspark.sql.functions import avg
df_discount=df.groupBy("sub_category").agg(avg("discount"))
df_discount.show()

# COMMAND ----------

# ✅ 4. Top 5 Customers by Profit
# Identify top 5 customers who contributed maximum profit.

df_top_cust=df.select("customer_name","profit").groupBy("customer_name").agg(sum("profit").alias("total_profit")).orderBy(col("total_profit").desc()).limit(5)

df_top_cust.show()



# df.groupBy("Customer_Name") \
#   .agg(sum("Profit").alias("Total_Profit")) \
#   .orderBy("Total_Profit", ascending=False) \
#   .show(5)


# COMMAND ----------

# Number of Loss-Making Orders

from pyspark.sql.functions import col, count

df_loss = df.where(col("profit") < 0).select(count("order_id"))
df_loss.show()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView("store")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ✅ 1. Total Sales and Profit
# MAGIC SELECT SUM(profit) AS total_profit, SUM(sales) AS total_sales FROM store;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --  Sales by Region
# MAGIC
# MAGIC select region,sum(sales) as total_sales_per_region
# MAGIC from store group by region;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # Most Profitable Category
# MAGIC
# MAGIC select category, sum(profit) as max_profit
# MAGIC from store group by category order by max_profit desc limit 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # Top 5 Products by Sales
# MAGIC
# MAGIC select product_name,sum(sales) from store group by product_name order by sum(sales)
# MAGIC desc limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC --  Average Discount by Category
# MAGIC select avg(discount ) as avg_discount, category
# MAGIC from store group by category order by avg_discount desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- .Orders per Ship Mode
# MAGIC
# MAGIC select ship_mode ,count(order_id) as order_per_ship from store group by ship_mode 
# MAGIC order by order_per_ship desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Repeat Customers
# MAGIC -- Find customers who placed more than one order.
# MAGIC select customer_name, count(order_id) as orders
# MAGIC from store group by customer_name having orders >1 order by orders desc;
# MAGIC

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- finding sales rank per category and subcategory
# MAGIC
# MAGIC SELECT 
# MAGIC Category,Sub_Category,SUM(Sales) AS Total_Sales,
# MAGIC RANK() OVER (PARTITION BY Category ORDER BY SUM(Sales) DESC) AS Subcategory_Rank
# MAGIC FROM superstore
# MAGIC
# MAGIC GROUP BY Category, Sub_Category
# MAGIC
# MAGIC ORDER BY Category, Subcategory_Rank;
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

