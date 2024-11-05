# Databricks notebook source
# MAGIC %md
# MAGIC ## Mounting

# COMMAND ----------

#set up the config for mounting the GCS bucket
gcs_bucket_name ='gcpbatchprocess'
mount_point='/mnt/dbgcb'
project_id='mentorsko-1723044071957'
service_account_key='/dbfs/FileStore/tables/mentorsko_1723044071957_9b99929f56b4.json'

#define the GCS service account credentials
config={
  'fs.gs.project.id': project_id,
  'fs.gs.auth.service.account.json.keyfile': service_account_key
}

#mount the GCS bucket
dbutils.fs.mount(
  source=f'gs://{gcs_bucket_name}',
  mount_point=mount_point,
  extra_configs=config
)

#display the contents of the mounted directory to verify
display(dbutils.fs.ls(mount_point))

# COMMAND ----------

orders_path = 'dbfs:/mnt/dbgcb/orders.csv'
orders_df = spark.read.option("header","true").option("inferSchema","true").csv(orders_path)
payment_path = 'dbfs:/mnt/dbgcb/payments.csv'
payment_df = spark.read.option("header","true").option("inferSchema","true").csv(payment_path)
payment_methods_path = 'dbfs:/mnt/dbgcb/payment_methods.csv'
payment_methods_df = spark.read.option("header","true").option("inferSchema","true").csv(payment_methods_path)
address_path = 'dbfs:/mnt/dbgcb/addresses.csv'
address_df = spark.read.option("header","true").option("inferSchema","true").csv(address_path)
customers_path = 'dbfs:/mnt/dbgcb/customers.csv'
customers_df = spark.read.option("header","true").option("inferSchema","true").csv(customers_path)
orders_items_path = 'dbfs:/mnt/dbgcb/orders_items.csv'
orders_items_df = spark.read.option("header","true").option("inferSchema","true").csv(orders_items_path)
products_path = 'dbfs:/mnt/dbgcb/products.csv'
products_df = spark.read.option("header","true").option("inferSchema","true").csv(products_path)
returns_path = 'dbfs:/mnt/dbgcb/returns.csv'
returns_df = spark.read.option("header","true").option("inferSchema","true").csv(returns_path)
shipping_tier_path = 'dbfs:/mnt/dbgcb/shipping_tier.csv'
shipping_tier_df = spark.read.option("header","true").option("inferSchema","true").csv(shipping_tier_path)
suppliers_path = 'dbfs:/mnt/dbgcb/suppliers.csv'
suppliers_df = spark.read.option("header","true").option("inferSchema","true").csv(suppliers_path)

# COMMAND ----------

orders_df.createOrReplaceTempView("orders")
products_df.createOrReplaceTempView("products")
orders_items_df.createOrReplaceTempView("order_items")

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte1 as (
# MAGIC select o.OrderID,
# MAGIC        o.ProductID,
# MAGIC        (o.quantity * float(p.actual_price)) as Price
# MAGIC from order_items o
# MAGIC join products p
# MAGIC on o.ProductID=p.Product_ID),
# MAGIC cte2 as (
# MAGIC select OrderID, sum(Price) as Order_amount
# MAGIC from cte1
# MAGIC group by OrderID),
# MAGIC cte3 as(
# MAGIC select c.OrderID,
# MAGIC        od.CustomerID,
# MAGIC        c.Order_amount
# MAGIC from cte2 c
# MAGIC join orders od
# MAGIC on c.OrderID=od.OrderID )
# MAGIC select CustomerID, sum(Order_amount) as final_spent
# MAGIC from cte3
# MAGIC group by CustomerID
# MAGIC order by final_spent desc
# MAGIC limit 10

# COMMAND ----------

shipping_tier_df.createOrReplaceTempView("shipping_tier")

# COMMAND ----------

display(shipping_tier_df)

# COMMAND ----------

from pyspark.sql import functions as F

merged_df = customers_df.join(orders_df, customers_df.CustomerID == orders_df.CustomerID, "inner") \
    .join(payment_df, orders_df.OrderID == payment_df.OrderID, "inner") \
    .join(shipping_tier_df, orders_df.ShippingTierID == shipping_tier_df.ShippingTierID, "inner")

merged_df = merged_df.drop(orders_df.CustomerID).drop(payment_df.OrderID).drop(orders_df.ShippingTierID)

display(merged_df)

# COMMAND ----------

merged_df.createOrReplaceTempView("merged_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC     ShippingTierID, 
# MAGIC     COUNT(*) AS OrderCount
# MAGIC FROM 
# MAGIC     merged_view
# MAGIC GROUP BY 
# MAGIC     ShippingTierID
# MAGIC ORDER BY 
# MAGIC     OrderCount DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte1 as (
# MAGIC select o.OrderID,
# MAGIC        p.Product_ID,
# MAGIC        o.Quantity,
# MAGIC        p.Actual_price,
# MAGIC        (o.Quantity * p.Actual_price) as sales,
# MAGIC        Od.OrderDate
# MAGIC from order_items o
# MAGIC join products p
# MAGIC on o.ProductID=p.Product_ID
# MAGIC join orders Od
# MAGIC on o.OrderID=Od.OrderID
# MAGIC order by 2,6)
# MAGIC select OrderID,
# MAGIC        Product_ID,
# MAGIC        sales,Quantity,Actual_price,
# MAGIC        sum(sales)over(partition by Product_ID order by OrderDate) as rolling_sum
# MAGIC from cte1      

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH order_dates AS (
# MAGIC     SELECT
# MAGIC         o.CustomerID,
# MAGIC         o.OrderDate,
# MAGIC         LEAD(o.OrderDate) OVER (PARTITION BY o.CustomerID ORDER BY o.OrderDate) AS next_order_date
# MAGIC     FROM
# MAGIC         orders o
# MAGIC )
# MAGIC SELECT
# MAGIC     c.CustomerID,
# MAGIC     COUNT(DISTINCT o.OrderID) AS tot_orders,
# MAGIC     COUNT(DISTINCT r.OrderId) AS tot_returns,
# MAGIC     SUM(CAST(REPLACE(REPLACE(p.Discounted_Price, '₹', ''), ',', '') AS INT) * oi.Quantity) AS order_value,
# MAGIC     FLOOR(SUM(oi.Quantity) / COUNT(DISTINCT o.OrderID)) AS avg_basket_size,
# MAGIC     round(SUM(CAST(REPLACE(REPLACE(p.Discounted_Price, '₹', ''), ',', '') AS INT) * oi.Quantity) / COUNT(DISTINCT o.OrderID),2) AS avg_basket_value,
# MAGIC     DATEDIFF(MAX(o.OrderDate), MIN(o.OrderDate)) AS length_of_stay_days,
# MAGIC     ROUND(AVG(DATEDIFF(od.next_order_date, o.OrderDate))) AS order_purchase_frequency
# MAGIC FROM
# MAGIC     customers c
# MAGIC JOIN
# MAGIC     orders o ON c.CustomerID = o.CustomerID
# MAGIC LEFT JOIN
# MAGIC     returns r ON o.OrderID = r.OrderID
# MAGIC JOIN
# MAGIC     order_items oi ON o.OrderID = oi.OrderID
# MAGIC JOIN
# MAGIC     products p ON p.Product_ID = oi.ProductID
# MAGIC JOIN
# MAGIC     order_dates od ON o.CustomerID = od.CustomerID AND o.OrderDate = od.OrderDate
# MAGIC GROUP BY
# MAGIC     c.CustomerID

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW supplier_product_count AS
# MAGIC SELECT
# MAGIC     s.SupplierID,
# MAGIC     COUNT(p.Product_ID) AS NumberOfProducts
# MAGIC FROM
# MAGIC     suppliers s
# MAGIC JOIN
# MAGIC     orders o ON o.SupplierID = s.SupplierID
# MAGIC JOIN
# MAGIC     order_items oi ON oi.OrderID = o.OrderID
# MAGIC JOIN
# MAGIC     products p ON oi.ProductID = p.Product_ID
# MAGIC GROUP BY
# MAGIC     s.SupplierID

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3

# COMMAND ----------

avg_ratings_df = products_df.withColumn("Product_Rating", F.col("Product_Rating").cast("float")) \
    .groupBy("Product_ID") \
    .agg(F.round(F.avg("Product_Rating"), 1).alias("AvgRating"))

highly_rated_products_df = avg_ratings_df.filter(F.col("AvgRating") >= 4.5)

display(highly_rated_products_df)

# COMMAND ----------

from pyspark.sql.functions import col, floor

dated = orders_df.withColumn("datediff", floor((col("ShippingDate").cast("long") - col("OrderDate").cast("long")) / (24 * 60 * 60)))
display(dated)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, year, month, sum, lag
from pyspark.sql.window import Window

cte1 = (
    orders_items_df
    .join(products_df, orders_items_df.ProductID == products_df.Product_ID)
    .withColumn("sales", col("Quantity") * regexp_replace(col("Discounted_Price"), '[₹,]', '').cast("float"))
    .select("OrderID", orders_items_df.ProductID, "sales")
)

cte2 = (
    cte1
    .join(orders_df, "OrderID")
    .withColumn("year", year("OrderDate"))
    .withColumn("month", month("OrderDate"))
    .select("year", "month", "sales")
    .groupBy("year", "month")
    .agg(sum("sales").alias("sales"))
    .orderBy("year", "month")
)

cte3 = (
    cte2
    .withColumn("prevMonthSales", lag("sales").over(Window.partitionBy("year").orderBy("month")))
    .withColumn("mom", 100 * (col("sales") - col("prevMonthSales")) / col("prevMonthSales"))
)

display(cte3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, sum, avg, count, expr

# Calculate total orders, total units sold, total revenue, and average price
product_performance = (
    orders_items_df
    .join(products_df, orders_items_df.ProductID == products_df.Product_ID)
    .withColumn("revenue", col("Quantity") * regexp_replace(col("Discounted_Price"), '[₹,]', '').cast("float"))
    .groupBy("ProductID", "Product_Name")
    .agg(
        count("OrderID").alias("Total_Orders"),
        sum("Quantity").alias("Total_Units_Sold"),
        sum("revenue").alias("Total_Revenue"),
        avg(regexp_replace(col("Discounted_Price"), '[₹,]', '').cast("float")).alias("Avg_Price")
    )
)

# Calculate total returns and return rate
returns = (
    returns_df
    .groupBy("OrderId")
    .agg(
        count("OrderId").alias("Total_Returns")
    )
)


# COMMAND ----------

# Merge returns with orders_items_df on OrderID
orders_items_with_returns = (
    orders_items_df
    .join(returns, "OrderID", "left")
    .withColumn("Total_Returns", col("Total_Returns").cast("int"))
)
display(orders_items_with_returns)

# COMMAND ----------

# Join the performance and returns dataframes
product_analysis_report = (
    product_performance
    .join(orders_items_with_returns, "ProductID", "left")
    .withColumn("Total_Returns", col("Total_Returns").cast("int"))
    .withColumn("Return_rate", (col("Total_Returns") / col("Total_Orders")) * 100)
    .select("ProductID", "Product_Name", "Total_Orders", "Total_Units_Sold", "Total_Revenue", "Avg_Price", "Total_Returns", "Return_rate")
)

display(product_analysis_report)

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, sum, when, lit

cte = (
    products_df
    .join(orders_items_df, orders_items_df.ProductID == products_df.Product_ID)
    .join(orders_df, "OrderID")
    .withColumn("sales", col("Quantity") * regexp_replace(col("Discounted_Price"), '[₹,]', '').cast("float"))
    .select("CustomerID", "sales")
    .groupBy("CustomerID")
    .agg(sum("sales").alias("total_spent"))
)

classify_customers = (
    cte
    .withColumn("type", when(col("total_spent") > 1000, lit("Platinum"))
                .otherwise(when(col("total_spent") < 500, lit("Silver"))
                .otherwise(lit("Gold"))))
)

display(classify_customers)
