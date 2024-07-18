import pandas as pd
import sqlite3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, month, year

# Load the dataset
file_path = './Retail_Transaction_Dataset.csv'
df = pd.read_csv(file_path)

spark = SparkSession.builder.appName("RetailTransactionAnalysis").getOrCreate()

# Load the dataset into a Spark DataFrame
df_spark = spark.read.option("header", "true").csv(file_path, inferSchema=True)

# 1. Total Sales per Product Category
total_sales_per_category = df_spark.groupBy("ProductCategory").agg(sum("TotalAmount").alias("TotalSales"))
total_sales_per_category.show()

# 2. Total Sales per Store Location
total_sales_per_store = df_spark.groupBy("StoreLocation").agg(sum("TotalAmount").alias("TotalSales"))
total_sales_per_store.show()

# 3. Top 5 Customers by Total Spend
top_customers = df_spark.groupBy("CustomerID").agg(sum("TotalAmount").alias("TotalSpend")).orderBy(col("TotalSpend").desc()).limit(5)
top_customers.show()

# 4. Monthly Sales Trend
monthly_sales_trend = df_spark.withColumn("Month", month("TransactionDate")).withColumn("Year", year("TransactionDate")).groupBy("Year", "Month").agg(sum("TotalAmount").alias("TotalSales")).orderBy("Year", "Month")
monthly_sales_trend.show()

# 5. Payment Method Distribution
payment_method_distribution = df_spark.groupBy("PaymentMethod").count()
payment_method_distribution.show()

# 6. Average Discount by Product Category
average_discount_per_category = df_spark.groupBy("ProductCategory").agg(avg("DiscountApplied(%)").alias("AverageDiscount"))
average_discount_per_category.show()

# 7. Product Performance
product_performance = df_spark.groupBy("ProductID").agg(sum("Quantity").alias("TotalQuantitySold")).orderBy(col("TotalQuantitySold").desc())
product_performance.show()

# Pandas Part
# Perform the analysis using pandas

# 1. Total Sales per Product Category
total_sales_per_category_pandas = df.groupby("ProductCategory")["TotalAmount"].sum().reset_index()

# 2. Total Sales per Store Location
total_sales_per_store_pandas = df.groupby("StoreLocation")["TotalAmount"].sum().reset_index()

# 3. Top 5 Customers by Total Spend
top_customers_pandas = df.groupby("CustomerID")["TotalAmount"].sum().reset_index().sort_values(by="TotalAmount", ascending=False).head(5)

# 4. Monthly Sales Trend
df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])
df['Year'] = df['TransactionDate'].dt.year
df['Month'] = df['TransactionDate'].dt.month
monthly_sales_trend_pandas = df.groupby(["Year", "Month"])["TotalAmount"].sum().reset_index()

# 5. Payment Method Distribution
payment_method_distribution_pandas = df['PaymentMethod'].value_counts().reset_index()
payment_method_distribution_pandas.columns = ['PaymentMethod', 'Count']

# 6. Average Discount by Product Category
average_discount_per_category_pandas = df.groupby("ProductCategory")["DiscountApplied(%)"].mean().reset_index()

# 7. Product Performance
product_performance_pandas = df.groupby("ProductID")["Quantity"].sum().reset_index().sort_values(by="Quantity", ascending=False)

# Create SQLite database connection
conn = sqlite3.connect('./retail_analysis.db')

# Normalize and store each result set into separate tables
total_sales_per_category_pandas.to_sql('TotalSalesPerCategory', conn, if_exists='replace', index=False)
total_sales_per_store_pandas.to_sql('TotalSalesPerStore', conn, if_exists='replace', index=False)
top_customers_pandas.to_sql('TopCustomers', conn, if_exists='replace', index=False)
monthly_sales_trend_pandas.to_sql('MonthlySalesTrend', conn, if_exists='replace', index=False)
payment_method_distribution_pandas.to_sql('PaymentMethodDistribution', conn, if_exists='replace', index=False)
average_discount_per_category_pandas.to_sql('AverageDiscountPerCategory', conn, if_exists='replace', index=False)
product_performance_pandas.to_sql('ProductPerformance', conn, if_exists='replace', index=False)

# Close the database connection
conn.close()
