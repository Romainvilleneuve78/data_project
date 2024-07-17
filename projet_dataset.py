from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, avg
import sqlite3

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("Transaction Analysis") \
    .getOrCreate()

# Charger le fichier CSV dans un DataFrame Spark en spécifiant l'option de délimitation
file_path = r'C:\efrei cour\M1\Semestre_8\data\projet\Normalized_Transaction_Dataset.csv'
df_spark = spark.read.csv(file_path, header=True, inferSchema=True, multiLine=True, escape='"')

# Convertir les colonnes nécessaires en types appropriés
df_spark = df_spark.withColumn("Quantity", col("Quantity").cast("double"))
df_spark = df_spark.withColumn("Price", col("Price").cast("double"))
df_spark = df_spark.withColumn("DiscountApplied(%)", col("DiscountApplied(%)").cast("double"))
df_spark = df_spark.withColumn("TotalAmount", col("TotalAmount").cast("double"))

# Fonction pour stocker un DataFrame Spark dans une table SQLite
def store_in_sqlite(df, table_name, db_name='results.db'):
    # Convertir DataFrame Spark en DataFrame Pandas
    df_pandas = df.toPandas()
    
    # Connexion à la base de données SQLite
    conn = sqlite3.connect(db_name)
    
    # Stocker les données dans une table SQLite
    df_pandas.to_sql(table_name, conn, index=False, if_exists='replace')
    
    # Fermer la connexion
    conn.close()

# Requêtes complexes et stockage des résultats dans SQLite

# Total des ventes par catégorie de produit et méthode de paiement
total_sales_by_category_payment = df_spark.groupBy("ProductCategory", "PaymentMethod").sum("TotalAmount")
total_sales_by_category_payment.show()
store_in_sqlite(total_sales_by_category_payment, 'total_sales_by_category_payment')

# Nombre de clients uniques par magasin
unique_customers_by_store = df_spark.groupBy("StoreLocation").agg(countDistinct("CustomerID").alias("UniqueCustomers"))
unique_customers_by_store.show()
store_in_sqlite(unique_customers_by_store, 'unique_customers_by_store')

# Montant moyen des ventes par client
average_sales_by_customer = df_spark.groupBy("CustomerID").avg("TotalAmount")
average_sales_by_customer.show()
store_in_sqlite(average_sales_by_customer, 'average_sales_by_customer')

# Top 3 des magasins avec le plus de transactions en termes de quantité
top_stores_by_quantity = df_spark.groupBy("StoreLocation").sum("Quantity").orderBy(col("sum(Quantity)").desc()).limit(3)
top_stores_by_quantity.show()
store_in_sqlite(top_stores_by_quantity, 'top_stores_by_quantity')

# Montant total des ventes par mois et méthode de paiement
df_spark = df_spark.withColumn("YearMonth", df_spark["TransactionDate"].substr(1, 7))
total_sales_by_month_payment = df_spark.groupBy("YearMonth", "PaymentMethod").sum("TotalAmount")
total_sales_by_month_payment.show()
store_in_sqlite(total_sales_by_month_payment, 'total_sales_by_month_payment')

# Durée moyenne des transactions par client (utilisation de timestampdiff si applicable)
# Note: Assuming there's a column for transaction end time, which is not present in the given data. 
# If there's a column like "TransactionEndTime", we can use it.
# average_transaction_duration = df_spark.withColumn("TransactionDuration", 
#    unix_timestamp("TransactionEndTime") - unix_timestamp("TransactionDate")).groupBy("CustomerID").avg("TransactionDuration")
# average_transaction_duration.show()
# store_in_sqlite(average_transaction_duration, 'average_transaction_duration')

# Distribution des remises appliquées par catégorie de produit
discount_distribution_by_category = df_spark.groupBy("ProductCategory").avg("DiscountApplied(%)")
discount_distribution_by_category.show()
store_in_sqlite(discount_distribution_by_category, 'discount_distribution_by_category')

# Fermer la session Spark
spark.stop()
