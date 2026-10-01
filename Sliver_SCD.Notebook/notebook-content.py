# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "69118442-f38c-4f27-ac49-50e1498a489b",
# META       "default_lakehouse_name": "New_Lake",
# META       "default_lakehouse_workspace_id": "84782802-80aa-4b18-8e26-c94ddb42e9a6",
# META       "known_lakehouses": [
# META         {
# META           "id": "69118442-f38c-4f27-ac49-50e1498a489b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd
file_path = "/lakehouse/default/Files/FakeStore_Orders_OrderDetails_Customers_Products_Linked.xlsx"
excel_file = pd.ExcelFile(file_path)
print(excel_file.sheet_names)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#
df_orders = pd.read_excel(file_path, sheet_name="Orders")
df_orderdetails = pd.read_excel(file_path, sheet_name="OrderDetails")
df_customers = pd.read_excel(file_path, sheet_name="Customers")
df_products = pd.read_excel(file_path, sheet_name="Products")

print("Orders:", df_orders.shape)
print("OrderDetails:", df_orderdetails.shape)
print("Customers:", df_customers.shape)
print("Products:", df_products.shape)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 
spark_orders = spark.createDataFrame(df_orders)
spark_orderdetails = spark.createDataFrame(df_orderdetails)
spark_customers = spark.createDataFrame(df_customers)
spark_products = spark.createDataFrame(df_products)

# 
spark_orders.write.mode("overwrite").saveAsTable("Bronze_FakeStore_Orders")
spark_orderdetails.write.mode("overwrite").saveAsTable("Bronze_FakeStore_OrderDetails")
spark_customers.write.mode("overwrite").saveAsTable("Bronze_FakeStore_Customers")
spark_products.write.mode("overwrite").saveAsTable("Bronze_FakeStore_Products")

print("DONE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

clean_orders = spark.read.table("Silver_Orders")
print("أعمدة Silver_Orders:")
print(clean_orders.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# check = spark.read.table("Silver_Orders")
# check.filter(check.OrderID == 10248).select("OrderID", "CustomerID", "Freight", "LoadDate", "IsCurrent").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, concat_ws, sha2, when, current_timestamp

def apply_scd_type2(bronze_table: str, silver_table: str, key_cols: list, exclude_cols: list = None):
    """
    SCD Type 2 Dynamic باستخدام Row Hash
    """
    exclude_cols = set(exclude_cols or []) | {"RowHash", "IsCurrent", "LoadDate"}

    # نعمل Refresh للجداول  عشان نتجنب مشاكل الـ Schema Cache
    spark.catalog.refreshTable(bronze_table)
    if spark.catalog.tableExists(silver_table):
        spark.catalog.refreshTable(silver_table)

    bronze_df = spark.read.table(bronze_table)

    hash_cols = [c for c in bronze_df.columns if c not in exclude_cols]
    bronze_hashed = bronze_df.withColumn(
        "RowHash", sha2(concat_ws("||", *[col(c).cast("string") for c in hash_cols]), 256)
    )

    # --- Initial Load ---
    if not spark.catalog.tableExists(silver_table):
        initial_df = (bronze_hashed
                      .withColumn("IsCurrent", lit(True))
                      .withColumn("LoadDate", current_timestamp()))
        initial_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
        spark.catalog.refreshTable(silver_table)
        print(f"✅ Initial Load - {silver_table}: {initial_df.count()} rows")
        return

    silver_df = spark.read.table(silver_table)

    # --- Backfill لو RowHash/IsCurrent/LoadDate مش موجودين ---
    if "RowHash" not in silver_df.columns:
        silver_hash_cols = [c for c in hash_cols if c in silver_df.columns]
        silver_df = silver_df.withColumn(
            "RowHash", sha2(concat_ws("||", *[col(c).cast("string") for c in silver_hash_cols]), 256)
        )
    if "IsCurrent" not in silver_df.columns:
        silver_df = silver_df.withColumn("IsCurrent", lit(True))
    if "LoadDate" not in silver_df.columns:
        silver_df = silver_df.withColumn("LoadDate", current_timestamp())

    silver_current = silver_df.filter(col("IsCurrent") == True).select(*key_cols, col("RowHash").alias("Silver_RowHash"))

    compare = bronze_hashed.join(silver_current, key_cols, "left")

    new_or_changed = (compare
                       .filter(col("Silver_RowHash").isNull() | (col("RowHash") != col("Silver_RowHash")))
                       .drop("Silver_RowHash")
                       .withColumn("IsCurrent", lit(True))
                       .withColumn("LoadDate", current_timestamp()))

    keys_to_close = (compare
                      .filter(col("Silver_RowHash").isNotNull() & (col("RowHash") != col("Silver_RowHash")))
                      .select(*key_cols)
                      .withColumn("_close", lit(True)))

    silver_updated = (silver_df
                       .join(keys_to_close, key_cols, "left")
                       .withColumn("IsCurrent",
                                   when((col("IsCurrent") == True) & (col("_close") == True), lit(False))
                                   .otherwise(col("IsCurrent")))
                       .drop("_close"))

    final_df = silver_updated.unionByName(new_or_changed, allowMissingColumns=True)

    #  write in temp table
    temp_table = f"{silver_table}_tmp_write"
    final_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(temp_table)

    final_written = spark.read.table(temp_table)
    final_written.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)
    spark.sql(f"DROP TABLE IF EXISTS {temp_table}")

    spark.catalog.refreshTable(silver_table)

    result = spark.read.table(silver_table)
    print(f"✅ {silver_table} -> Total: {result.count()} | Current: {result.filter(col('IsCurrent')==True).count()} | Old: {result.filter(col('IsCurrent')==False).count()}")


apply_scd_type2("Bronze_FakeStore_Orders",       "Silver_Orders",        key_cols=["OrderID"])
apply_scd_type2("Bronze_FakeStore_Customers",    "Silver_Customers",     key_cols=["CustomerID"])
apply_scd_type2("Bronze_FakeStore_Products",     "Silver_Products",      key_cols=["ProductID"])
apply_scd_type2("Bronze_FakeStore_OrderDetails", "Silver_Order_Details", key_cols=["OrderID", "ProductID"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

check = spark.read.table("Silver_Orders")
check.filter(check.OrderID == 10248).select("OrderID", "CustomerID", "Freight", "LoadDate", "IsCurrent").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
