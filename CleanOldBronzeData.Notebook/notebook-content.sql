-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "69118442-f38c-4f27-ac49-50e1498a489b",
-- META       "default_lakehouse_name": "New_Lake",
-- META       "default_lakehouse_workspace_id": "84782802-80aa-4b18-8e26-c94ddb42e9a6",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "69118442-f38c-4f27-ac49-50e1498a489b"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from datetime import datetime, timedelta
-- MAGIC 
-- MAGIC tables_to_clean = ["Bronze_FakeStore_Orders", "Bronze_FakeStore_Customers","bronze_fakestore_orderdetails", "Bronze_FakeStore_Products"]
-- MAGIC cutoff_date = datetime.now() - timedelta(days=30)
-- MAGIC 
-- MAGIC for table in tables_to_clean:
-- MAGIC     try:
-- MAGIC         df = spark.read.table(table)
-- MAGIC         before_count = df.count()
-- MAGIC 
-- MAGIC         df_filtered = df.filter(df["LoadDate"] >= cutoff_date)
-- MAGIC         after_count = df_filtered.count()
-- MAGIC 
-- MAGIC         df_filtered.write.format("delta").mode("overwrite").saveAsTable(table)
-- MAGIC 
-- MAGIC         print(f"{table}: {before_count} -> {after_count} rows (removed {before_count - after_count} old rows)")
-- MAGIC     except Exception as e:
-- MAGIC         print(f"Failed to clean {table}: {str(e)}")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC from pyspark.sql.functions import lit
-- MAGIC from datetime import date
-- MAGIC 
-- MAGIC today_date = date.today()
-- MAGIC 
-- MAGIC tables_to_update = ["Bronze_FakeStore_Orders", "Bronze_FakeStore_Customers", "Bronze_FakeStore_OrderDetails", "Bronze_FakeStore_Products"]
-- MAGIC 
-- MAGIC for table in tables_to_update:
-- MAGIC     try:
-- MAGIC         df = spark.read.table(table)
-- MAGIC         df_with_date = df.withColumn("LoadDate", lit(today_date))
-- MAGIC         df_with_date.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
-- MAGIC         print(f"{table}: LoadDate (today) added successfully! Rows: {df_with_date.count()}")
-- MAGIC     except Exception as e:
-- MAGIC         print(f"Failed to update {table}: {str(e)}")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }
