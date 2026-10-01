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

df_customers = spark.read.table("silver_customers")
print("silver_customers:")
df_customers


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("--- 1. Building Customer Dimension ---")
df_customers = spark.read.table("silver_customers")
customer_dim = df_customers.select(
    "CustomerID", "CompanyName", "ContactName", "ContactTitle", 
    "Address", "City", "Region", "PostalCode", "Country", "Phone"
).dropDuplicates(["CustomerID"])
customer_dim = customer_dim.withColumn("CustomerKey", F.row_number().over(Window.orderBy("CustomerID")))
customer_dim = customer_dim.select("CustomerKey", "CustomerID", "CompanyName", "ContactName", "ContactTitle", "Address", "City", "Region", "PostalCode", "Country", "Phone")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- 2. Building Product Dimension ---")
df_products = spark.read.table("silver_products")
product_dim = df_products.select(
    "ProductID", "ProductName", "SupplierID", "CategoryID", 
    "QuantityPerUnit", "UnitPrice", "UnitsInStock"
).dropDuplicates(["ProductID"])
product_dim = product_dim.withColumn("ProductKey", F.row_number().over(Window.orderBy("ProductID")))
product_dim = product_dim.select("ProductKey", "ProductID", "ProductName", "SupplierID", "CategoryID", "QuantityPerUnit", "UnitPrice", "UnitsInStock")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- 3. Building Employee Dimension ---")
df_emp = spark.read.table("silver_employees")
employee_dim = df_emp.select(
    "EmployeeID", "LastName", "FirstName", "Title", "City", "Country"
).dropDuplicates(["EmployeeID"])
employee_dim = employee_dim.withColumn("EmployeeKey", F.row_number().over(Window.orderBy("EmployeeID")))
employee_dim = employee_dim.select("EmployeeKey", "EmployeeID", "LastName", "FirstName", "Title", "City", "Country")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- 4. Building Shipper Dimension ---")
df_ship = spark.read.table("silver_shippers")
shipper_dim = df_ship.select(
    "ShipperID", "CompanyName", "Phone"
).dropDuplicates(["ShipperID"])
shipper_dim = shipper_dim.withColumn("ShipperKey", F.row_number().over(Window.orderBy("ShipperID")))
shipper_dim = shipper_dim.select("ShipperKey", "ShipperID", "CompanyName", "Phone")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- 5. Building Supplier Dimension ---")
df_sup = spark.read.table("silver_suppliers")
supplier_dim = df_sup.select(
    "SupplierID", "CompanyName", "ContactName", "City", "Country"
).dropDuplicates(["SupplierID"])
supplier_dim = supplier_dim.withColumn("SupplierKey", F.row_number().over(Window.orderBy("SupplierID")))
supplier_dim = supplier_dim.select("SupplierKey", "SupplierID", "CompanyName", "ContactName", "City", "Country")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- 6. Building Territory Dimension ---")
df_terr = spark.read.table("silver_territories")
territory_dim = df_terr.select(
    "TerritoryID", "TerritoryDescription", "RegionID"
).dropDuplicates(["TerritoryID"])
territory_dim = territory_dim.withColumn("TerritoryKey", F.row_number().over(Window.orderBy("TerritoryID")))
territory_dim = territory_dim.select("TerritoryKey", "TerritoryID", "TerritoryDescription", "RegionID")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- 7. Building Date Dimension ---")
df_orders = spark.read.table("silver_orders")
date_bounds = df_orders.select(
    F.min(F.to_date("OrderDate")).alias("min_date"),
    F.max(F.to_date("OrderDate")).alias("max_date")
).collect()[0]

date_df = spark.sql(f"""
    SELECT EXPLODE(sequence(to_date('{date_bounds["min_date"]}'), to_date('{date_bounds["max_date"]}'), interval 1 day)) as FullDate
""")

dim_date = date_df.select(
    F.date_format("FullDate", "yyyyMMdd").cast("long").alias("DateKey"),
    F.col("FullDate"),
    F.year("FullDate").alias("Year"),
    F.month("FullDate").alias("Month"),
    F.dayofmonth("FullDate").alias("Day"),
    F.date_format("FullDate", "MMMM").alias("MonthName"),
    F.dayofweek("FullDate").alias("DayOfWeek"),
    F.when(F.dayofweek("FullDate").isin([1, 7]), True).otherwise(False).alias("IsWeekend")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- 8. Building Fact Table (fact_orders) ---")

# 1. read from silver 
df_orders = spark.read.table("silver_orders")
df_order_details = spark.read.table("silver_order_details")
df_products = spark.read.table("silver_products")

# 2. fact_df)
fact_df = df_orders.join(df_order_details, on="OrderID", how="inner")

# 3. 
fact_df = fact_df.join(df_products.select("ProductID", "SupplierID"), on="ProductID", how="left")

# 4. ( fact_df)
fact_df = fact_df.join(customer_dim.select("CustomerID", "CustomerKey"), on="CustomerID", how="left")
fact_df = fact_df.join(product_dim.select("ProductID", "ProductKey"), on="ProductID", how="left")
fact_df = fact_df.join(employee_dim.select("EmployeeID", "EmployeeKey"), on="EmployeeID", how="left")
fact_df = fact_df.join(shipper_dim.select("ShipperID", "ShipperKey"), fact_df["ShipVia"] == shipper_dim["ShipperID"], how="left")
fact_df = fact_df.join(supplier_dim.select("SupplierID", "SupplierKey"), on="SupplierID", how="left")

# 5.  (Territory)
fact_df = fact_df.join(
    territory_dim.select("TerritoryKey", "TerritoryID", "TerritoryDescription"), 
    fact_df["ShipCity"] == territory_dim["TerritoryDescription"], 
    how="left"
)

# 6. 
fact_orders = fact_df.select(
    "OrderID",
    "CustomerKey",
    "ProductKey",
    "EmployeeKey",
    "ShipperKey",
    "SupplierKey",
    "TerritoryKey",
    "TerritoryID",
    F.date_format(F.col("OrderDate"), "yyyyMMdd").cast("long").alias("OrderDateKey"),
    F.year(F.to_date("OrderDate")).alias("Year"),
    F.month(F.to_date("OrderDate")).alias("Month"),
    F.dayofmonth(F.to_date("OrderDate")).alias("Day"),
    "Quantity",
    "UnitPrice",
    "Discount",
    (F.col("Quantity") * F.col("UnitPrice") * (1 - F.col("Discount"))).alias("ExtendedPrice"),
    "Freight",
    "ShipCountry",
    "ShipCity"
)

fact_orders.write.format("delta").mode("overwrite").saveAsTable("gold_fact_orders")
print("Fact table built and saved successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("--- 9. Saving All Tables to Delta Lake (Golden Layer) ---")
customer_dim.write.format("delta").mode("overwrite").saveAsTable("gold_customer_dim")
product_dim.write.format("delta").mode("overwrite").saveAsTable("gold_product_dim")
employee_dim.write.format("delta").mode("overwrite").saveAsTable("gold_employee_dim")
shipper_dim.write.format("delta").mode("overwrite").saveAsTable("gold_shipper_dim")
supplier_dim.write.format("delta").mode("overwrite").saveAsTable("gold_supplier_dim")
territory_dim.write.format("delta").mode("overwrite").saveAsTable("gold_territory_dim")
dim_date.write.format("delta").mode("overwrite").saveAsTable("gold_dim_date")
fact_orders.write.format("delta").mode("overwrite").saveAsTable("gold_fact_orders")

print(" All Golden Layer tables have been successfully created and saved!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("gold_fact_orders")
df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# tables_to_drop = [
#     "gold_customer_dim",
#     "gold_product_dim",
#     "gold_employee_dim",
#     "gold_shipper_dim",
#     "gold_supplier_dim",
#     "gold_territory_dim",
#     "gold_dim_date",
#     "gold_fact_orders"
# ]

# #  (Drop Table with Purge)
# for table in tables_to_drop:
#     spark.sql(f"DROP TABLE IF EXISTS {table}")
#     print(f"Dropped table: {table}")

# print("✨ All old golden tables have been successfully dropped!")# 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
