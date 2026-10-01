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

import requests
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

base_url = "https://services.odata.org/V4/Northwind/Northwind.svc/"
url = base_url + "Order_Details_Extendeds"
all_data = []

while url:
    response = requests.get(url, headers={"Accept": "application/json"})
    result = response.json()
    all_data.extend(result["value"])
    
    next_link = result.get("@odata.nextLink")
    if next_link and not next_link.startswith("http"):
        next_link = base_url + next_link
    url = next_link

print("Total rows fetched:", len(all_data))

schema = StructType([
    StructField("OrderID", LongType(), True),
    StructField("ProductID", LongType(), True),
    StructField("ProductName", StringType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("Quantity", LongType(), True),
    StructField("Discount", DoubleType(), True),
    StructField("ExtendedPrice", DoubleType(), True),
])

rows = [(d["OrderID"], d["ProductID"], d["ProductName"], float(d["UnitPrice"]), 
          d["Quantity"], float(d["Discount"]), float(d["ExtendedPrice"])) for d in all_data]

df = spark.createDataFrame(rows, schema)
print("Final row count:", df.count())

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("Order_Details_Extendeds")
print("Table written successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# - Loops through 26 tables (the original ones )
# - Removes duplicate rows (rows fully identical to another row)
# - Saves each table as Silver
tables = ["Categories", "CustomerDemographics", "Customers", "Employees",
          "Order_Details", "Orders", "Products", "Regions", 
          "Shippers", "Suppliers", "Territories",
          "Alphabetical_list_of_products", "Category_Sales_for_1997",
          "Current_Product_Lists", "Customer_and_Suppliers_by_Cities",
          "Invoices", "Order_Details_Extendeds", "Order_Subtotals",
          "Orders_Qries", "Product_Sales_for_1997", "Products_Above_Average_Prices",
          "Products_by_Categories", "Sales_by_Categories", "Sales_Totals_by_Amounts",
          "Summary_of_Sales_by_Quarters", "Summary_of_Sales_by_Years"]

def clean_table(table_name):
    df = spark.read.table(table_name)
    
  
    cols_to_drop = [c for c in df.columns if "@odata" in c]
    df = df.drop(*cols_to_drop)
    
   
    for c in df.columns:
        if c.startswith("value."):
            new_name = c.replace("value.", "", 1)
            df = df.withColumnRenamed(c, new_name)
    
    before = df.count()
    df_clean = df.dropDuplicates()
    after = df_clean.count()
    
    print(f"{table_name}: {before} rows -> {after} rows (removed {before-after} duplicates)")
    
    df_clean.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"Silver_{table_name}")
    return df_clean

for t in tables:
    try:
        clean_table(t)
    except Exception as e:
        print(f" {t}: FAILED - {str(e)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This code checks all 26 Silver tables and verifies they're properly cleaned:
# - For each table, it checks if any "@odata" metadata columns are still present
# - It also checks if any column names still have the "value." prefix
# - Prints ✅ if the table is clean, or ⚠️ if it still has leftover columns to fix

# tables = ["Categories", "CustomerDemographics", "Customers", "Employees",
#           "Order_Details", "Orders", "Products", "Regions", 
#           "Shippers", "Suppliers", "Territories",
#           "Alphabetical_list_of_products", "Category_Sales_for_1997",
#           "Current_Product_Lists", "Customer_and_Suppliers_by_Cities",
#           "Invoices", "Order_Details_Extendeds", "Order_Subtotals",
#           "Orders_Qries", "Product_Sales_for_1997", "Products_Above_Average_Prices",
#           "Products_by_Categories", "Sales_by_Categories", "Sales_Totals_by_Amounts",
#           "Summary_of_Sales_by_Quarters", "Summary_of_Sales_by_Years"]

for t in tables:
    df = spark.read.table(f"Silver_{t}")
    has_odata = any("@odata" in c for c in df.columns)
    has_value_prefix = any(c.startswith("value.") for c in df.columns)
    status = "✅clean" if not has_odata and not has_value_prefix else "⚠️ still we have A Problem"
    print(f"{t}: {status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This checks all 26 Silver tables for missing (null) values, column by column.
# For each table, it counts how many nulls exist in every column and prints the result.
from pyspark.sql.functions import col, sum as spark_sum, when, lit

# tables = ["Categories", "CustomerDemographics", "Customers", "Employees",
#           "Order_Details", "Orders", "Products", "Regions", 
#           "Shippers", "Suppliers", "Territories",
#           "Alphabetical_list_of_products", "Category_Sales_for_1997",
#           "Current_Product_Lists", "Customer_and_Suppliers_by_Cities",
#           "Invoices", "Order_Details_Extendeds", "Order_Subtotals",
#           "Orders_Qries", "Product_Sales_for_1997", "Products_Above_Average_Prices",
#           "Products_by_Categories", "Sales_by_Categories", "Sales_Totals_by_Amounts",
#           "Summary_of_Sales_by_Quarters", "Summary_of_Sales_by_Years"]

results = []

for t in tables:
    df = spark.read.table(f"Silver_{t}")
    for c in df.columns:
        null_count = df.filter(col(c).isNull()).count()
        if null_count > 0:
            results.append((t, c, null_count))

#
summary_df = spark.createDataFrame(results, ["Table", "Column", "NullCount"])
summary_df.orderBy("Table", "Column").show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for negative values in key numeric columns across several important tables
# Loops through each table, checks the specified columns for negative values,
# and prints a summary table showing only the columns where negative values were found

from pyspark.sql.functions import col

checks = {
    "Order_Details": ["UnitPrice", "Quantity", "Discount"],
    "Order_Details_Extendeds": ["UnitPrice", "Quantity", "Discount", "ExtendedPrice"],
    "Order_Subtotals": ["Subtotal"],
    "Products": ["UnitPrice", "UnitsInStock"],
    "Invoices": ["UnitPrice", "Quantity", "Discount", "ExtendedPrice", "Freight"]
}

results = []

for table, columns in checks.items():
    df = spark.read.table(f"Silver_{table}")
    for c in columns:
        if c in df.columns:
            negative_count = df.filter(col(c) < 0).count()
            if negative_count > 0:
                results.append((table, c, negative_count))

if results:
    summary_df = spark.createDataFrame(results, ["Table", "Column", "NegativeCount"])
    summary_df.show(truncate=False)
else:
    print("No negative values found in any checked table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if any orders have a shipped date earlier than their order date


df = spark.read.table("Silver_Orders")

invalid_dates = df.filter(df.ShippedDate < df.OrderDate)
print("Orders with ShippedDate before OrderDate:", invalid_dates.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import trim
# Trim extra whitespace from all string (text) columns
for field in df.schema.fields:
    if str(field.dataType) == "StringType()":
        df = df.withColumn(field.name, trim(col(field.name)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, round

df = spark.read.table("Silver_Order_Details")

df_with_total = df.withColumn(
    "TotalAmount", 
    round(col("UnitPrice") * col("Quantity") * (1 - col("Discount")), 2)
)

df_with_total.select("OrderID", "ProductID", "UnitPrice", "Quantity", "Discount", "TotalAmount").show(10)

df_with_total.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("Silver_Order_Details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check every string column in every Silver table for inconsistent formatting
# (i.e. the same value appearing with different capitalization, like "Germany" vs "germany")

from pyspark.sql.functions import col, lower, countDistinct

# tables = ["Categories", "CustomerDemographics", "Customers", "Employees",
#           "Order_Details", "Orders", "Products", "Regions", 
#           "Shippers", "Suppliers", "Territories",
#           "Alphabetical_list_of_products", "Category_Sales_for_1997",
#           "Current_Product_Lists", "Customer_and_Suppliers_by_Cities",
#           "Invoices", "Order_Details_Extendeds", "Order_Subtotals",
#           "Orders_Qries", "Product_Sales_for_1997", "Products_Above_Average_Prices",
#           "Products_by_Categories", "Sales_by_Categories", "Sales_Totals_by_Amounts",
#           "Summary_of_Sales_by_Quarters", "Summary_of_Sales_by_Years"]

results = []

for t in tables:
    df = spark.read.table(f"Silver_{t}")
    string_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]
    
    for c in string_cols:
        distinct_original = df.select(c).distinct().count()
        distinct_lower = df.select(lower(col(c))).distinct().count()
        
        if distinct_original > distinct_lower:
            results.append((t, c, distinct_original, distinct_lower, distinct_original - distinct_lower))

if results:
    summary_df = spark.createDataFrame(results, ["Table", "Column", "DistinctValues", "DistinctLowercase", "InconsistentCount"])
    summary_df.show(50, truncate=False)
else:
    print("No inconsistent text formatting found in any column")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date

df = spark.read.table("Silver_Employees")
df = df.withColumn("BirthDate", to_date(col("BirthDate")))
df = df.withColumn("HireDate", to_date(col("HireDate")))

df.printSchema()
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("Silver_Employees")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check Order_Details for invalid numeric values:
# negative quantities, negative prices, and discounts outside the valid 0-1 range

df = spark.read.table("Silver_Order_Details")

print("Negative quantities:", df.filter(df.Quantity < 0).count())
print("Negative prices:", df.filter(df.UnitPrice < 0).count())
print("Invalid discounts (below 0 or above 1):", df.filter((df.Discount < 0) | (df.Discount > 1)).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check referential integrity across multiple table relationships
# For each pair, find rows in the "child" table whose foreign key
# doesn't match any row in the "parent" table

checks = [
    ("Order_Details", "Orders", "OrderID"),
    ("Order_Details", "Products", "ProductID"),
    ("Orders", "Employees", "EmployeeID"),
    ("Products", "Categories", "CategoryID"),
    ("Products", "Suppliers", "SupplierID"),
]

results = []

for child_table, parent_table, key_col in checks:
    child = spark.read.table(f"Silver_{child_table}")
    parent = spark.read.table(f"Silver_{parent_table}")
    
    orphans = child.join(parent, key_col, "left_anti")
    count = orphans.count()
    results.append((child_table, parent_table, key_col, count))

summary_df = spark.createDataFrame(results, ["ChildTable", "ParentTable", "KeyColumn", "OrphanCount"])
summary_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check referential integrity across all meaningful table relationships in the 26-table dataset

checks = [
    ("Order_Details", "Orders", "OrderID"),
    ("Order_Details", "Products", "ProductID"),
    ("Order_Details_Extendeds", "Orders", "OrderID"),
    ("Order_Details_Extendeds", "Products", "ProductID"),
    ("Order_Subtotals", "Orders", "OrderID"),
    ("Orders", "Employees", "EmployeeID"),
    ("Orders", "Customers", "CustomerID"),
    ("Orders", "Shippers", "ShipVia"),
    ("Products", "Categories", "CategoryID"),
    ("Products", "Suppliers", "SupplierID"),
    ("Products_by_Categories", "Categories", "CategoryID"),
    ("Employees", "Employees", "ReportsTo"),
]

results = []

for child_table, parent_table, key_col in checks:
    try:
        child = spark.read.table(f"Silver_{child_table}")
        parent = spark.read.table(f"Silver_{parent_table}")
        
        if key_col in child.columns and key_col in parent.columns:
            orphans = child.join(parent, key_col, "left_anti")
            count = orphans.count()
            results.append((child_table, parent_table, key_col, count))
    except Exception as e:
        results.append((child_table, parent_table, key_col, f"ERROR: {str(e)}"))

summary_df = spark.createDataFrame(results, ["ChildTable", "ParentTable", "KeyColumn", "OrphanCount"])
summary_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
