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

#Data Profiling/Count Check
import requests

tables = ["Categories", "CustomerDemographics", "Customers", "Employees",
          "Order_Details", "Orders", "Products", "Regions", 
          "Shippers", "Suppliers", "Territories",
          "Alphabetical_list_of_products", "Category_Sales_for_1997",
          "Current_Product_Lists", "Customer_and_Suppliers_by_Cities",
          "Invoices", "Order_Details_Extendeds", "Order_Subtotals",
          "Orders_Qries", "Product_Sales_for_1997", "Products_Above_Average_Prices",
          "Products_by_Categories", "Sales_by_Categories", "Sales_Totals_by_Amounts",
          "Summary_of_Sales_by_Quarters", "Summary_of_Sales_by_Years"]

results = []

for t in tables:
    try:
        url = f"https://services.odata.org/V4/Northwind/Northwind.svc/{t}/$count"
        source_count = int(requests.get(url).text)
        
        bronze_count = spark.read.table(t).count()
        
        match = "Match" if source_count == bronze_count else "Mismatch"
        results.append((t, source_count, bronze_count, match))
    except Exception as e:
        results.append((t, -1, -1, "Error"))

summary_df = spark.createDataFrame(results, ["Table", "SourceCount", "BronzeCount", "Status"])
summary_df.show(30, truncate=False) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

def get_source_sum(table, column):
    base_url = "https://services.odata.org/V4/Northwind/Northwind.svc/"
    url = f"{base_url}{table}?$select={column}"
    total = 0
    
    while url:
        response = requests.get(url, headers={"Accept": "application/json"})
        result = response.json()
        total += sum(float(d[column]) for d in result["value"] if d[column] is not None)
        
        next_link = result.get("@odata.nextLink")
        if next_link and not next_link.startswith("http"):
            next_link = base_url + next_link
        url = next_link
    
    return total

checks = {
    "Products": "UnitPrice",
    "Order_Details": "UnitPrice",
    "Order_Details_Extendeds": "ExtendedPrice",
    "Orders": "Freight",
}

results = []

for table, column in checks.items():
    source_sum = get_source_sum(table, column)
    
    df = spark.read.table(table)
    bronze_col = column if table == "Order_Details_Extendeds" else f"value.{column}"
    bronze_sum = df.selectExpr(f"sum(`{bronze_col}`)").collect()[0][0]
    
    status = "Match" if round(source_sum, 2) == round(bronze_sum, 2) else "Mismatch"
    results.append((table, column, round(source_sum, 2), round(bronze_sum, 2), status))

summary_df = spark.createDataFrame(results, ["Table", "Column", "SourceSum", "BronzeSum", "Status"])
summary_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

def get_source_minmax(table, column):
    base_url = "https://services.odata.org/V4/Northwind/Northwind.svc/"
    url = f"{base_url}{table}?$select={column}"
    values = []
    
    while url:
        response = requests.get(url, headers={"Accept": "application/json"})
        result = response.json()
        for d in result["value"]:
            v = d.get(column)
            if v is not None:
                try:
                    values.append(float(v))
                except (ValueError, TypeError):
                    return None, None
        
        next_link = result.get("@odata.nextLink")
        if next_link and not next_link.startswith("http"):
            next_link = base_url + next_link
        url = next_link
    
    if not values:
        return None, None
    return min(values), max(values)

tables = ["Categories", "CustomerDemographics", "Customers", "Employees",
          "Order_Details", "Orders", "Products", "Regions", 
          "Shippers", "Suppliers", "Territories",
          "Alphabetical_list_of_products", "Category_Sales_for_1997",
          "Current_Product_Lists", "Customer_and_Suppliers_by_Cities",
          "Invoices", "Order_Details_Extendeds", "Order_Subtotals",
          "Orders_Qries", "Product_Sales_for_1997", "Products_Above_Average_Prices",
          "Products_by_Categories", "Sales_by_Categories", "Sales_Totals_by_Amounts",
          "Summary_of_Sales_by_Quarters", "Summary_of_Sales_by_Years"]

detailed_results = []

for t in tables:
    try:
        df = spark.read.table(t)
        
        # نشيل "value." من أسامي الأعمدة عشان نعرف الاسم الحقيقي للمقارنة مع المصدر
        col_map = {}
        for f in df.schema.fields:
            clean_name = f.name.replace("value.", "") if f.name.startswith("value.") else f.name
            col_map[clean_name] = f.name
        
        numeric_cols = [f.name for f in df.schema.fields 
                         if f.dataType.typeName() in ["long", "double", "integer", "float"]]
        
        for c in numeric_cols:
            clean_name = c.replace("value.", "") if c.startswith("value.") else c
            
            row = df.selectExpr(f"min(`{c}`) as mn", f"max(`{c}`) as mx").collect()[0]
            bronze_min, bronze_max = row["mn"], row["mx"]
            if bronze_min is None:
                continue
            
            source_min, source_max = get_source_minmax(t, clean_name)
            if source_min is None:
                continue
            
            status = "Match" if round(source_min, 2) == round(float(bronze_min), 2) and round(source_max, 2) == round(float(bronze_max), 2) else "Mismatch"
            
            detailed_results.append((t, clean_name, float(source_min), float(bronze_min), float(source_max), float(bronze_max), status))
    except Exception:
        pass

detail_df = spark.createDataFrame(detailed_results, ["Table", "Column", "SourceMin", "BronzeMin", "SourceMax", "BronzeMax", "Status"])
detail_df.orderBy("Table").show(100, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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
