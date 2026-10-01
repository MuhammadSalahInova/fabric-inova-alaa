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

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

schema = StructType([
    StructField("TableName", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("RunDate", TimestampType(), True),
    StructField("ErrorMessage", StringType(), True)
])

initial_row = spark.createDataFrame(
    [Row(TableName="Test", Status="Success", RunDate=datetime.now(), ErrorMessage=None)],
    schema=schema
)

initial_row.write.format("delta").mode("overwrite").saveAsTable("PipelineTableLog")

print("PipelineTableLog table created successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

schema = StructType([
    StructField("TableName", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("RunDate", TimestampType(), True),
    StructField("ErrorMessage", StringType(), True)
])

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

overall_status = "Success"
overall_error = None

for t in tables:
    tableName = t
    try:
        clean_table(t)
        status = "Success"
        error_msg = None
    except Exception as e:
        status = "Failed"
        error_msg = str(e)
        overall_status = "Failed"
        overall_error = f"{t}: {error_msg}"
        print(f"⚠️ {t}: FAILED - {error_msg}")
    
   
    new_row = spark.createDataFrame(
        [Row(TableName=tableName, Status=status, RunDate=datetime.now(), ErrorMessage=error_msg)],
        schema=schema
    )
    new_row.write.format("delta").mode("append").saveAsTable("PipelineTableLog")


latest_row = spark.createDataFrame(
    [Row(TableName="Bronze_Silver_Pipeline", Status=overall_status, RunDate=datetime.now(), ErrorMessage=overall_error)],
    schema=schema
)
latest_row.write.format("delta").mode("overwrite").saveAsTable("LatestStatus")

print(f"✅ Done - LatestStatus = {overall_status}")

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
