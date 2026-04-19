# setting up libraries

import requests
import json
import os
import time
import requests.exceptions
from collections import defaultdict

# import pyspark and libraries and creating spark session

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# creating spark session

spark = SparkSession.builder \
    .appName("API_data") \
    .config("spark.sql.debug.maxToStringFields", 200) \
    .getOrCreate()
    
# import credentials

from credentials import BASE_URL, headers, token

# Fetch data from API for 'products'

from requests import status_codes

# defining the function to fetch data from the API

class APIError(Exception):
    """Raised when the API call fails."""
    pass


def fetch_data_from_api(base_url: str, headers: dict) -> list: # Fetch data from the API
    response = requests.get(f"{base_url}/products", headers=headers)
    response.raise_for_status()
    return response.json()


def normalize(record: dict) -> dict: #Normalize the record to be compatible with Spark schema
    record = record.copy()
    # Serialize nested dicts to JSON strings, these fields are nested in the API response
    for key , value in record.items():
        if isinstance(value, (dict, list)):
            record[key] = json.dumps(value)
        elif value is None:
                record[key] = "" # Stringify None values
    return record

# load data into spark dataframe function to load the normalized data into a spark dataframe

def load_data_into_spark(spark: SparkSession, data: list, table_name: str) -> None:
    normalized = [normalize(r) for r in data]  # matches the renamed function
    
    type_map = defaultdict(set)
    for record in normalized:                  # iterating over the list
        for k, v in record.items():
            type_map[k].add(type(v).__name__)
    all_none = [k for k, types in type_map.items() if types == {"NoneType"}]
    print(f"[DEBUG] All-None fields: {all_none}")


    # flag any remaining type inconsistencies before createDataFrame
    for i, record in enumerate(normalized):
        cat = record.get("category")
        if not isinstance(cat, (str, type(None))):
            print(f"[WARN] Row {i}: category is still {type(cat).__name__!r} → {cat!r}")
    df = spark.createDataFrame(normalized)
    df = cleanse_data(df)
    df.show(truncate=False)
    return df

def cleanse_data(df):
 
    #Replace empty strings with NULL across all string columns to avoid silent failures
    
    str_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]
    for col in str_cols:
        df = df.withColumn(col, F.nullif(F.col(col), F.lit("")))

    #Cast date strings to TimestampType
    
    for date_col in ("createdOn", "modifiedOn"):
        if date_col in df.columns:
            df = df.withColumn(date_col, F.to_timestamp(F.col(date_col)))
            
    #Drop columns where every row is NULL avoid unnecessary data storage
    
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0]
    total = df.count()
    all_null_cols = [c for c in df.columns if null_counts[c] == total]
    print(f"[CLEANSE] Dropping all-null columns: {all_null_cols}")
    df = df.drop(*all_null_cols)
    
    # Standatize name casing with title case 
    
    df = df.withColumn("name", F.initcap(F.trim(F.col("name"))))

    TYPO_MAP = {
        
        "Agua Gristal": "Agua Cristal",
        "Adiiconal Salsa": "Adicional Salsa",
    }
    for wrong, correct in TYPO_MAP.items():
        df = df.withColumn("name", F.when(F.col("name") == wrong, F.lit(correct)).otherwise(F.col("name")))
        
        
    # Extract real price from first location entry
    df = df.withColumn(
        "price",
        F.get_json_object(F.col("locationsStock"), "$[0].price").cast("long")
    )
    df = df.withColumn("category", F.get_json_object(F.col("category"), "$.name"))

    df = df.filter((F.col("isActive") == True) & (F.col("deleted") == False))

    df = df.select("name", "price", "category", "isActive", "type", "createdOn", "modifiedOn")

    df = df.withColumn("name", F.regexp_replace(F.col("name"), "\\s+", " "))

    df.groupBy("name").count().filter(F.col("count") > 1).orderBy(F.desc("count")).show(truncate=False)

    window = Window.partitionBy("name").orderBy(F.col("modifiedOn").desc())
    df = df.withColumn("_row", F.row_number().over(window))
    df = df.filter(F.col("_row") == 1).drop("_row")
    df = df.fillna({"price": 0})

    print("[CLEANSE] Remaining duplicates:")
    df.groupBy("name").count().filter(F.col("count") > 1).show(truncate=False)
    print(f"[CLEANSE] Final row count: {df.count()}")

    df = df.select("name", "price", "category", "type", "createdOn", "modifiedOn")

    print("[CLEANSE] Null counts:")
    
    df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).show(truncate=False)


    df.printSchema()
    
    return df
    
        
# Main function

def main():
    spark = SparkSession.builder.appName("travuco_etl").getOrCreate()
    data = fetch_data_from_api(BASE_URL, headers)
    df = load_data_into_spark(spark, data, "products")

    # Export to CSV
    output_path = "output/products_clean"
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(output_path)
    print(f"[EXPORT] CSV written to {output_path}")

    # Rename to a clean single file
    import glob, shutil
    part_file = glob.glob(f"{output_path}/part-*.csv")[0]
    shutil.move(part_file, "output/products_clean.csv")
    shutil.rmtree(output_path)
    print("[EXPORT] Final file: output/products_clean.csv")

if __name__ == "__main__":
    main()