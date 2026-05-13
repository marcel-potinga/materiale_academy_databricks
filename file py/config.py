from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import logging
from datetime import datetime
import pytest

STORAGE_ACCOUNT = "" #inserire nome storage account
CATALOG_NAME = "medallion_work1" #inserire nome catalogo
RAW = "raw" #inserire nome container raw
SCHEMA_NAME_B = "bronze1" #inserire nome schema bronze
SCHEMA_NAME_S = "silver1" #inserire nome schema silver
SCHEMA_NAME_G = "gold1" #inserire nome schema gold
#NEL NOSTRO CONFIG AVEVAMO 2 TABELLE STORE E SALES, SE NE AVESSIMO AVUTE DI PIU' BASTA AGGIUNGERE LE VARIABILI QUI SOTTO E AGGIUNGERE LE VARIABILI FULL_TABLE_PATH_B_XXX, FULL_TABLE_PATH_S_XXX E FULL_TABLE_PATH_GOLD 
TABLE_NAME_STORE = "store" #inserire nome tabella store
TABLE_NAME_SALES = "sales" #inserire nome tabella sales

FULL_TABLE_PATH_B_STORE = f"{CATALOG_NAME}.{SCHEMA_NAME_B}.{TABLE_NAME_STORE}"  # archiviazioneaccount.bronze.store
FULL_TABLE_PATH_B_SALES = f"{CATALOG_NAME}.{SCHEMA_NAME_B}.{TABLE_NAME_SALES}"   # archiviazioneaccount.bronze.sales

FULL_TABLE_PATH_S_STORE = f"{CATALOG_NAME}.{SCHEMA_NAME_S}.{TABLE_NAME_STORE}"      # archiviazioneaccount.silver.store
FULL_TABLE_PATH_S_SALES = f"{CATALOG_NAME}.{SCHEMA_NAME_S}.{TABLE_NAME_SALES}"      # archiviazioneaccount.silver.sales

FULL_TABLE_PATH_GOLD = f"{CATALOG_NAME}.{SCHEMA_NAME_G}.gold_kpi"               # archiviazioneaccount.gold.gold_kpi


store_path = f"abfss://{RAW}@{STORAGE_ACCOUNT}.dfs.core.windows.net/stores/" #inserire percorso completo file del vostro dataset
raw_path = f"abfss://{RAW}@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales/" #inserire percorso completo file del vostro dataset


spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    dbutils.secrets.get(scope="my-kv-scope", key="storage-key")
) #inserire nome scope e nome chiave del vostro secret 

# COMMAND ----------



# Queste funzioni sono state usate nel nostro progetto per i layer silver, potete prendere nota in quanto sono abbastanza comuni e potrebbero essere utili anche per i vostri progetti. Ovviamente, se avete bisogno di funzioni più specifiche, potete crearle voi stessi seguendo la stessa logica.
# Non sono state usate direttamente in quanto defindole le potevamo anche testare singolarmente, ma sono state usate all'interno di una funzione più grande che le chiamava tutte in sequenza.

"""
Trasformations for store df
"""
def map_assortment(df: DataFrame) -> DataFrame:

    return df.withColumn(
        "Assortment",
        F.when(F.col("Assortment") == "a", "Basic")
         .when(F.col("Assortment") == "b", "Extra")
         .when(F.col("Assortment") == "c", "Extended")
         .otherwise("Other")
    )


def filter_null_store(df: DataFrame) -> DataFrame:
    return df.filter(F.col("Store").isNotNull())


def deduplicate_stores(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["Store"])


def add_promo_flag(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "Any_Promotions",
        F.when(F.col("Promo2") == 0, "No").otherwise("Yes")
    )


def add_silver_timestamp(df: DataFrame) -> DataFrame:
    return df.withColumn("_silver_processed_at", F.current_timestamp())


def transform_store(df: DataFrame) -> DataFrame:
    """
    Applica tutte le trasformazioni nell'ordine corretto.
    """
    df = map_assortment(df)
    df = filter_null_store(df)
    df = deduplicate_stores(df)
    df = add_promo_flag(df)
    df = add_silver_timestamp(df)
    df = df.select(
        "Store", "Assortment", "CompetitionDistance",
        "CompetitionOpenSinceMonth", "CompetitionOpenSinceYear",
        "Promo2SinceWeek", "Promo2SinceYear", "PromoInterval",
        "Any_Promotions", "_silver_processed_at"
    )
    return df



# COMMAND ----------

"""
Trasformations for sales df
"""
def map_state_holiday(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "StateHoliday",
        F.when(F.col("StateHoliday") == "0", "None")
         .when(F.col("StateHoliday") == "a", "Public")
         .when(F.col("StateHoliday") == "b", "Easter")
         .when(F.col("StateHoliday") == "c", "Christmas")
         .otherwise("Other")
    )


def filter_null_store_and_date(df: DataFrame) -> DataFrame:
    return df.filter(F.col("Store").isNotNull()).filter(F.col("Date").isNotNull())


def deduplicate_sales(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["Store", "Date"])


def cast_store_to_int(df: DataFrame) -> DataFrame:
    return df.withColumn("Store", F.expr("try_cast(Store as int)"))


def transform_sales(df: DataFrame) -> DataFrame:
    """
    Pipeline completa Sales: Bronze to Silver.
    """
    df = map_state_holiday(df)
    df = filter_null_store_and_date(df)
    df = deduplicate_sales(df)
    df = cast_store_to_int(df)
    df = add_silver_timestamp(df)
    df = df.select(
        F.col("Store"), "Date", "DayOfWeek", "Sales", "Customers",
        "Open", "Promo", "StateHoliday", "SchoolHoliday",
        "_silver_processed_at"
    )
    return df