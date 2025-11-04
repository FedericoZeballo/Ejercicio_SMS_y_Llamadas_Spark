# FUNCIONES - SMS FACTURACIÓN
# En este notebook contiene las funciones necesarias para resolver el  ejercicio solicitado.

# Configuración de entorno

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType 
import hashlib

spark = SparkSession.builder \
    .appName("SMS Facturacion") \
    .getOrCreate()

# Carga los datos

def cargar_eventos(path_eventos):
    return spark.read.csv(path_eventos, header=True, inferSchema=True)
    
def cargar_destinos_gratis(path_destinos_gratis):
    return spark.read.csv(path_destinos_gratis, header=True, inferSchema=True)

# Limpiar datos

def filtrar_nulos(df):
    return df.filter(df.id_source.isNotNull() & df.id_destination.isNotNull())

# Calcular la facturacion via SMS

def calcular_monto_sms(df_eventos, df_destinos_gratis):
    df_eventos = df_eventos.join(df_destinos_gratis, df_eventos.id_destination == df_destinos_gratis.id, "left_outer")
    df_eventos = df_eventos.withColumn("monto_sms", when(df_destinos_gratis.id.isNotNull(), 0.0)
        .when(df_eventos.region.between(1, 5), 1.5)
        .when(df_eventos.region.between(6, 9), 2.0)
        .otherwise(0.0))
    df_eventos = df_eventos.withColumn("total_sms", df_eventos.sms * df_eventos.monto_sms)
    return df_eventos

# Top de usuarios por facturación

import hashlib
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def hash_md5(value):
    return hashlib.md5(value.encode()).hexdigest()

hash_udf = udf(hash_md5, StringType())

def top_usuarios(df_eventos):
    df_top = df_eventos.groupBy("id_source").sum("total_sms").withColumnRenamed("sum(total_sms)", "monto_total")
    df_top = df_top.orderBy(col("monto_total").desc()).limit(100)
    df_top = df_top.withColumn("id_hash", hash_udf(col("id_source")))
    return df_top

# Guarda el archivo parquet

def guardar_parquet(df, path):
    df.write.parquet(path, compression="gzip", mode="overwrite")