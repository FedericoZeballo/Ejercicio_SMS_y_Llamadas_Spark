# FUNCIONES - SMS FACTURACIÓN
# Este notebook contiene las funciones se son llamadas desde SMS_Facturacion_Ejecutor

# Configuración de entorno

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf, broadcast
from pyspark.sql.types import StringType 
import pyspark.sql.functions as F
import hashlib


# ===================================================================================
# Carga los datos

def cargar_eventos(spark, path_eventos):
    return spark.read.csv(path_eventos, header=True, inferSchema=True)    
def cargar_destinos_gratis(spark, path_destinos_gratis):
    return spark.read.csv(path_destinos_gratis, header=True, inferSchema=True)


# ==================================================================================
# Limpia datos

def filtrar_nulos(df):
    return df.filter(df.id_source.isNotNull() & df.id_destination.isNotNull())


# ==================================================================================
# Calcula la facturacion via SMS

def calcular_monto_sms(df_eventos, df_destinos_gratis):
    # Asigna el alias a los DataFrames
    eventos = df_eventos.alias("eventos")
    gratis = df_destinos_gratis.alias("gratis")

    # Realiza el join con los destinos gratis
    df_joined = eventos.join(
        broadcast(gratis),
        col("eventos.id_destination") == col("gratis.id"),
        "left"
    )

    # Calcula el monto usando columnas con alias
    df_joined = df_joined.withColumn(
        "monto_sms",
        F.when(F.col("gratis.id").isNotNull(), F.lit(0.0))
        .when(F.col("eventos.region").between(1, 5), F.lit(1.5))
        .when(F.col("eventos.region").between(6, 9), F.lit(2.0))
        .otherwise(F.lit(0.0))
    )

    df_joined = df_joined.withColumn("total_sms", F.col("eventos.sms") * F.col("monto_sms"))

    return df_joined


# ==================================================================================
# Top de usuarios por facturación

def hash_md5(value):
    return hashlib.md5(value.encode()).hexdigest()

hash_udf = udf(hash_md5, StringType())

def top_usuarios(df_eventos):
    df_top = df_eventos.groupBy("id_source").sum("total_sms").withColumnRenamed("sum(total_sms)", "monto_total")
    df_top = df_top.orderBy(col("monto_total").desc()).limit(100)
    df_top = df_top.withColumn("id_hash", hash_udf(col("id_source")))
    return df_top


# ==================================================================================
# Guarda el archivo parquet

def guardar_parquet(df, path):
    df.write.parquet(path, compression="gzip", mode="overwrite")


# ==================================================================================
# Calcula las llamadas por hora para el histograma

def llamadas_por_hora(df_eventos):
    return df_eventos.groupBy("hour").sum("calls").orderBy("hour")
