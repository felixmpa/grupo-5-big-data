"""Procesamiento del Data Lake con Spark.

Lee el histórico crudo que dejó el consumidor de HDFS (PR 4) y genera dos
tablas en Parquet, listas para el modelo del PR 6:

1. lecturas: lecturas limpias (sin duplicados ni datos inválidos).
2. ventanas: características por máquina cada 30 segundos (promedio,
   desviación, mínimo, máximo y tendencia de cada sensor).

    crudo (JSON)  ->  limpiar + deduplicar  ->  lecturas (Parquet)
                                            ->  ventanas (Parquet)
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

HDFS = os.getenv("HDFS_URL", "hdfs://namenode:8020")
ENTRADA = f"{HDFS}/datalake/crudo/sensores"
SALIDA_LECTURAS = f"{HDFS}/datalake/procesado/lecturas"
SALIDA_VENTANAS = f"{HDFS}/datalake/procesado/ventanas"
VENTANA = os.getenv("VENTANA", "30 seconds")

SENSORES = ["temperatura", "vibracion", "presion", "rpm"]
ESTADOS = ["normal", "degradacion", "falla"]
# Rangos físicamente posibles. Lo que quede fuera es un error del sensor.
RANGOS = {
    "temperatura": (-40, 200),  # °C
    "vibracion": (0, 100),      # mm/s
    "presion": (0, 20),         # bar
    "rpm": (0, 5000),
}

# Esquema explícito: si un valor no tiene el tipo esperado (por ejemplo
# "temperatura": "caliente") Spark lo deja en null en vez de fallar.
ESQUEMA = StructType([
    StructField("maquina_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("temperatura", DoubleType()),
    StructField("vibracion", DoubleType()),
    StructField("presion", DoubleType()),
    StructField("rpm", DoubleType()),
    StructField("estado", StringType()),
    StructField("_kafka_particion", IntegerType()),
    StructField("_kafka_offset", LongType()),
])


def limpiar(crudo):
    """Quita duplicados y lecturas inválidas. Devuelve (limpias, resumen)."""
    # 1. Duplicados: la misma (partición, offset) de Kafka es la misma lectura.
    unicas = crudo.dropDuplicates(["_kafka_particion", "_kafka_offset"])

    # 2. Fecha: try_to_timestamp devuelve null si el texto no es una fecha.
    con_fecha = unicas.withColumn("momento", F.try_to_timestamp("timestamp"))

    # 3. Validación: campos presentes, estado conocido y valores en rango.
    valida = (
        F.col("maquina_id").isNotNull()
        & F.col("momento").isNotNull()
        & F.col("estado").isin(ESTADOS)
    )
    for sensor, (minimo, maximo) in RANGOS.items():
        valida &= F.col(sensor).between(minimo, maximo)  # null -> no válida

    limpias = (
        con_fecha.where(valida)
        .select("maquina_id", "momento", *SENSORES, "estado")
        .withColumn("fecha", F.to_date("momento"))
    )
    return limpias, unicas


def caracteristicas(lecturas):
    """Resume cada máquina en ventanas de tiempo fijas."""
    segundos = F.col("momento").cast("double")  # para calcular la tendencia
    agregados = [F.count("*").alias("lecturas")]
    for s in SENSORES:
        agregados += [
            F.avg(s).alias(f"{s}_media"),
            F.stddev(s).alias(f"{s}_desv"),
            F.min(s).alias(f"{s}_min"),
            F.max(s).alias(f"{s}_max"),
            # Pendiente de la recta que mejor ajusta: cuánto sube por segundo.
            # try_divide: si la ventana tiene una sola lectura, da null en vez de error.
            F.try_divide(F.covar_pop(segundos, s), F.var_pop(segundos)).alias(f"{s}_tendencia"),
        ]
    # Etiquetas (solo existen porque la simulación las envía): sirven para
    # entrenar y evaluar el modelo en el PR 6.
    agregados += [
        F.mode("estado").alias("estado_mayoritario"),
        F.sum((F.col("estado") == "degradacion").cast("int")).alias("lecturas_degradacion"),
        F.sum((F.col("estado") == "falla").cast("int")).alias("lecturas_falla"),
    ]
    resumen = lecturas.groupBy("maquina_id", F.window("momento", VENTANA).alias("ventana")).agg(*agregados)
    columnas = [c for c in resumen.columns if c not in ("maquina_id", "ventana")]
    return resumen.select(
        "maquina_id",
        F.col("ventana.start").alias("inicio"),
        F.col("ventana.end").alias("fin"),
        *columnas,
    )


def main():
    spark = (
        SparkSession.builder.appName("procesar-datalake")
        .config("spark.sql.session.timeZone", "UTC")
        # Conectarse al datanode por su nombre ("datanode") y no por su IP
        # interna de Docker. Necesario para correr este script fuera de Docker.
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        # Pocos datos: 8 particiones internas alcanzan (por defecto son 200).
        # En un clúster real con muchos datos se sube este número.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.debug.maxToStringFields", "100")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"Leyendo {ENTRADA}")
    crudo = spark.read.schema(ESQUEMA).json(ENTRADA).cache()
    lecturas, unicas = limpiar(crudo)
    lecturas = lecturas.cache()

    total, n_unicas, n_validas = crudo.count(), unicas.count(), lecturas.count()
    print(f"Lecturas en el Data Lake: {total}")
    print(f"  duplicadas eliminadas:  {total - n_unicas}")
    print(f"  inválidas descartadas:  {n_unicas - n_validas}")
    print(f"  lecturas limpias:       {n_validas}")

    # repartition("fecha"): un archivo por día. Sin esto Spark escribe ~200
    # archivos chicos (uno por partición interna), el mismo problema del PR 4.
    lecturas.repartition("fecha").write.mode("overwrite").partitionBy("fecha").parquet(SALIDA_LECTURAS)
    print(f"Guardado {SALIDA_LECTURAS}")

    ventanas = caracteristicas(lecturas).withColumn("fecha", F.to_date("inicio"))
    ventanas.repartition("fecha").write.mode("overwrite").partitionBy("fecha").parquet(SALIDA_VENTANAS)
    ventanas = spark.read.parquet(SALIDA_VENTANAS)
    print(f"Guardado {SALIDA_VENTANAS} ({ventanas.count()} ventanas de {VENTANA})")

    print("\nPromedio de las características según el estado de la máquina:")
    (
        ventanas.groupBy("estado_mayoritario")
        .agg(
            F.count("*").alias("ventanas"),
            *[F.round(F.avg(f"{s}_media"), 2).alias(f"{s}_media") for s in SENSORES],
            F.round(F.avg("vibracion_desv"), 3).alias("vibracion_desv"),
            F.round(F.avg("temperatura_tendencia"), 3).alias("temp_tendencia_por_s"),
        )
        .orderBy(F.array_position(F.array(*map(F.lit, ESTADOS)), F.col("estado_mayoritario")))
        .show(truncate=False)
    )
    spark.stop()


if __name__ == "__main__":
    main()
