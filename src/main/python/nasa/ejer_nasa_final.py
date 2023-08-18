from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First PySpark Demo") \
    .master("local[2]") \
    .getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

nasa_df = spark.read.text("C:/Users/julia.blanco/Desktop/repositorios/WebLogServersNasa/src/main/resources/access_log_Jul95")
nasa_df.printSchema()

nasa_df.show(3, truncate=False)

reg_exp = "^(\\S+) \\S+ \\S+ \\[([\\w:/]+)\\s[\\-]\\d{4}\\] \"(\\S+) (\\S+) (\\w+/\\S*)\" (\\d{3}) (\\S+)$"

nasa_df_final = nasa_df.select(
    regexp_extract(col("value"), reg_exp, 1).alias("host"),
    regexp_extract(col("value"), reg_exp, 2).alias("timestamp"),
    when(regexp_extract(col("value"), reg_exp, 3) == "", "Unknown")
    .otherwise(regexp_extract(col("value"), reg_exp, 3)).alias("method"),
    regexp_extract(col("value"), reg_exp, 4).alias("resource"),
    when(regexp_extract(col("value"), reg_exp, 5) == "", "Unknown")
    .otherwise(
        when(regexp_extract(col("value"), reg_exp, 5) == "HTTP/V1.0", "HTTP/1.0")
        .otherwise(regexp_extract(col("value"), reg_exp, 5))
    ).alias("protocol"),
    regexp_extract(col("value"), reg_exp, 6).cast("integer").alias("status"),
    when(regexp_extract(col("value"), reg_exp, 7) == "-", None)
    .otherwise(regexp_extract(col("value"), reg_exp, 7).cast("integer")).alias("size")
).withColumn("timestamp", to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss"))

nasa_df_final.show()

# Consultas

# ¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.
nasa_df_final.groupBy("protocol").count().show()

# ¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos para ver cuál es el más común.
nasa_df_final.groupBy("status")\
    .count()\
    .orderBy(desc("count"))\
    .show()

# ¿Y los métodos de petición (verbos) más utilizados?
nasa_df_final.groupBy("method")\
    .count()\
    .orderBy(desc("count"))\
    .show()

# ¿Qué recurso tuvo la mayor transferencia de Bytes?
nasa_df_final.groupBy("resource")\
    .agg(sum("size").alias("totalSize"))\
    .orderBy(desc("totalSize"))\
    .show(1, truncate=False)

# Además, queremos saber qué recurso de nuestra web es el que más tráfico recibe.
nasa_df_final.groupBy("resource")\
    .count()\
    .orderBy(desc("count"))\
    .show(1, truncate=False)

# ¿Qué día la web recibió más tráfico?
nasa_with_date = nasa_df_final.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
nasa_with_date.groupBy("date").count().orderBy(desc("count")).show(10)

# ¿Cuáles son los hosts más frecuentes?
nasa_df_final.groupBy("host").count().orderBy(desc("count")).show()

# ¿A qué horas se produce el mayor tráfico de la web?
nasa_with_hours = nasa_df_final.withColumn("hour", hour(col("timestamp")))
nasa_with_hours.groupBy("hour").count().orderBy(desc("count")).show(10)

# ¿Cuál es el número de errores 404 que ha habido cada día?
nasa_error_date = nasa_df_final.filter(col("status") == 404).withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
nasa_error_date.groupBy("date").count().withColumnRenamed("count", "errors").orderBy(desc("errors")).show()