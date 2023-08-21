from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("First PySpark Demo") \
    .master("local[2]") \
    .getOrCreate()

datos_padron = "C:/Users/julia.blanco/Desktop/repositorios/WebLogServersNasa/src/main/resources/estadisticas202212.csv"

df = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .option("delimiter", ";") \
  .option("quote", "\"") \
  .option("emptyValue", "0") \
  .load(datos_padron)

df.show(10, truncate=False)

df.printSchema()

# DOS MANERAS DE HACER EL TRIM
# OPCIÓN 1:
'''
padron_df = df.select([trim(col(field.name)).alias(field.name) if (field.dataType == StringType) else col(field.name) for field in df.schema.fields])
padron_df.show(10, truncate=False)
'''
# OPCIÓN 2:
columnas_string = ["DESC_DISTRITO", "DESC_BARRIO"]
#columnas_no_string = [col for col in df.schema.names if col not in columnas_string]
padron_df = df.select([trim(col(column)).alias(column) if column in columnas_string else col(column) for column in df.schema.names])
padron_df.show(10, truncate=False)

# 6.3 Enumera todos los barrios diferentes.
barrios_df = (padron_df
              .select('COD_BARRIO', 'DESC_BARRIO')
              .distinct()
              .orderBy('COD_BARRIO')
              .show(30, truncate=False))

# 6.4 Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios diferentes que hay.
padron_df.createOrReplaceTempView("padron")
numero_barrios = spark.sql("SELECT COUNT(DISTINCT DESC_BARRIO) FROM padron")
numero_barrios.show()

# 6.5 Crea una nueva columna que muestre la longitud de los campos de la columna DESC_DISTRITO y que se llame "longitud".
padron_long_df = padron_df.withColumn("longitud", length(padron_df["DESC_DISTRITO"]))
padron_long_df.select('DESC_DISTRITO', 'longitud').show(30, truncate=False)

# 6.6 Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.
padron_df = padron_df.withColumn('VALOR_5', lit(5))
padron_df.show(5, truncate=False)

# 6.7 Borra esta columna.
padron_df = padron_df.drop('VALOR_5')
padron_df.show(10,truncate=False)

# 6.8 Particiona el DataFrame por las variables DESC_DISTRITO Y DESC_BARRIO.
# Para hacer una partición con partitionBy() tengo que hacerla en el contexto de guardar el DataFrame en un archivo, por ejemplo así:
'''
padron_df.write.format("csv")\
  .option("header", "true")\
  .partitionBy('DESC_DISTRITO', 'DESC_BARRIO')\
  .save("C:/Users/julia.blanco/Desktop/repositorios/WebLogServersNasa/src/main/resources/padron_particionado.csv")
'''

# La otra manera de hacerlo sin hacer un .write es con repartition:
padron_df = padron_df.repartition('DESC_DISTRITO', 'DESC_BARRIO')

# 6.9 Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado de los rdds almacenados.
padron_df.cache()

# 6.10 Lanza una consulta contra el DF resultante en la que muestre el número total de "espanoleshombres", "espanolesmujeres",
# "extranjeroshombres" y "extranjerosmujeres" por cada barrio de cada distrito. Las columnas distrito y bariro deben ser
# las primeras en aparecer en el show. Los resultados deben estar ordenados en orden de más a menos según la columna
# "extranjerosmujeres" y desempatarán por la columna "extranjeroshombres".
import pyspark.sql.functions as F

numhabitantes_df = padron_df.groupBy("DESC_DISTRITO", "DESC_BARRIO")\
  .agg(
      F.sum("espanoleshombres").alias("total_espanoleshombres"),
      F.sum("espanolesmujeres").alias("total_espanolesmujeres"),
      F.sum("extranjeroshombres").alias("total_extranjeroshombres"),
      F.sum("extranjerosmujeres").alias("total_extranjerosmujeres")
  )\
  .orderBy(
      F.col("total_extranjerosmujeres").desc(),
      F.col("total_extranjeroshombres").desc()
  )\
  .show()

# 6.11 Elimina el registro en caché.
padron_df.unpersist()

# 6.12 Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO,
# otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" residentes en cada distrito de cada barrio.
# Únelo (con un join) con el DataFrame original a través de las columnas en común.

espanoleshombres_df = padron_df.groupBy("DESC_BARRIO", "DESC_DISTRITO")\
    .agg(F.sum("espanoleshombres").alias("total_espanoleshombres"))
espanoleshombres_df.show(10, truncate=False)

union_df = padron_df.join(
    espanoleshombres_df,
    padron_df.DESC_BARRIO == espanoleshombres_df.DESC_BARRIO
)
union_df.show(10, truncate=False)

# 6.13 Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....)).
from pyspark.sql import Window
padron_df = padron_df.withColumn("total_espanoleshombres", sum("espanoleshombres").over(Window.partitionBy("DESC_BARRIO", "DESC_DISTRITO")))

padron_df.show(10, truncate=False)

# 6.14 Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que contenga los valores
# totales (la suma de valores) de espanolesmujeres para cada distrito y en cada rango de edad (COD_EDAD_INT). Los distritos
# incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas.

tabla_contingencia = padron_df.filter(padron_df.DESC_DISTRITO.isin(["CENTRO", "BARAJAS", "RETIRO"]))\
                              .groupBy("COD_EDAD_INT")\
                              .pivot("DESC_DISTRITO")\
                              .agg(sum("espanolesmujeres"))\
                              .orderBy("COD_EDAD_INT")

tabla_contingencia.show(2, truncate=False)

# 6.15 Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje de la suma de
# "espanolesmujeres" en los tres distritos para cada rango de edad representa cada uno de los tres distritos.
# Debe estar redondeada a 2 decimales. Puedes imponerte la condición extra de no apoyarte en ninguna columna auxiliar
# creada para el caso.

totalmujeres = tabla_contingencia.CENTRO + tabla_contingencia.BARAJAS + tabla_contingencia.RETIRO

porcentaje_mujeres = tabla_contingencia.withColumn("MUJERES_CENTRO", round((tabla_contingencia.CENTRO / totalmujeres) * 100, 2))\
                                       .withColumn("MUJERES_BARAJAS", round((tabla_contingencia.BARAJAS / totalmujeres) * 100, 2))\
                                       .withColumn("PMUJERES_RETIRO", round((tabla_contingencia.RETIRO / totalmujeres) * 100, 2))

porcentaje_mujeres.show()

# 6.16 Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un directorio local.
# Consulta el directorio para ver la estructura de los ficheros y comprueba que es la esperada.

# padron_df.write.partitionBy("DESC_DISTRITO", "DESC_BARRIO").format("csv").mode("overwrite").save("C:/Users/julia.blanco/Desktop/repositorios/WebLogServersNasa/src/main/resources/padron_df.csv")

#6.17 Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el resultado anterior.
# padron_df.write.partitionBy("DESC_DISTRITO", "DESC_BARRIO").mode("overwrite").format("parquet").save("C:/Users/julia.blanco/Desktop/repositorios/WebLogServersNasa/src/main/resources/padron_df.parquet")