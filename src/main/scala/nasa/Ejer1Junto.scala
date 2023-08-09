package nasa

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.sql.types._
object Ejer1Junto {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession
      .builder
      .appName("LibroSpark")
      .master("local[2]")
      .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    val nasadf = spark.read.text("src/main/resources/access_log_Jul95")
    nasadf.printSchema()

    nasadf.show(3, truncate=false)

    val regExp = "^(\\S+) \\S+ \\S+ \\[([\\w:/]+)\\s[\\-]\\d{4}\\] \"(\\S+) (\\S+) (\\w+/\\S*)\" (\\d{3}) (\\S+)$"

    val nasaDfFinal = nasadf.select(
      regexp_extract(col("value"), regExp, 1).alias("host"),
      regexp_extract(col("value"), regExp, 2).alias("timestamp"),
      when(regexp_extract(col("value"), regExp, 3) === "", "Unknown")
        .otherwise(regexp_extract(col("value"), regExp, 3)).alias("method"),
      regexp_extract(col("value"), regExp, 4).alias("resource"),
      when(regexp_extract(col("value"), regExp, 5) === "", "Unknown")
        .otherwise(
          when(regexp_extract(col("value"), regExp, 5) === "HTTP/V1.0", "HTTP/1.0")
            .otherwise(regexp_extract(col("value"), regExp, 5))
        ).alias("protocol"),
      regexp_extract(col("value"), regExp, 6).cast(IntegerType).alias("status"),
      when(regexp_extract(col("value"), regExp, 7) === "-", null)
        .otherwise(regexp_extract(col("value"), regExp, 7).cast(IntegerType)).alias("size")
    )
    .withColumn("timestamp", to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss"))


    nasaDfFinal.show()

    //consultas
    //¿Cuáles son los distintos protocolos web utilizados? Agrúpalos.

    nasaDfFinal.groupBy("protocol").count().show()

    //¿Cuáles son los códigos de estado más comunes en la web? Agrúpalos y ordénalos para ver cuál es el más común.
    nasaDfFinal.groupBy("status")
      .count()
      .orderBy(desc("count"))
      .show()

    //¿Y los métodos de petición (verbos) más utilizados?
    nasaDfFinal.groupBy("method")
      .count()
      .orderBy(desc("count"))
      .show()

    //¿Qué recurso tuvo la mayor transferencia de Bytes?
    nasaDfFinal.groupBy("resource")
      .agg(sum("size").alias("totalSize"))
      .orderBy(desc("totalSize"))
      .show(1,truncate = false)

    //Además, queremos saber que recurso de nuestra web es el que más tráfico recibe. Es decir, el recurso con más registros en nuestro log.
    nasaDfFinal.groupBy("resource")
      .count()
      .orderBy(desc("count"))
      .show(1, truncate = false)

    //¿Qué día la web recibió más tráfico?
    val nasaWithDate = nasaDfFinal.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
      .groupBy("date")
      .count()
      .orderBy(desc("count"))
    nasaWithDate.show(10)

    //¿Cuáles son los hosts más frecuentes?
    nasaDfFinal
      .groupBy("host")
      .count()
      .orderBy(desc("count"))
      .show()

    //¿A qué horas se produce el mayor tráfico de la web?

    val nasaWithHours = nasaDfFinal
      .withColumn("hour", hour(col("timestamp")))
      .groupBy("hour")
      .count()
      .orderBy(desc("count"))
    nasaWithHours.show(10)

    //¿Cuál es el número de errores 404 que ha habido cada día?
    nasaDfFinal.filter(col("status") === 404)
      .groupBy(date_format(col("timestamp"), "yyyy-MM-dd").alias("day"))
      .count()
      .orderBy(desc("count"))
      .show()

    val nasaErrorDate = nasaDfFinal.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
      .filter(col("status") === 404)
      .groupBy("date")
      .count()
      .withColumnRenamed("count", "errors")
      .orderBy(desc("errors"))
    nasaErrorDate.show()



  }

}
