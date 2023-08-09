package nasa

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.sql.types._
object ejer1 {
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

    val hostPattern = "^(\\S+) "
    val timestampPattern = "\\[([\\w:/]+)\\s[\\-]\\d{4}\\]"
    val methodPattern = "\"(\\S+) "
    val resourcePattern = "\"\\S+ (\\S+)"
    val protocolPattern = "(\\S*)\" "
    val statusPattern = " (\\d{3}) "
    val sizePattern = " (\\S+)$"



    val pasoUnoDf = nasadf.select(regexp_extract(col("value"), hostPattern, 1).alias("host"),
      regexp_extract(col("value"), timestampPattern, 1).alias("timestamp"),
      regexp_extract(col("value"), methodPattern, 1).alias("method"),
      regexp_extract(col("value"), resourcePattern, 1).alias("resource"),
      regexp_extract(col("value"), protocolPattern, 1).alias("protocol"),
      regexp_extract(col("value"), statusPattern, 1).cast(IntegerType).alias("status"),
      regexp_extract(col("value"), sizePattern, 1).alias("size"))

    val pasoDosDf = pasoUnoDf.withColumn(
      "timestamp",
      to_timestamp(col("timestamp"), "dd/MMM/yyyy:HH:mm:ss")
    )

    val pasoTresDf = pasoDosDf.withColumn(
      "size",
      when(col("size") === "-", null).otherwise(col("size"))
    )

    val nasaDfFinal = pasoTresDf.withColumn(
      "size",
      col("size").cast(IntegerType)
    )

    nasaDfFinal.show()

    nasaDfFinal.write.format("txt").option("header", "true").save("src/main/resources/nasaDfFinal.txt")

    //Consulta 1
    nasaDfFinal.groupBy("protocol").count().show()

  }

}
