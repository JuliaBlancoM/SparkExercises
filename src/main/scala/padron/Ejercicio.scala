package padron

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Ejercicio {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LibroSpark")
      .master("local[2]")
      .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    val datos_padron = "src/main/resources/estadisticas202212.csv"

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .option("quote", "\"")
      .option("emptyValue", "0")
      .load(datos_padron)

    df.show(10, truncate = false)
    df.printSchema()



    val padron_df = df.select(df.schema.fields.map { field =>
      if (field.dataType == StringType) {
        trim(col(field.name)).alias(field.name)
      } else {
        col(field.name)
      }
    }: _*)

    padron_df.show(10, truncate = false)



  }

}
