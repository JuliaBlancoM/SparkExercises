package padron

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ejercicioPadron {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LibroSpark")
      .master("local[2]")
      .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    val datosPadron = "src/main/resources/estadisticas202212.csv"

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .option("quote", "\"")
      .option("emptyValue", "0")
      .load(datosPadron)

    df.show(10, truncate = false)
    df.printSchema()



    val padronDf = df.select(df.schema.fields.map { field =>
      if (field.dataType == StringType) {
        trim(col(field.name)).alias(field.name)
      } else {
        col(field.name)
      }
    }: _*)

    padronDf.show(10, truncate = false)

// 6.3 Enumera todos los barrios diferentes:
    val barriosDf = padronDf
      .select("COD_BARRIO", "DESC_BARRIO")
      .distinct()
      .orderBy("COD_BARRIO")
    barriosDf.show(30, false)

//6.4 Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios diferentes que hay.
    padronDf.createOrReplaceTempView("padron")
    val numero_barrios = spark.sql("SELECT COUNT(DISTINCT DESC_BARRIO) FROM padron")
    numero_barrios.show()

//6.5 Crea una nueva columna que muestre la longitud de los campos de la columna DESC_DISTRITO y que se llame "longitud".
    val padronLongDf = padronDf.withColumn("longitud", length(col("DESC_DISTRITO")))
    padronLongDf.select("DESC_DISTRITO", "longitud").show(30, false)

//6.6 Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.
    val padronDfcon5 = padronDf.withColumn("VALOR_5", lit(5))
    padronDfcon5.show(5, false)

//6.7 Borra esta columna.
    padronDfcon5.drop(col("VALOR_5"))
    padronDfcon5.show(5, false)

//DUDA: ESTAS DOS POSIBILIDADES NO FUNCIONAN:
    //La primera es poner directamente esto (es decir, no definir nueva variable): padronDf.withColumn("VALOR_5", lit(5))
    //La segunda es borrar la columna del df padronDfcon5 pero de esta forma: df.drop(df("VALOR_5"))

//6.8 Particiona el DataFrame por las variables DESC_DISTRITO Y DESC_BARRIO.
/*    padronDf.write.format("csv")
      .option("header", "true")
      .partitionBy("DESC_DISTRITO", "DESC_BARRIO")
      .save("C:/Users/julia.blanco/Desktop/repositorios/WebLogServersNasa/src/main/resources/padron_partition2.csv")
*/
    val padronDfParticion = padronDf.repartition(col("DESC_DISTRITO"), col("DESC_BARRIO"))

//6.9 Almacénalo en caché
    padronDfParticion.cache()

//6.10 Lanza una consulta contra el DF resultante en la que muestre el número total de "espanoleshombres", "espanolesmujeres",
    // "extranjeroshombres" y "extranjerosmujeres" por cada barrio de cada distrito. Las columnas distrito y bariro
    // deben ser las primeras en aparecer en el show. Los resultados deben estar ordenados en orden de más a menos según
    // la columna "extranjerosmujeres" y desempatarán por la columna "extranjeroshombres".

    val numhabitantesDf = padronDfParticion.groupBy("DESC_DISTRITO", "DESC_BARRIO")
      .agg(
        sum("ESPANOLESHOMBRES").alias("total_espanoleshombres"),
        sum("ESPANOLESMUJERES").alias("total_espanolesmujeres"),
        sum("EXTRANJEROSHOMBRES").alias("total_extranjeroshombres"),
        sum("EXTRANJEROSMUJERES").alias("total_extranjerosmujeres")
      )
      .orderBy(desc("total_extranjerosmujeres"), desc("total_extranjeroshombres"))
    numhabitantesDf.show(10, false)

    //6.11 Elimina el registro en caché.
    padronDfParticion.unpersist()

    //6.12 Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO,
    // otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" residentes en cada distrito de cada barrio.
    // Únelo (con un join) con el DataFrame original a través de las columnas en común.

    val espanoleshombresDf = padronDf
      .groupBy("DESC_BARRIO", "DESC_DISTRITO")
      .agg(sum("espanoleshombres").alias("total_espanoleshombres"))

    espanoleshombresDf.show(10, false)
    val unionDf = padronDf.join(
      espanoleshombresDf,
      padronDf("DESC_BARRIO").equalTo(espanoleshombresDf("DESC_BARRIO"))
    )

    unionDf.show(10, false)

  }

}
