package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object App {

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("PrimerProyectoSpark")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val rdd = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
    val rddCollect = rdd.collect()
    println("Number of Partitions: " + rdd.getNumPartitions)
    println("Action: First element: " + rdd.first())
    println("Action: RDD converted to Array[Int] : ")
    rddCollect.foreach(println)


    import spark.implicits._

    val df = spark.createDataFrame(List(("A",1),("B",2),("C",3)))
    val df2 = Seq((1,"Uno"),(2,"Dos"),(3,"Tres")).toDF("Numero","String")
    df2.show()

    df.withColumn("Test",lit("AAAA")).show()

    val df3 = spark.read.text("src/main/resources/el_quijote.txt")
    df3.show()
    //df.write.csv("src/resources/dfPruebas_")



 

  }

}
