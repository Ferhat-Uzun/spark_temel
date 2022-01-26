package SparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCountRdd {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark =SparkSession.builder()
      .appName("RDD").master("local[4]").config("spark-executor.memory","4g").config("spark-driver.memory","2g")
      .getOrCreate()

    val sc = spark.sparkContext

    val storyRDD= sc.textFile("/home/ferhat/Downloads/omer_seyfettin_forsa_hikaye.txt")
    println(storyRDD.count())

    val kelimeler= storyRDD.flatMap(satir => satir.split(" "))
    val kelimeSayisi= kelimeler.map(kelime=> (kelime,1)).reduceByKey((x,y) => x+y)
    println(kelimeSayisi.count())

    kelimeSayisi.take(10).foreach(println)

    val kelimeSayisi2 = kelimeSayisi.map(x => (x._2,x._1))

    kelimeSayisi2.take(10).foreach(println)


    println("\n ******** En Çok Tekrar Eden Kelimeler ********")
    kelimeSayisi2.sortByKey(false).take(20).foreach(println)
  }

}
