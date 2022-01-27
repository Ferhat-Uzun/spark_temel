package SparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RDDOlusturmak {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("RDD")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    val sc = spark.sparkContext

    //Listten rdd olusturma
    val rddList=sc.makeRDD(List(1,2,3,4,5,6,7,8,9))
    rddList.take(5).foreach(println)

    //Tupleden rdd olusturma
    val rddTupleList=sc.makeRDD(List((1,2,3),(4,5),(6,7,8),9))
    rddTupleList.take(5 ).foreach(println)
  }
}
