package FullTextSearch

import org.apache.spark.sql.SparkSession
import org.zouzias.spark.lucenerdd.LuceneRDD

/**
 * @author ${user.name}
 */
object App {
  

  def main(args : Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()       //.config("spark.some.config.option", "some-value")
//    val elem = Array("fear", "death", "water", "fire", "house")
//      .zipWithIndex.map{ case (str, index) =>
//      FavoriteCaseClass(str, index, 10L, 12.3F, s"${str}@gmail.com")}
//    val rdd = sparkSession.sparkContext.parallelize(elem)
//    val df=sparkSession.createDataFrame(rdd)
    val df = sparkSession.read.json("source2/")
    df.printSchema()
    val luceneRDD = LuceneRDD(df)

    val results = luceneRDD.termQuery("name", "justin", 3)
    results.collect().foreach(println)
//    val seq = Seq("1","2","3")
//    println(seq.foldLeft(Set[String]())((set, i) => set +i ))

  }

}
case class FavoriteCaseClass(name: String, age: Int, myLong: Long, myFloat: Float, email: String)