package chapter12

import org.apache.spark.sql.SparkSession

/**
  * 代码清单12-4
  * Created by 朱小厮 on 2019-03-04.
  */
object StructuredStreamingWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[2]") .appName("StructuredStreamingWordCount") .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load()
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
