package chapter12

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.SparkSession

/**
  * 代码清单12-5
  * Created by 朱小厮 on 2019-03-04.
  */
class StructuredStreamingWithKafka {
  object StructuredStreamingWithKafka {
    val brokerList = "localhost:9092" //Kafka 集群的地址
    val topic = "topic-spark" //订阅的主题
    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder.master("local[2]")
        .appName("StructuredStreamingWithKafka").getOrCreate()

      import spark.implicits._

      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",brokerList)
        .option("subscribe",topic)
        .load()

      val ds = df.selectExpr("CAST(value AS STRING)").as[String]

      val words=ds.flatMap(_.split("")).groupBy("value").count()
      val query = words.writeStream
        .outputMode("complete")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .format("console")
        .start()
      query.awaitTermination()
    } }
}
