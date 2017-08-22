import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark.SparkConf

object KafkaWordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: KafkaWordCount    ")
      System.exit(1)
    }

    //val Array(zkQuorum, topics,numThreads) = args
    val Array(zkQuorum,topics) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc =  new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> zkQuorum,//"master:9092,slave1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array(topics), kafkaParams)
    )

    //stream.map(record => (record.key, record.value))



    //val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap
    //val lines = KafkaUtils.createDirectStream(ssc, zkQuorum, group, topicpMap).map(_._2)
    val lines = stream.map(record => record.value)
    val words = lines//lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}


