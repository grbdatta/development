package com.excel_read
import java.util.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.kafka.common.serialization.StringDeserializer

object KafkaTest {
  def main(args: Array[String]) {
    import org.apache.spark.streaming._
    import org.apache.spark.SparkContext
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local[*]"))
    val ssc = new StreamingContext(sc, Seconds(2))
    val preferredHosts = LocationStrategies.PreferConsistent
    val topics = List("test")
    //import org.apache.kafka.common.serialization.StringDeserializer
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-notes",
      "auto.offset.reset" -> "earliest")
    import org.apache.kafka.common.TopicPartition
    val offsets = Map(new TopicPartition("test", 0) -> 2L)

    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets))

    dstream.foreachRDD(rdd => {
      for (item <- rdd.collect().toArray) {
        println("result::"+item);
      }
    })

    println("----------------==========---------")
    ssc.start()
    // the above code is printing out topic details every 5 seconds
    // until you stop it.

    ssc.stop()
  }
}