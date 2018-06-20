package com.excel_read
import scala.collection.JavaConverters._
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.Properties
import java.util

object Consumer {
  def main(args: Array[String]) {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "consumer-tutorial")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])
    import org.apache.kafka.clients.consumer.KafkaConsumer
    val kafkaConsumer = new KafkaConsumer[String, String](properties)
    kafkaConsumer.subscribe(util.Collections.singletonList("test"))

    while (true) {
      val results = kafkaConsumer.poll(2000).asScala
      for (data <- results) {
        // Do stuff
        println("---------------------")
        println(data.value())
        println(data.timestamp())

      }
    }
  }
}