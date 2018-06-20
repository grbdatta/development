package com.excel_read

import java.util.Properties

import kafka.producer._

import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.util.RawTextHelper
import java.util.Calendar


object KafkaProducerCode {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)
    var range = 0
    // Send some messages
    while (true) {
      val messages = (1 to 3).map { messageNum =>
        //val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
        val master_list = RandomDetails.generateInfo()
        val str = master_list.map(x => x(range)).mkString(",")
        range = range + 1
        if (range > 12) {
          range = 0
        }
        new KeyedMessage[String, String](topic, str+","+Calendar.getInstance.getTime)
      }.toArray

      producer.send(messages: _*)
      Thread.sleep(1000)
    }
  }

}