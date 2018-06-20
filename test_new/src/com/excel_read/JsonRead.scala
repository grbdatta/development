package com.excel_read

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql._
import org.json4s.JsonAST.JValue
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.explode

object JsonRead {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }
    System.setProperty("hadoop.home.dir", "C:\\hadoop");

    val Array(brokers, topics) = args

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local[*]"))
    val ssc = new StreamingContext(sc, Seconds(10))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create direct kafka stream with brokers and topics
    val topicsSet = "test".split(",").toSet
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "test-consumer-group")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words, count the words and print
    val lines = messages.map(_.value)

    //lines.print()

    lines.foreachRDD { l =>
      if (!l.isEmpty()) {
        val json = l.map(data => data.replaceAll(" +", "").trim() replaceAll ("\n", ""))
        val formattedJson = json.collect().reduce(_ + _)
        print(formattedJson)
        import sqlContext.implicits._

        val df = sqlContext.read.option("multiline", "true").option("mode", "PERMISSIVE").json(sc.parallelize(Seq(formattedJson)))
        df.show(false)
        df.printSchema
        //df.withColumn("medi", explode($"medications")).select().show()
      }

    }

    //lines.saveAsTextFiles("C:\\Users\\gourabdatta\\Documents\\kafka_msg_write\\" + System.currentTimeMillis())

    ssc.start()
    ssc.awaitTermination()
  }
  

}