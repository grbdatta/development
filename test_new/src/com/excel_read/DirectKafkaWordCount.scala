package com.excel_read

import org.apache.spark.SparkConf
import java.util.Properties
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
import com.dataquality.Execute
import org.apache.spark.SparkContext
import com.validity.OpenStreetMapUtils

object DirectKafkaWordCount extends java.io.Serializable {

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

    var sparkConf = new SparkConf().setAppName("testing").setMaster("local")

    val sc = new SparkContext(sparkConf)
    sparkConf.set("spark.driver.allowMultipleContexts", "true")
    val Array(brokers, topics) = args

    val spark: SparkSession = SparkSession.builder().appName("AppName").config("spark.master", "local").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))

    val schema = new StructType()
      .add(StructField("name", StringType, true))
      .add(StructField("performance_percent", DoubleType, true))
      .add(StructField("city", StringType, true))
      .add(StructField("profit", DoubleType, true))
      .add(StructField("curnt_time", StringType, true))

    //sparkConf.set("spark.driver.allowMultipleContexts","true")

    // Create direct kafka stream with brokers and topics
    val topicsSet = "test".split(",").toSet
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "test-consumer-group",
      "auto.offset.reset" -> "earliest")
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words
    //val lines = messages.window(Minutes(10)).map(l=>(l.value().toString()))
    val lines = messages.map(_.value)

    //lines.print()
    import spark.implicits._
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root")

    lines.foreachRDD { l =>

      //l.map(_.split(",")).foreach(println)

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      val df = spark.createDataFrame(l.map(_.split(",").to[List]).map(row), schema)

      val filteredDf = Execute.listOfValueValidity(df, List("city"), sqlContext,List("Kolkata"))
      
      //val fdf=df.select(OpenStreetMapUtils.getInstance.getCoordinates("city"))

      filteredDf._1.show(false)
      filteredDf._2.write.csv("C://Users//gourabdatta//Documents//error//"+System.currentTimeMillis())
      filteredDf._1.write.mode("append") jdbc ("jdbc:mysql://127.0.0.1:3306/test", "details", connectionProperties)

    }

    ssc.start()
    ssc.awaitTermination()
  }
  def row(line: List[String]): Row =
    Row(line(0), line(1).toDouble, line(2), line(3).toDouble, line(4).toString())

  //Row(line.map(l => l))

}