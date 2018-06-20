package com.load.data

import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import de.frosner.ddq._
import de.frosner.ddq.core.Check
import de.frosner.ddq.reporters.ConsoleReporter
import java.io.{ PrintStream, File }
import de.frosner.ddq.reporters.MarkdownReporter
import de.frosner.ddq.core.Runner
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import jdk.nashorn.internal.codegen.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object Load {
 
  def load(spark:SparkSession,location:String):DataFrame={
    
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(location)
    //df.show(false)
    df
  }
  def load(spark: SparkSession, location: String,sheetName:String): DataFrame = {

    val df = spark.read.format("com.crealytics.spark.excel")
      .option("sheetName", sheetName) // Required
      .option("useHeader", "true") // Required
      .option("location", location)
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option("inferSchema", "true") // Optional, default: false
      .option("addColorColumns", "false")
      .load(location)

    df
  }
  
  
}