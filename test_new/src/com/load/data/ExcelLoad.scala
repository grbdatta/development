package com.load.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object ExcelLoad {

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