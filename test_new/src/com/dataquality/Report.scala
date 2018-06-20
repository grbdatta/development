package com.dataquality

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame

object Report {
  
  def generateReportDQ(sqlContext:SQLContext,errorDataFrame:DataFrame,reportLocation:String):Unit={
   
    errorDataFrame.write.csv(reportLocation+"\\" + System.currentTimeMillis())
  }
    def generateReportDQ(sqlContext:SQLContext,errorDataFrame:DataFrame,reportLocation:String,delimiter:String,headerWanted:Boolean):Unit={
   
    errorDataFrame.write
      .format("com.databricks.spark.csv")
      .option("header", headerWanted.toString())
      .option("delimiter", delimiter)
      .save(reportLocation+"\\" + System.currentTimeMillis())
  }
  def generateReportDQ(sqlContext:SQLContext,errorDataFrame:DataFrame,reportLocation:String,extension:String):Unit={
   
    errorDataFrame.write.format(extension).save(reportLocation+"\\" + System.currentTimeMillis())
  }
  
}