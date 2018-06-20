package com.load.data

import org.apache.spark.SparkContext
import java.security.MessageDigest
import java.math.BigInteger
import org.apache.spark.sql.functions._
import com.dataquality.Rules.checkSumGen

import org.apache.log4j.Logger

import org.apache.log4j.Level

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
import com.dataquality.Rules._
import com.dataquality.Execute
import com.dataquality.Report
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import com.use.udaf.SpecialCount
import scala.util.Properties
import java.util.Properties

object GenericDataLoad {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    System.setProperty("hadoop.home.dir", "C:\\hadoop");

    var sparkConf = new SparkConf().setAppName("testing").setMaster("local")

    val sc = new SparkContext(sparkConf)
    sparkConf.set("spark.driver.allowMultipleContexts", "true")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")

    //val df = Load.load(spark, "C:\\Users\\gourabdatta\\Documents\\test.txt")
    //val inc_df = Load.load(spark, "C:\\Users\\gourabdatta\\Documents\\emp_inc.txt")
    //val df_map = Load.load(spark, "C:\\Users\\gourabdatta\\Documents\\map.txt")
    //val df = Load.load(spark, "C:/Users/gourabdatta/Documents/Sample_Superstore.xlsx","Orders")
    //val validity=sqlContext.udf.register("validity", valid)

    //Execute.checkValidity(df,"Last_name",sqlContext).show()
    //Execute.phoneNumberValidity(df,"phone_number",5,sqlContext).show()

    println("--------------------")
    //Execute.numberFormatValidity(df,"Last_name",sqlContext).show()
    //Execute.listOfValueValidity(df, List("Last_name","phone_number"), sqlContext, List("datta","saha")).show
    //Execute.isNullValidity(df, "Last_name", sqlContext).
    //Execute.isNullValidity(df, List("First_name"), sqlContext).show
    //Execute.numberFormatValidity(df, List("*"), sqlContext).show
    // df_map.show()
    //println("total count:" + df.count())
    //df.show
    /*val finalDf = (Execute.isDateValidity(df, List("Date"), "dd/mm/yyyy", sqlContext)as 'f)
    .join(df_map as 'm ,$"m.Product_Code"=== $"f.Product_Code","inner")
    .groupBy("f.First_name","f.Last_name").agg(min("m.Product_Code")).show*/
    //val refinedDf=Execute.ageValidity(df, "Date","30/04/2018", 10, sqlContext)

    //val refined1Df=Execute.phoneNumberValidity(df, List("phone_number"), 6, sqlContext)
    //println("valid phone number count:"+refined1Df._2.count)
    //val refined2Df = Execute isNullValidity(df, List("*"), sqlContext)
    //val refined3Df = Execute isDateValidity(df, List("Date"), "dd/mm/yyyy", sqlContext)
    //val refined4Df=Execute.emailValidity(df, List("mail"), sqlContext)
    //val entireReport = (refined2Df._2).union(refined3Df._2)

    //println("total error record count:"+entireReport.count())
    // refined1Df._1.write.format("json").save("C:\\Users\\gourabdatta\\Documents\\error\\"+System.currentTimeMillis())

    //entireReport.write.format("json").save("C:\\Users\\gourabdatta\\Documents\\error\\" + System.currentTimeMillis())
    //entireReport.show(false)

    //entireReport.write.csv("C:\\Users\\gourabdatta\\Documents\\error\\"+System.currentTimeMillis())
    //refined3Df._1.show(false)
    /*entireReport.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", "|")
      .save("C:\\Users\\gourabdatta\\Documents\\error\\" + System.currentTimeMillis())*/

    //Report.generateReportDQ(sqlContext, entireReport, "C:\\Users\\gourabdatta\\Documents\\error\\", "#", true)

    //val report=df.except(refinedDf).withColumn("Reason_of_elimination",lit("invalid date")).coalesce(1)
    //report.write.csv("C:\\Users\\gourabdatta\\Documents\\error\\"+System.currentTimeMillis())

    //Execute.

    // df.except(Execute isDateValidity (df, List("Date"), "yyyy/mm/dd", sqlContext)).withColumn("Reason", lit("invalid date")).show

    //val window = Window.partitionBy("name").orderBy("day")
    //val df_diff=df.withColumn("previous_km_travel", (lag($"km_travel", 1, 0)).over(window))
    //df_diff.registerTempTable("t")
    //sqlContext.sql("SELECT name, km_travel, sum(km_travel) OVER (ORDER BY km_travel group by name) as CumSrome FROM t").show
    // df.join(df_diff,df("name")===df_diff("name")).withColumn("Cumulative",df("km_travel")+df_diff("previous_km_travel")).show
    /* inc_df.show

    val primary_key_col_list=List("emp_id")

    val joinExprs = primary_key_col_list.map { case (c1) => df(c1) === inc_df(c1) }.reduce(_ && _)
    val joineddf = df.join(inc_df, joinExprs, "outer")
    val changedExprs = primary_key_col_list.map { case (c1) => (inc_df(c1).isNotNull && df(c1).isNotNull) || (df(c1).isNull) }.reduce(_ && _)
    val changedDataDf = joineddf.filter(changedExprs).select(inc_df("*"))
    println("Changed Data")
    changedDataDf.show

    val unchangedExprs = primary_key_col_list.map { case c1 => (inc_df(c1).isNull) }.reduce(_ && _)
    val unchangedDataDf = joineddf.filter(unchangedExprs).select(df("*"))
    println("Unchanged Data")
    unchangedDataDf.show*/

    //val combined_df=unchangedDataDf.union(changedDataDf)
    //println("Final Data")
    //combined_df.show

    /*val schema = StructType(
        StructField("id", StringType, false) ::
    StructField("Names", StringType, false) ::
    StructField("Values", DoubleType, false) :: Nil)

    val row = Array((Row(1,"nice"),13.4),(Row(1, "nice"),4.5))

    val rdd=sc.parallelize(row).map(x=>Row(x._1.get(0).toString(),x._1.get(1),x._2))
    rdd.collect().foreach(println)
    val df1 = sqlContext.createDataFrame(rdd, schema)
    df1.show*/

    //df.show
    //val specialC = new SpecialCount()
    //df.agg(specialC(df("profit")))
    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root")

    val data_from_rdbms = sqlContext.read.jdbc("jdbc:mysql://127.0.0.1:3306/test", "username", connectionProperties)
    val refinedDF_fromRDBMS = Execute.checksumValidity(data_from_rdbms, List("*"), sqlContext)
    println("Data from RDBMS")
    refinedDF_fromRDBMS.show(false)

    val data_from_hive = sqlContext.read.jdbc("jdbc:mysql://127.0.0.1:3306/test", "username_rep", connectionProperties)
    val refinedDF_fromhive = Execute.checksumValidity(data_from_hive, List("*"), sqlContext)
    println("Data from hive")
    refinedDF_fromhive.show(false)

    val mismatchData = refinedDF_fromRDBMS.select("checksum").except(refinedDF_fromhive.select("checksum"))
    
    println("checksum mismatch between RDBMS and HIVE data")
    mismatchData.show(false)
    
    println("Data which is mismatching checksum value")
    mismatchData.join(refinedDF_fromRDBMS,mismatchData("checksum")===refinedDF_fromRDBMS("checksum")).select(refinedDF_fromRDBMS("*")).show(false)

  }

}