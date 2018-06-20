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

object LoadInRDD {
  
   def main(args: Array[String]) {
    // create Spark context with Spark configuration
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    System.setProperty("hadoop.home.dir", "C:\\hadoop");

    var sparkConf = new SparkConf().setAppName("testing").setMaster("local")

    val sc = new SparkContext(sparkConf)
    
    val data=sc.textFile("C:/Users/gourabdatta/Documents/account_details.txt", 1)
    
    data.map(f=>(f.split(",")(0),f.split(",")(1).toInt,f.split(",")(2).toDouble)).groupBy(f=>f._1)
    .mapValues{_.toList.sortBy{(a)=>a._2}.scanLeft(("",0,0.0,0.0)){(a,b)=>(b._1,b._2,b._3,b._3+a._4)}.tail}
    .flatMapValues(f=>f).values.foreach{
      f=>
        println(f._1+","+f._2+","+f._3+","+f._4)}
    
    //data.foreach(println)
    
   }
}