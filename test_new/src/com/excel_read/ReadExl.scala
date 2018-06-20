package com.excel_read

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

object ReadExl {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // get threshold
    val threshold = 2

    // read in text file and split each document into words
   // val tokenized = sc.textFile("C:/Users/gourabdatta/Documents/sample.xlsx").flatMap(_.split(" "))

    val outputSchema =
    StructType(
      Array(
        StructField("Row ID", StringType, nullable=false),
        StructField("Order ID", IntegerType, nullable=false),
        StructField("Order Date", DateType, nullable=false),
        StructField("Ship Date", DateType, nullable=false),
        StructField("Ship Mode", StringType, nullable=false),
        StructField("Customer ID", StringType, nullable=false),
        StructField("Customer Name", StringType),
        StructField("Segment", StringType),
        StructField("Country", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("Postal Code", StringType),
        StructField("Region", StringType),
        StructField("Product ID", StringType),
        StructField("Category", StringType),
        StructField("Sub-Category", StringType),
        StructField("Product Name", StringType),
        StructField("Sales", DoubleType),
        StructField("Quantity", DoubleType),
        StructField("Discount", DoubleType),
        StructField("Profit", DoubleType)
        
      )
    )
    val df = sqlContext.read
      .format("com.crealytics.spark.excel")
      .option("sheetName", "Orders") // Required
      .option("useHeader", "true") // Required
      .option("location", "C:/Users/gourabdatta/Documents/Sample_Superstore.xlsx")
      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      .option("inferSchema", "false") // Optional, default: false
      .option("addColorColumns", "false")
      .schema(outputSchema) // Optional, default: false
      .load("C:/Users/gourabdatta/Documents/Sample_Superstore.xlsx")
      
      df.printSchema()

    df.show()
    /*df.select("id", "name").filter(df("name") !== "biman").show
    val check1 = Check(df).isAlwaysNull("Name")
    print(check1)

    val markdownMd = new PrintStream(new File("report.md"))
    val markdownReporter = new MarkdownReporter(markdownMd)

    Runner.run(Seq(check1), Seq( markdownReporter))

    markdownMd.close()*/

  }

}