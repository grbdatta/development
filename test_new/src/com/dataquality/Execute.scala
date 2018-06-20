package com.dataquality

import org.apache.spark.sql.DataFrame
import com.dataquality.Rules._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.exceptions.InvalidColumnException

object Execute {

  /*def checkValidity(df: DataFrame, columnName: List[String], sqlContext: SQLContext) = {
    val validity = sqlContext.udf.register("validity", valid)
    if (columnName.length == 1 && columnName(0).equalsIgnoreCase("*")) {
      val expressionBuilder = df.columns.map { case (c1) => (validity(df(c1)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      refinedDf
    } else {
      val expressionBuilder = columnName.map { case (c1) => (validity(df(c1)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      refinedDf
    }

  }*/
  /*
   * @Description
   * Validate a phone number country wise.
   *
   * @Function Definition
   * def phoneNumberValidity(df: DataFrame, columnName: List[String], lengthOfNumber: Int, sqlContext: SQLContext, phoneNumberStartsWith: String) = {
   *
   * @param
   * df => DataFrame on which specific value check will be performed.
   * columnName => pass all the possible columns for which system is expecting phone number.
   * lengthOfNumber => pass length of phone number which is country specific.
   * sqlContext => using the same instance of sqlContext from the driver program(GenericDataLoad.scala).
   * phoneNumberStartsWith => pass country code for specific country.
   *
   * @return
   * (Dataset[Row], Dataset[Row]) => 2 DataSet containing refined data as well as report of error data.
   */
  def phoneNumberValidity(df: DataFrame, columnName: List[String], lengthOfNumber: Int, sqlContext: SQLContext, phoneNumberStartsWith: String) = {
    val validity = sqlContext.udf.register("validity", phoneNumberCheck)
    if (columnName.length == 1 && columnName(0).equalsIgnoreCase("*")) {
      val expressionBuilder = df.columns.map { case (c1) => (validity(df(c1), lit(lengthOfNumber), lit(phoneNumberStartsWith)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.select(df("*")).where(!expressionBuilder).withColumn("Reason_of_elimination", lit("not a valid phone number as expected in the columns " + columnName.mkString(","))).coalesce(1)
      //val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("not a valid phone number as expected in the columns " + df.columns.mkString(","))).coalesce(1)
      (refinedDf, report)
    } else {
      val expressionBuilder = columnName.map { case (c1) => (validity(df(c1), lit(lengthOfNumber), lit(phoneNumberStartsWith)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.select(df("*")).where(!expressionBuilder).withColumn("Reason_of_elimination", lit("not a valid phone number as expected in the columns " + columnName.mkString(","))).coalesce(1)
      //val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("not a valid phone number as expected in the columns " + columnName.mkString(","))).coalesce(1)
      (refinedDf, report)
    }

  }
  /*
   * @Description
   * Check the format of data for  numeric column/columns.
   *
   * @Function Definition
   * def numberFormatValidity(df: DataFrame, columnName: List[String], sqlContext: SQLContext)
   *
   * @param
   * df => DataFrame on which specific value check will be performed.
   * columnName => pass all the possible columns for which system is expecting numeric value .
   * sqlContext => using the same instance of sqlContext from the driver program(GenericDataLoad.scala).
   *
   * @return
   * (Dataset[Row], Dataset[Row]) => 2 DataSet containing refined data as well as report of error data.
   */
  def numberFormatValidity(df: DataFrame, columnName: List[String], sqlContext: SQLContext) = {
    val validity = sqlContext.udf.register("validity", numberFormatCheck)
    if (columnName.length == 1 && columnName(0).equalsIgnoreCase("*")) {
      val expressionBuilder = df.columns.map { case (c1) => (validity(df(c1)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Number format invalid in columns " + df.columns.mkString(","))).coalesce(1)
      (refinedDf, report)
    } else {
      val expressionBuilder = columnName.map { case (c1) => (validity(df(c1)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Number format invalid in columns " + columnName.mkString(","))).coalesce(1)
      (refinedDf, report)
    }

  }
  /*
   * @Description
   * Check the column values should be between upper limit and lower limit mentioned while calling the function.
   *
   * @Function Definition
   * def rangeValidity(df: DataFrame, columnName: List[String], upperLimit: BigDecimal, lowerLimit: BigDecimal, sqlContext: SQLContext) = {
   *
   * @param
   * df => DataFrame on which specific value check will be performed.
   * columnName => pass all the possible columns for which system is expecting specific value .
   * upperLimit => pass upper limit for the column/columns.
   * lowerLimit => pass lower limit for the column/columns
   * sqlContext => using the same instance of sqlContext from the driver program(GenericDataLoad.scala).
   *
   * @return
   * (Dataset[Row], Dataset[Row]) => 2 DataSet containing refined data as well as report of error data.
   */
  def rangeValidity(df: DataFrame, columnName: List[String], upperLimit: BigDecimal, lowerLimit: BigDecimal, sqlContext: SQLContext) = {
    val validity = sqlContext.udf.register("validity", rangeCheck)
    if (columnName.length == 1 && columnName(0).equalsIgnoreCase("*")) {
      val expressionBuilder = df.columns.map { case (c1) => (validity(df(c1), lit(upperLimit), lit(lowerLimit)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("value is out of range in columns " + df.columns.mkString(","))).coalesce(1)
      (refinedDf, report)
    } else {
      val expressionBuilder = columnName.map { case (c1) => (validity(df(c1), lit(upperLimit), lit(lowerLimit)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("value is out of range in columns " + columnName.mkString(","))).coalesce(1)
      (refinedDf, report)
    }

  }
  /*
   * @Description
   * Check the column values should contain the between passed value.
   *
   * @Function Definition
   * def listOfValueValidity(df: DataFrame, columnName: List[String], sqlContext: SQLContext, values: List[String])
   *
   * @param
   * df => DataFrame on which specific value check will be performed.
   * columnName => pass all the possible string columns for which system is expecting specific value .
   * sqlContext => using the same instance of sqlContext from the driver program(GenericDataLoad.scala).
   * values => Pass all the possible list of values.
   *
   * @return
   * (Dataset[Row], Dataset[Row]) => 2 DataSet containing refined data as well as report of error data.
   */

  def listOfValueValidity(df: DataFrame, columnName: List[String], sqlContext: SQLContext, values: List[String]) = {

    var name_of_value = values.mkString(",")
    val validity = sqlContext.udf.register("validity", listOfValuesCheck)
    if (columnName.length == 1 && columnName(0).equalsIgnoreCase("*")) {
      val expressionBuilder = df.columns.map { case (c1) => (validity(df(c1), lit(name_of_value)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Values mentioned not matching in columns " + df.columns.mkString(",") + " values")).coalesce(1)
      (refinedDf, report)
    } else {
      val expressionBuilder = columnName.map { case (c1) => (validity(df(c1), lit(name_of_value)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Values mentioned not matching in columns " + columnName.mkString(",") + " values")).coalesce(1)
      
      (refinedDf, report)
    }
  }
  
  
  /*
   * @Description
   * Eliminate the null value records from the data set.
   *
   * @Function Definition
   * def isNullValidity(df: DataFrame, columnName: List[String], sqlContext: SQLContext)
   *
   * @param
   * df => DataFrame on which null values check will be performed.
   * columnName => pass all the possible string columns for which system is expecting null values .
   * sqlContext => using the same instance of sqlContext from the driver program(GenericDataLoad.scala).
   *
   * @return
   * (Dataset[Row], Dataset[Row]) => 2 DataSet containing refined data as well as report of error data.
   */
  def isNullValidity(df: DataFrame, columnName: List[String], sqlContext: SQLContext) = {

    try {
      val validity = sqlContext.udf.register("validity", isNull)
      if (columnName.length == 1 && columnName(0).equalsIgnoreCase("*")) {
        val expressionBuilder = df.columns.map { case (c1) => (validity(df(c1)) !== false) }.reduce(_ && _)
        val refinedDf = df.select(df("*")).where(expressionBuilder)
        val report = df.select(df("*")).where(!expressionBuilder).withColumn("Reason_of_elimination", lit("Null values in some of the columns in " + df.columns.mkString(","))).coalesce(1)
        //val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Null values in some of the column in " + df.columns.mkString(","))).coalesce(1)
        (refinedDf, report)
      } else {
        val expressionBuilder = columnName.map { case (c1) => (validity(df(c1)) !== false) }.reduce(_ && _)
        val refinedDf = df.select(df("*")).where(expressionBuilder)
        val report = df.select(df("*")).where(!expressionBuilder).withColumn("Reason_of_elimination", lit("Null values in some of the columns in " + columnName.mkString(","))).coalesce(1)
        // val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Null values in some of the columns in " + columnName.mkString(","))).coalesce(1)
        (refinedDf, report)
      }
    } catch {
      case ex: Exception => {
        throw new InvalidColumnException("" + ex.getMessage)
        null
      }
    }
  }
  /*
   * @Description
   * Find out the valid date format from the data set.
   *
   * @Function Definition
   * def isDateValidity(df: DataFrame, columnName: List[String], inputDateFormat: String, sqlContext: SQLContext)
   *
   * @param
   * df => DataFrame on which date format validity will be performed.
   * columnName => pass all the possible date columns.
   * inputDateFormat => if the system is expecting some predefined date format in the data set to streamline the process.
   * sqlContext => using the same instance of sqlContext from the driver program(GenericDataLoad.scala).
   *
   * @return
   * (Dataset[Row], Dataset[Row]) => 2 DataSet containing refined data as well as report of error data.
   */

  def isDateValidity(df: DataFrame, columnName: List[String], inputDateFormat: String, sqlContext: SQLContext) = {

    val validity = sqlContext.udf.register("validity", dateFormatCheck)
    if (columnName.length == 1 && columnName(0).equalsIgnoreCase("*")) {
      val expressionBuilder = df.columns.map { case (c1) => (validity(df(c1), lit(inputDateFormat)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.select(df("*")).where(!expressionBuilder).withColumn("Reason_of_elimination", lit("Not Valid Date in some of the columns in " + df.columns.mkString(","))).coalesce(1)
      //val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Not Valid Date in some of the columns in " + df.columns.mkString(","))).coalesce(1)
      (refinedDf, report)

    } else {
      val expressionBuilder = columnName.map { case (c1) => (validity(df(c1), lit(inputDateFormat)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.select(df("*")).where(!expressionBuilder).withColumn("Reason_of_elimination", lit("Not Valid Date in some of the columns in " + columnName.mkString(","))).coalesce(1)
      //val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Not Valid Date in some of the columns in " + columnName.mkString(","))).coalesce(1)
      (refinedDf, report)
    }
  }
  /*
   * @Description
   * Find out the age from the date of birth or it can be used for multiple way like it gives the difference in years from 2 dates.
   *
   * @Function Definition
   * def ageValidity(df: DataFrame, fromDate: String, toDate: String, ageLimit: Int, sqlContext: SQLContext)
   *
   * @param
   * df => DataFrame on which age validity will be performed.
   * fromDate => from date has to passed
   * toDate => to date has to passed(for inferring age system date should be passed.)
   * sqlContext => using the same instance of sqlContext from the driver program(GenericDataLoad.scala).
   *
   * @return
   * (Dataset[Row], Dataset[Row]) => 2 DataSet containing refined data as well as report of error data.
   *
   */

  def ageValidity(df: DataFrame, fromDate: String, toDate: String, ageLimit: Int, sqlContext: SQLContext) = {
    sqlContext.setConf("spark.sql.shuffle.partitions", "4")
    val validity = sqlContext.udf.register("validity", validAgeCheck)
    val refinedDf = df.select(df("*")).where(validity(df(fromDate), lit(toDate), lit(ageLimit)) !== false)
    val errorDf = df.select(df("*")).where(validity(df(fromDate), lit(toDate), lit(ageLimit)) === false)
    val report = errorDf.withColumn("Reason_of_elimination", lit("Age limit exceeding in " + fromDate + " and " + toDate)).coalesce(1)
    //report.write.csv("C:\\Users\\gourabdatta\\Documents\\error\\"+System.currentTimeMillis())
    (refinedDf, report)

  }
  /*
   * @Description
   * Valid Email id check.Performed on list of email Id columns or all the columns in a table if the table contains only emailId .
   *
   * @Function Definition
   * def IpValidity(df: DataFrame, emailIdColumns: List[String], sqlContext: SQLContext)
   *
   * @param
   * df => DataFrame on which email id validity will be performed.
   * emailIdColumns=> columns in which email Id are expected.
   * sqlContext => using the same instance of sqlContext from the driver program(GenericDataLoad.scala).
   *
   * @return
   * (Dataset[Row], Dataset[Row]) => 2 DataSet containing refined data as well as report of error data.
   *
   */
  def emailValidity(df: DataFrame, emailIdColumns: List[String], sqlContext: SQLContext) = {
    val validity = sqlContext.udf.register("validity", validEmailCheck)
    if (emailIdColumns.length == 1 && emailIdColumns(0).equalsIgnoreCase("*")) {
      val expressionBuilder = df.columns.map { case (c1) => (validity(df(c1)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.select(df("*")).where(!expressionBuilder).withColumn("Reason_of_elimination", lit("Invalid email id encounter in some of the columns in " + df.columns.mkString(","))).coalesce(1)
      //val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Not Valid Date in some of the columns in " + df.columns.mkString(","))).coalesce(1)
      (refinedDf, report)

    } else {
      val expressionBuilder = emailIdColumns.map { case (c1) => (validity(df(c1)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.select(df("*")).where(!expressionBuilder).withColumn("Reason_of_elimination", lit("Invalid email id encounter in some of the columns in " + emailIdColumns.mkString(","))).coalesce(1)
      //val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Not Valid Date in some of the columns in " + columnName.mkString(","))).coalesce(1)
      (refinedDf, report)
    }
  }
  /*
   * @Description
   * Valid IP check.Performed on list of ip columns or all the columns in a table if the table contains only Ip .
   *
   * @Function Definition
   * def IpValidity(df: DataFrame, ipColumns: List[String], sqlContext: SQLContext)
   *
   * @param
   * df => DataFrame on which ip validity will be performed.
   * ipColumns=> columns in which ip are expected.
   * sqlContext => using the same instance of sqlContext from the driver program(GenericDataLoad.scala).
   *
   * @return
   * (Dataset[Row], Dataset[Row]) => 2 DataSet containing refined data as well as report of error data.
   *
   */
  def IpValidity(df: DataFrame, ipColumns: List[String], sqlContext: SQLContext) = {
    val validity = sqlContext.udf.register("validity", validIPCheck)
    if (ipColumns.length == 1 && ipColumns(0).equalsIgnoreCase("*")) {
      val expressionBuilder = df.columns.map { case (c1) => (validity(df(c1)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.select(df("*")).where(!expressionBuilder).withColumn("Reason_of_elimination", lit("Invalid email id encounter in some of the columns in " + df.columns.mkString(","))).coalesce(1)
      //val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Not Valid Date in some of the columns in " + df.columns.mkString(","))).coalesce(1)
      (refinedDf, report)

    } else {
      val expressionBuilder = ipColumns.map { case (c1) => (validity(df(c1)) !== false) }.reduce(_ && _)
      val refinedDf = df.select(df("*")).where(expressionBuilder)
      val report = df.select(df("*")).where(!expressionBuilder).withColumn("Reason_of_elimination", lit("Invalid email id encounter in some of the columns in " + ipColumns.mkString(","))).coalesce(1)
      //val report = df.except(refinedDf).withColumn("Reason_of_elimination", lit("Not Valid Date in some of the columns in " + columnName.mkString(","))).coalesce(1)
      (refinedDf, report)
    }
  }

  def checksumValidity(df: DataFrame, columns: List[String], sqlContext: SQLContext) = {
    val validity = sqlContext.udf.register("validity", checkSumGen)  
    val expression=df.columns.mkString(",")
    df.registerTempTable("temp")
    val refinedDf = sqlContext.sql(f"select *,validity(concat($expression))as checksum from temp")
    
    refinedDf
  }

}