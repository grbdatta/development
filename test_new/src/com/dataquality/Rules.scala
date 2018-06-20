package com.dataquality
import org.apache.spark.sql.functions._
import java.security.MessageDigest;
import org.joda.time.format._
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.Try
import com.exceptions.InvalidDateFormatException
import java.util.Formatter.DateTime
import org.joda.time.Years
import org.joda.time._
import java.math.BigInteger
import org.apache.spark.sql.DataFrame

object Rules {

  System.setProperty("hadoop.home.dir", "C:\\hadoop");
  def valid = (lastName: String) => {
    if (!lastName.endsWith("a")) {
      true
    } else
      false
  }
  def phoneNumberCheck = (phone_number: String, lengthOfNumber: Int, startWith: String) => {
    if (phone_number.length() == lengthOfNumber && phone_number.startsWith(startWith))
      true
    else
      false
  }
  def numberFormatCheck = (number: String) => {
    Try { number.toDouble }.isSuccess
  }
  def rangeCheck = (upperLimit: BigDecimal, lowerLimit: BigDecimal, actualValue: BigDecimal) => {
    if (actualValue <= upperLimit && actualValue >= lowerLimit)
      true
    else
      false
  }

  def listOfValuesCheck = (columnValue: String, values: String) => {
    values.split(",").contains(columnValue)
  }
  def isNull = (x: String) => {
    if (x == null || x.isEmpty)
      false
    else
      true
  }
  def dateFormatCheck = (inputDate: String, format: String) => {

    try {
      val fmt = DateTimeFormat forPattern format
      val output = fmt.parseDateTime(inputDate)
      true
    } catch {
      case ex: Exception => {
        //throw new InvalidDateFormatException(inputDate+" not matching with specified "+format+" format")
        false
      }
    }
  }

  def validAgeCheck = (fromDate: String, toDate: String, ageLimit: Int) => {

    try {
      println("----------Age check------------------")
      val fromDt = new LocalDate(fromDate.split("/")(2).toInt, fromDate.split("/")(1).toInt, fromDate.split("/")(0).toInt); //Birth date
      val toDt = new LocalDate(toDate.split("/")(2).toInt, toDate.split("/")(1).toInt, toDate.split("/")(0).toInt); //Today's date
      val age = new Period(fromDt, toDt, PeriodType.years());
      println("Age:" + age.getYears)
      if (age.getYears >= ageLimit)
        true
      else
        false
    } catch {
      case ex: Exception => {
        println("Date format exception")
        ex.printStackTrace
        //throw new InvalidDateFormatException(inputDate+" not matching with specified "+format+" format")
        false
      }
    }

  }
  
  def validEmailCheck = (email: String) => {
    if ("""(?=[^\s]+)(?=(\w+)@([\w\.]+))""".r.findFirstIn(email) == None) 
      false 
    else 
      true
  }
  def validIPCheck=(ip:String)=>{
    if (""".*?(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3}).*""".r.findFirstIn(ip) == None) 
      false 
    else 
      true
  }
  
  def checkSumGen=(columns:String)=>{
    
    val str=columns
    //println(str)
    String.format("%032x", new BigInteger(1, MessageDigest.getInstance("SHA-512").digest(str.getBytes("UTF-8"))))
    
  }
}