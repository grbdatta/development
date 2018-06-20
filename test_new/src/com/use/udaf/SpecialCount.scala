package com.use.udaf

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._

class SpecialCount() extends UserDefinedAggregateFunction {

  def deterministic: Boolean = true
  def inputSchema: StructType = StructType(Array(StructField("value", LongType)))
  def dataType: DataType = LongType
 
  def bufferSchema = StructType(Array(
    StructField("count", LongType)
  ))
  
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 1.toLong
  }
 
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    buffer(0) = buffer.getLong(0)*input.getLong(0)
    println(buffer(0))
  }
 
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    println(buffer1(0))
  }
 
  def evaluate(buffer: Row): Long = {
    buffer.getLong(0).toLong
  }
 }