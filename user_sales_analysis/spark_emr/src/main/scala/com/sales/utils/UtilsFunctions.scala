package com.sales.utils

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, regexp_replace, udf, when}
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.text.SimpleDateFormat

trait UtilsFunctions {

  def alterTargetTable(procDF: DataFrame, targetTblDF1: DataFrame, targetTblName: String): Seq[String] = {

    var alterCols = ""
    var StrType = "StringType"
    alterCols = procDF.schema.fields.diff(targetTblDF1.schema.fields).map { field =>
      val dataType = field.dataType match {
        //case StringType.getClass => "String"
        case IntegerType => "Int"
        case LongType => "Long"
        case FloatType => "Float"
        case DoubleType => "Double"
        case DateType => "String"
        case TimestampType => "String"
        case BooleanType => "String"
        case _: DecimalType => "Decimal"
        case _ => throw new UnsupportedOperationException(s"Unsupported data type: ${field.dataType}")
      }
      s"${field.name} $dataType"
    }.mkString(",\n   ")

    val alterQuery = "alter table %s add columns( %s) ".format(targetTblName, alterCols)

    var alterColumns = procDF.schema.fields.diff(targetTblDF1.schema.fields).map { field =>
      s"${field.name}"
    }.mkString(",\n   ")

    println("Schema Evolution - Column/s Addition : " + alterColumns)

    var returnVar = Seq(alterQuery, alterColumns)

    return returnVar
  }

  def replaceTransformation(columns: Array[String]): Array[Column] = {
    columns.map(c => {
      regexp_replace(col(c), ",", "_").alias(c)
    })
  }

  def toDate = udf((dateString: String, dateFormatString: String) => {
    if(dateString == null){null}
    else if (dateString.contains("T")) {dateString.replace("T", " ").substring(0, 19)}
    else if (dateString.length >= 10){
      val putdate = new Timestamp(dateString.toLong)
      val dateFormat = new SimpleDateFormat(dateFormatString)
      dateFormat.format(putdate)
    }
    else {
      dateString
    }
  })

  def replaceToNullObject(columns: Array[String]): Array[Column] = {
    columns.map(c => {
      when(col(c) === "Other" ||
        col(c) === "Other" ||
        col(c) === "Other" ||
        col(c) === "" ||
        col(c).isNull, "Other")
        .otherwise(col(c)).alias(c)
    })
  }
  def createTargetTable(procDF: DataFrame, targetTblName: String): String = {
    var createTableStatement = ""

    val columnNames = procDF.schema.fields
      .map { field =>
        val dataType = field.dataType match {
          case StringType => "String"
          case IntegerType => "Int"
          case LongType => "Long"
          case FloatType => "Float"
          case DoubleType => "Double"
          case DateType => "String"
          case TimestampType => "String"
          case BooleanType => "String"
          case _: DecimalType => "Decimal"
          case otherType => otherType.toString
        }
        s"${field.name}  $dataType"
      }
      .mkString(",\n   ")

    val query =
      """CREATE TABLE IF NOT EXISTS %s(%s));""".stripMargin.format(targetTblName, columnNames)

    return query
  }

  def insertToTable (writeDF:DataFrame,dbName:String,tableName:String): Unit = {
    writeDF.write
      .format("parquet")
      .saveAsTable(s"$dbName.$tableName")
  }


}