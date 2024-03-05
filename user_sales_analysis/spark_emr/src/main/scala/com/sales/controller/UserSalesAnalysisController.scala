package com.sales.controller

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.io.Source
import com.sales.utils._


object UserSalesAnalysisController extends Serializable with UtilsFunctions {

  def main(args: Array[String]): Unit = {
    val Array(configFilePath, salesDataPath, apiUserURL,apiWeatherUrl,appid) = args

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate();

    import spark.implicits._

    //val configFile = "/Users/lakshimi.mariappan/Desktop/Work/Repo_SSO/user_sales_analysis/schemaJson/configuration.json"
    val schemaStr = (spark
      .read
      .text(configFilePath)
      .collect()
      .map(row => row.getString(0))
      .mkString(" ")
      )
    val jsonSchemaDataset = spark.createDataset(schemaStr :: Nil)

    val jsonSchemaDataFrame = (spark
      .read
      .option("inferSchema", "true")
      .option("multiline", "true")
      .json(jsonSchemaDataset))

    val tableNameTarget = jsonSchemaDataFrame.select("tableNameTarget").first().get(0).toString
    val dbName = jsonSchemaDataFrame.select("dbName").first().get(0).toString

    val targetTblName = dbName + "." + tableNameTarget

    //val salesDataPath = "/Users/lakshimi.mariappan/Desktop/Work/Repo_SSO/user_sales_analysis/salesData/sales_data.csv"

    val salesDF = spark.read.option("header","true").csv(salesDataPath)

    //val apiUserURL = "https://jsonplaceholder.typicode.com/users"
    // Read API response and append to lines DataFrame
    val apiUserResponse = Source.fromURL(apiUserURL).getLines().mkString("\n")
    var userApiDF = spark.read.json(spark.sparkContext.parallelize(Seq(apiUserResponse)))

    val userCreateTblQuery = createTargetTable(userApiDF,"usermaster")

    spark.sql(userCreateTblQuery)

    insertToTable(userApiDF,dbName,"usermaster")

    val salesCreateTblQuery = createTargetTable(salesDF,"salesmaster")

    spark.sql(salesCreateTblQuery)

    insertToTable(salesDF,dbName,"salesmaster")

    val userLocCordDF = userApiDF.select("address.geo.lat","address.geo.lng")
    var locCordDF = spark.emptyDataFrame
    var weatherDF = spark.emptyDataFrame

    userLocCordDF.collect().foreach { row =>
      val lat = row.getString(0)
      val lng = row.getString(1)
      val WeatherUrl = s"$apiWeatherUrl"+s"sweather?lat=$lat&lon=$lng&appid=$appid"
      // Read API response and append to lines DataFrame

      weatherDF = spark.read.json(spark.sparkContext.parallelize(Seq(Source.fromURL(WeatherUrl).getLines().mkString("\n"))))
      if (weatherDF.columns.isEmpty) weatherDF = weatherDF else weatherDF = weatherDF.unionByName(weatherDF)

      var weatherCordDF = weatherDF.select("coord","weather","main")
      if (locCordDF.columns.isEmpty) locCordDF = weatherCordDF else locCordDF = locCordDF.unionByName(weatherCordDF)
    }
    
    val weatherCreateTblQuery = createTargetTable(weatherDF,"weathermaster")
    spark.sql(weatherCreateTblQuery)
    insertToTable(weatherDF,dbName,"weathermaster")

    var userSalesDF = userApiDF.join(salesDF,userApiDF("id") === salesDF("customer_id"),"left")
    val userSalesFilteredDF =  userSalesDF.select("id","name","username","order_id","product_id","quantity","price","order_date")

    val salesPerUser = userSalesFilteredDF.groupBy("id","name")
      .agg(functions.sum(col("price").cast(IntegerType)).alias("total_sales_amount"))
    salesPerUser.show(false)
    println("Total Sales Per Customer =========> ")
    

    // avg order quantity

    val avgOrderQuantity = salesDF.groupBy("product_id")
      .agg(round(functions.avg(col("quantity").cast(FloatType)))
        .alias("avgOrderQuantity"))
    avgOrderQuantity.show(false)
    println("Avergae Order Quantity =========> ")

    // Identify the top-selling products or customers.

    val topSellProdByQuantity = salesDF.groupBy("product_id")
      .agg(sum(col("quantity").cast(IntegerType))
        .alias("ts_prod_quantity"))
      .orderBy(desc("topSellProdByQuantity"))
      .limit(1)
    topSellProdByQuantity.show(false)
    println("Top Selling Product By Quantity =========>")

    val topSellProdByPrice = salesDF.groupBy("product_id")
      .agg(round(sum(col("price").cast(FloatType)), 2)
        .alias("ts_prod_price"))
      .orderBy(desc("ts_prod_price"))
      .limit(1)
    topSellProdByPrice.show(false)
    println("Top Selling Product By Price =========>")

    val dateFormat = "yyyy-MM-dd"
    val salesDFWithDate = salesDF.withColumn("order_date", to_date(col("order_date"), dateFormat))

    // Analyze sales trends over time (e.g., monthly or quarterly sales).

    val salesDFWithMonth = salesDFWithDate.withColumn("month", month(col("order_date")))
      .orderBy(asc("month"))
    val salesDFWithQuarter = salesDFWithDate.withColumn("quarter", quarter(col("order_date")))
      .orderBy(asc("quarter"))

    val monthlySalesPrice = salesDFWithMonth.groupBy("month")
      .agg(round(sum("price"),2).alias("total_sales_monthly"))
    monthlySalesPrice.show(false)
    val quarterlySalesPrice = salesDFWithQuarter.groupBy("quarter")
      .agg(round(sum("price"),2).alias("total_sales_quarterly"))
    quarterlySalesPrice.show(false)

    println("Analyze Sales Trends (Monthly & Quarterly) with Price. =========>")

    val monthlySalesQuantity = salesDFWithMonth.groupBy("month")
      .agg(sum("quantity").alias("total_sales_monthly"))
    val quarterlySalesQuantity = salesDFWithQuarter.groupBy("quarter")
      .agg(sum("quantity").alias("total_sales_quarterly"))

    monthlySalesQuantity.show(false)
    quarterlySalesQuantity.show(false)

    println("Analyze Sales Trends (Monthly & Quarterly) with Quantity. =========>")

    locCordDF = locCordDF.withColumn("l_lat_lng",concat(col("coord.lat") , col("coord.lon")))
    userSalesDF = userSalesDF.withColumn("u_lat_lng",concat(col("address.geo.lat"),col("address.geo.lng")))

    val finalDF =  userSalesDF.join(locCordDF,userSalesDF("u_lat_lng") === locCordDF("l_lat_lng"),"left")

    var weatherWiseSales = finalDF.groupBy(col("weather").getItem(0).getField("main").alias("Weather"))
      .agg(round(sum("price"), 2).alias("Sales_Price"))

    weatherWiseSales = weatherWiseSales.select(replaceToNullObject(weatherWiseSales.columns.toArray): _*)
    weatherWiseSales.select(col("Weather"),col("Sales_Price").cast(FloatType))
    finalDF.createOrReplaceTempView(tableNameTarget)

    val sqlQuery = jsonSchemaDataFrame.select("sql").first().get(0).toString.format(tableNameTarget)

    val finalUserSalesDF = spark.sql(sqlQuery)

    val finalUserSalesCreateTblQuery = createTargetTable(finalUserSalesDF,"usersales")
    spark.sql(finalUserSalesCreateTblQuery)
    insertToTable(finalUserSalesDF,dbName,"usersales")


  }


}