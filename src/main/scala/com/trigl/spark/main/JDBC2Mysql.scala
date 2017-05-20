package com.trigl.spark.main

import com.trigl.spark.util.DbUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Spark连接Mysql数据库
  * created by Trigl at 2017-05-20 14:21
  */
object JDBC2Mysql {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("JDBC2Mysql")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    // 查询Mysql
    val imeis = spark.read.format("jdbc").options(
      Map("url" -> DbUtil.IMEI_DB_URL,
//        "dbtable" -> "(SELECT id,imei,imeiid FROM t_imei_all) a",
        "dbtable" -> DbUtil.IMEI_ALL_TABLE,
        "user" -> DbUtil.IMEI_DB_USERNAME,
        "password" -> DbUtil.IMEI_DB_PASSWORD,
        "driver" -> "com.mysql.jdbc.Driver",
//        "fetchSize" -> "1000",
        "partitionColumn" -> "id",
        "lowerBound" -> "1",
        "upperBound" -> "15509195",
        "numPartitions" -> "20")).load()

    // 显示10条结果
    imeis.show(10)

    spark.stop()
  }
}
