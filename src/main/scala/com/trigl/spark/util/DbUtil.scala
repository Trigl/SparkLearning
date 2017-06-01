package com.trigl.spark.util

import java.sql.{Connection, DriverManager, Statement}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object DbUtil {

  //运营数据库
  val ADMIN_DB_URL = "jdbc:mysql://localhost:3306/tpdb?useUnicode=true&characterEncoding=UTF8"
  val ADMIN_DB_USERNAME = "rds_admin"
  val ADMIN_DB_PASSWORD = "changmi890*()"
  //渠道
  val CHANNEL_DB_URL = "jdbc:mysql://localhost:3306/channeldb?useUnicode=true&characterEncoding=UTF8"
  val CHANNEL_DB_USERNAME = "rds_channel"
  val CHANNEL_DB_PASSWORD = "changmi890*()"

  //广告主
  val ADV_DB_URL = "jdbc:mysql://localhost:3306/adverdb?useUnicode=true&characterEncoding=UTF8"
  val ADV_DB_USERNAME = "rds_ad"
  val ADV_DB_PASSWORD = "changmi890*()"

  //IMEI库
  val IMEI_DB_URL = "jdbc:mysql://localhost:3306/IMEI?useUnicode=true&characterEncoding=UTF8"
  val IMEI_DB_USERNAME = "root"
  val IMEI_DB_PASSWORD = "123456"
  val IMEI_ALL_TABLE = "t_imei_all"



  def cleanTable(url: String, username: String, password: String, tablename: String) = {
    var dbconn: Connection = null
    var stmt: Statement = null
    try {
      dbconn = DriverManager.getConnection(url, username, password)
      stmt = dbconn.createStatement()
      stmt.execute("truncate table " + tablename)
    } catch {
      case e: Exception =>
        println(">>>>>>>>>>>>清空表失败")
        e.printStackTrace()
    } finally {
      if (stmt != null)
        stmt.close()
      if (dbconn != null)
        dbconn.close()
    }

  }


}