package com.trigl.spark.util

import java.sql.{Connection, DriverManager, Statement}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object DbUtil {

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