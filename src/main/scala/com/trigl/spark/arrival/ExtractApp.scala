package com.trigl.spark.arrival

import java.text.SimpleDateFormat
import java.util.Calendar

import com.trigl.spark.util.HDFSUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 从Hive表base_arrival_old中提取信息到新表dim_app_old
  *
  * @author 白鑫
  */
object ExtractApp {

  case class AppDetail(signcrc32: String, pkg: String, appname: String)
  case class AppInfo(pkg: String, appname: String, signcrc32: String, imei: String, arrivetimes: Int, applist: Seq[AppDetail], year: String, month: String, day: String)

  val DELETE_HDFS_DIR = "/changmi" // 要删除空文件的hdfs文件夹

  // args -> 20170101
  def main(args: Array[String]) {

    if (args.length == 0) {
      System.err.println("参数异常")
      System.exit(1)
    }

    val year = args(0).substring(0, 4)
    val month = args(0).substring(4, 6)
    //    val day = args(0).substring(6, 8)

    //设置序列化器为KryoSerializer,也可以在配置文件中进行配置
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 设置应用名称，新建Spark环境
    val sparkConf = new SparkConf().setAppName("ExtractAppOld_" + args(0))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    println("Start " + "ExtractAppOld_" + args(0))

    import spark.sql

    sql("use arrival")
    val sqlStr = "select (case when (firstimei != ' ') then firstimei when (secondimei != ' ') then secondimei when (thirdimei != ' ') then thirdimei else null end) as imei, " +
      "applist, arrivetimes, year, month, day from base_arrival_old where year=" + year + " and month=" + month

    //    implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[AppInfo]
    val arrive = sql(sqlStr).rdd
    val rowRDD = arrive
      .filter(_.getSeq[Row](1) != null) // applist不能为空
      .map(a => AppInfo(null, null, null, a.getString(0), a.getInt(2), a.getSeq[Row](1)
      .map(r => AppDetail(r.getString(0), r.getString(1), r.getString(2))), a.getString(2), a.getString(3), a.getString(4)))
      .flatMap(getAppdetail)
      .map(app => Row(app.imei, app.appname, app.pkg, app.signcrc32, app.arrivetimes, app.year, app.month, app.day))

    // The schema is encoded in a string
    val schemaString = "imei appname pkg signcrc32 arrivetimes year month day"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map {
        fieldName =>
          if (fieldName.equals("arrivetimes")) StructField(fieldName, IntegerType, nullable = true)
          else StructField(fieldName, StringType, nullable = true)
      }
    val schema = StructType(fields)


    // Apply the schema to the RDD
    val appDF = spark.createDataFrame(rowRDD, schema)

    // Creates a temporary view using the DataFrame
    appDF.createOrReplaceTempView("app")
    val insertSql = "FROM app a\n" + generateInsertSql(args(0) + "01", addMonth(args(0)) + "01")
    //    val insertSql = "FROM app a\n" + generateInsertSql(year,month)
    sql(insertSql)

    spark.stop()

    // 删除hdfs空文件
    HDFSUtil.delete(DELETE_HDFS_DIR)
  }

  // 得到pkg，signcrc32，appname
  def getAppdetail(app: AppInfo): List[AppInfo] = {
    // Scala List不可变（immutable）
    val apps = ListBuffer[AppInfo]()

    if (app.applist != null) {
      for (detail <- app.applist) {
        val a = new AppInfo(detail.pkg, detail.appname, detail.signcrc32, app.imei, app.arrivetimes, null, app.year, app.month, app.day)
        apps += a
      }
    }

    apps.toList
  }

  def generateInsertSql(year: String, month: String, day: String): String = {
    val sb = new StringBuilder

    sb.append("INSERT OVERWRITE TABLE dim_app_old\n")
    sb.append("PARTITION (year=" + year + ",month=" + month + ",day=" + day + ")\n")
    sb.append("SELECT a.pkg,a.appname,a.signcrc32,a.opttime,a.firstimei,a.secondimei,a.thirdimei WHERE a.year=" + year + " and a.month=" + month + " AND a.day=" + day + "\n")
    sb.toString()
  }

  // 生成insert语句
  def generateInsertSql(date1: String, date2: String): String = {

    val sb = new StringBuilder
    val ca = Calendar.getInstance()

    val df = new SimpleDateFormat("yyyyMMdd")
    val yearDF = new SimpleDateFormat("yyyy")
    val monthDF = new SimpleDateFormat("MM")
    val dayDF = new SimpleDateFormat("dd")


    // 开始时间必须小于结束时间
    val beginDate = df.parse(date1)
    val endDate = df.parse(date2)
    var date = beginDate
    while (!date.equals(endDate)) {
      sb.append("INSERT OVERWRITE TABLE dim_app_old\n")
      sb.append("PARTITION (year=" + yearDF.format(date) + ",month=" + monthDF.format(date) + ",day=" + dayDF.format(date) + ")\n")
      sb.append("SELECT a.imei,a.appname,a.pkg,a.signcrc32,a.arrivetimes WHERE a.year=" + yearDF.format(date) + " and a.month=" + monthDF.format(date) + " AND a.day=" + dayDF.format(date) + "\n")
      ca.setTime(date)
      ca.add(Calendar.DATE, 1) // 日期加1天
      date = ca.getTime()
    }
    sb.toString()

  }

  // 月份加一
  def addMonth(oldDate: String): String = {
    val df = new SimpleDateFormat("yyyyMM")
    var date = df.parse(oldDate)
    val ca = Calendar.getInstance()
    ca.setTime(date)
    ca.add(Calendar.MONTH, 1) // 加一月
    date = ca.getTime()
    df.format(date)
  }

}