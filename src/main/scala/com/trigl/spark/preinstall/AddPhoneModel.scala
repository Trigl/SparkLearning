package com.trigl.spark.preinstall

import java.net.URLDecoder
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Date

import com.trigl.spark.util.DbUtil
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions.asScalaSet
import scala.collection.mutable.ArrayBuffer

/**
  * 解析预装数据，获取机型信息，将新机型添加到机型库
  */
object AddPhoneModel {

  //不同环境修改
  val sdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val UID = "2030"
  val T = "1492760643655"
  val M = "d3cb6b6be6a80b1ce3f1d9772db85d1f"

  def main(args: Array[String]) {

    // 参数格式：2017060108
    if (args == null || args.length < 1) {
      System.err.println("参数异常")
      System.exit(1)
    }

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    System.setProperty("spark.network.timeout", "300s")
    val sparkConf = new SparkConf().setAppName("AddPhoneModel_" + args(0))

    val sc = new SparkContext(sparkConf)

    val parseRDD = sc.textFile(args(0)).map(f => {
      try {
        // 上报的日志作了GBK编码，首先进行解码
        var x = URLDecoder.decode(f, "GBK")
        var map = Map[String, String]()

        // IP
        val ip = x.substring(0, x.indexOf("|"))
        x = x.substring(x.indexOf("|") + 1)

        // 13位时间戳,接收服务器的接收的时间
        var tss = x.substring(0, x.indexOf("|"))
        tss = tss.replaceAll("\\.", "")
        val ts = tss.toLong
        val posttime = sdf.format(new Date(ts))
        map += ("posttime" -> posttime)
        x = x.substring(x.indexOf("|") + 1)

        val arr = x.split("&")
        for (a <- arr) {
          val t = a.split("=")
          if (t.length == 1) //XX=&YY=c的情况，对XX= split之后长度为1
            map += (t(0) -> "")
          else
            map += (t(0) -> t(1))
        }

        //机型
        val model = map.getOrElse("model", "").toUpperCase()
        //品牌
        val brand = map.getOrElse("brand", "").toUpperCase()

        val lowModelAndBrand = model + brand
        // 过滤无效数据
        if (StringUtils.isBlank(model) ||
          StringUtils.isBlank(brand) ||
          lowModelAndBrand.contains("ADB") || lowModelAndBrand.contains("DAEMON") ||
          lowModelAndBrand.contains("ERROR") || lowModelAndBrand.contains("WARNING") ||
          lowModelAndBrand.contains("CANNOT")) {
          null
        } else {
          ((model, brand), 1)
        }
      } catch {
        case e: Exception =>
          null
      }
    }).filter(_ != null).distinct()

    // 获取库里的机型品牌信息
    val pres = getPrePhoneModelAndBrand()
    val presRDD = sc.parallelize(pres, 2)
      .map((_, 1))

    val result = parseRDD.subtractByKey(presRDD)
      .map(_._1).collect()

    sc.stop()

    // 通过接口把分析结果传入数据库
    if (result != null) {
      println("new phonemodel size:" + result.size)
      if (result.length > 0) {
        for (v <- result) {
          val map = new java.util.HashMap[String, String]()
          map.put("uid", UID)
          map.put("t", T)
          map.put("m", M)
          map.put("type", "phoneinfo")
          map.put("model", v._1)
          map.put("brand", v._2)
          val res = post("http://tpflash.gaojiquan.org/flashtool/romPackage/searchrom", map)
          println("res:" + res)
        }
      }
    }

    //执行
    val rt = Runtime.getRuntime()
    val cmd = Array("/bin/sh", "-c", "nohup /data/install/spark/bin/spark-submit --master spark://fenxi-xlg:7077 --executor-memory 12G --total-executor-cores 10 --name Data-Analyze --supervise --class com.analysis.test.prd.cpz.ParseBrushFinalSync_V2_3 --jars /data/install/hbase/lib/hbase-client-1.2.2.jar,/data/install/hbase/lib/hbase-server-1.2.2.jar,/data/install/hbase/lib/hbase-common-1.2.2.jar,/data/install/hbase/lib/hbase-protocol-1.2.2.jar,/data/install/hbase/lib/guava-12.0.1.jar,/data/install/hbase/lib/htrace-core-3.1.0-incubating.jar,/data/install/hbase/lib/metrics-core-2.2.0.jar,/home/hadoop/jars/mysql-connector-java-5.1.25.jar /home/hadoop/analysis-0.0.1-SNAPSHOT.jar " + args(0) + " > /home/hadoop/logs/spark_hbase.out &");

    try {
      val proc = rt.exec(cmd);
      // 获取进程的标准输入流
      val exitVal = proc.waitFor();
      proc.destroy()
      println("预装任务提交")
    } catch {
      case e: Exception =>
        println(args(0) + "预装任务提交失败：" + e.getMessage)
    }

  }

  /**
    * 发送post请求
    *
    * @param url
    * @param params
    * @return
    */
  def post(url: String, params: java.util.Map[String, String]): String = {
    try {
      val httpPost = new HttpPost(url)
      val client = new DefaultHttpClient()
      val valuePairs = new java.util.ArrayList[NameValuePair](params.size())

      for (entry <- params.entrySet()) {
        val nameValuePair = new BasicNameValuePair(entry.getKey(), String.valueOf(entry.getValue()))
        valuePairs.add(nameValuePair)
      }

      val formEntity = new UrlEncodedFormEntity(valuePairs, "UTF-8")
      httpPost.setEntity(formEntity)
      val resp = client.execute(httpPost)

      val entity = resp.getEntity()
      val respContent = EntityUtils.toString(entity, "UTF-8").trim()
      httpPost.abort()
      client.getConnectionManager().shutdown()

      respContent
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }

  /**
    * 从DB中获取机型信息
    *
    * @return
    */
  def getPrePhoneModelAndBrand(): ArrayBuffer[(String, String)] = {
    var dbconn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null

    var list = ArrayBuffer[(String, String)]()
    try {
      dbconn = DriverManager.getConnection(DbUtil.ADMIN_DB_URL, DbUtil.ADMIN_DB_USERNAME, DbUtil.ADMIN_DB_PASSWORD)

      stmt = dbconn.createStatement()

      rs = stmt.executeQuery("select a.phonemodelcode,b.manufacturername from t_phonemodel a left join t_phonemanufactur b on a.manufacturerid = b.manufacturerid group by a.phonemodelcode, b.manufacturername")
      while (rs.next()) {
        var model = rs.getString(1) // 转化成大写
        if (StringUtils.isNotBlank(model))
          model = model.toUpperCase()
        var brand = rs.getString(2)
        if (StringUtils.isNotBlank(brand))
          brand = brand.toUpperCase()
        list += ((model, brand))
      }
      println("获取机型成功")
    } catch {
      case e: Exception =>
        println("获取机型失败:" + e.getMessage())
        println("trace:" + e.printStackTrace())
        println("异常")
        System.exit(1)
    } finally {
      if (rs != null)
        rs.close()
      if (stmt != null)
        stmt.close()
      if (dbconn != null)
        dbconn.close()
    }
    list
  }

}