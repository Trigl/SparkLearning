package com.trigl.spark.arrival

import java.util.Date

import com.trigl.spark.util.{HBaseUtil, MyMultipleTextOutputFormat}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{CompareFilter, RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 从一个集群的HBase获取数据传到另一个集群的HDFS
  * created by Trigl at 2017-05-27 11:10
  */
object HBase2Hdfs {

  // 存放解析后的到达数据的路径
  val ARRIVAL_DIR = "/test/old/arrival_data"
  // SimpleDateFormat非线程安全，使用这个类来代替
  val fdf = FastDateFormat.getInstance("yyyy/MM/dd/")

  def main(args: Array[String]) {

    if (args.length == 0) {
      println("参数异常")
      System.exit(1)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 解决各种超时问题
    System.setProperty("spark.network.timeout", "600s")
    val sparkConf = new SparkConf().setAppName("HBase2Hdfs_" + args(0))
    val sc = new SparkContext(sparkConf)

    // 创建HBase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", HBaseUtil.HBASE_ZOOKEEPER_QUORUM)
    hBaseConf.set("hbase.zookeeper.property.clientPort", HBaseUtil.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    hBaseConf.set(TableInputFormat.INPUT_TABLE, HBaseUtil.TABLE_NAME_ARR_OLD)
    hBaseConf.setInt("hbase.rpc.timeout", 1200000)
    hBaseConf.setInt("hbase.client.operation.timeout", 1200000)
    hBaseConf.setInt("hbase.client.scanner.timeout.period", 1200000)

    // 行过滤器：正则匹配
    val scan = new Scan()
    scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^.{32}" + args(0) + ".*$")))
    hBaseConf.set(TableInputFormat.SCAN, HBaseUtil.convertScanToString(scan))

    // 应用newAPIHadoopRDD读取HBase，返回NewHadoopRDD
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    // 将数据映射为表，也就是将RDD转化为dataframe schema
    val resRDD = hbaseRDD.map(tuple => tuple._2)
      .map(genBasicInfo).filter(t =>
      StringUtils.isNotBlank(t._1) && StringUtils.isNotBlank(t._2)
    )

    resRDD.saveAsHadoopFile(ARRIVAL_DIR, classOf[String], classOf[String], classOf[MyMultipleTextOutputFormat[String, String]])

    sc.stop()
  }

  /**
    * 解析HBase获取的表
    * @param row
    * @return
    */
  def genBasicInfo(row: Result): (String, String) = {
    var day = ""
    val result = new StringBuilder
    val sb = new StringBuilder()
    try {
      // 解析所有字段
      val source = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("source")))
      val processType = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("processType")))
      val serverIp = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("serverIp")))
      val optTime = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("optTime")))
      val serviceProvider = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("serviceProvider")))
      val netType = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("netType")))
      val firstImei = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("firstImei")))
      val secondImei = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("secondImei")))
      val thirdImei = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("thirdImei")))
      val channelNumber = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("channelNumber")))
      val flag = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("flag")))
      val phoneBrand = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("phoneBrand")))
      val phoneModel = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("phoneModel")))
      val apps = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("appList")))
      val counterPkg = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("counterPkg")))
      val counterCrc32 = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("counterCrc32")))
      val longitude = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("longitude")))
      val latitude = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("latitude")))
      val simStatus = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("simStatus")))
      val simSequence = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("simSequence")))
      val identity = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("identity")))
      val arriveTimes = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("arriveTimes")))
      val acceptTimestamp = Bytes.toString(row.getValue(Bytes.toBytes(HBaseUtil.COLUMN_FAMILY), Bytes.toBytes("acceptTimestamp")))

      val date = new Date(acceptTimestamp.toLong)
      day = fdf.format(date)

      val arr = apps.split(";")
      // 获取签名crc和包名
      try {
        for (i <- 0 until arr.length) {
          val eme = arr(i)
          if ("1".equals(processType)) {
            // 老版本中只回传包名_apk文件crc
            // 格式“com.qihoo.appstore.FE1E4867”
            val idx = eme.lastIndexOf(".")
            val pkg = eme.substring(0, idx)
            val scrc = checkCrc(eme.substring(idx + 1))
            sb.append(";" + scrc + "," + pkg)

          } else if ("2".equals(processType)) {
            // processType == "2"
            // 新版本回传包名_apk文件crc.签名文件crc.安装分区
            // com.qihoo.appstore.FE1E4867
            // 或com.zj_lab.fireworks3.baidu_D60FA026.57113dce.1
            val lastIndex = eme.lastIndexOf(".")
            val last = eme.substring(lastIndex + 1)
            if (last.length() == 1) {
              // com.zj_lab.fireworks3.baidu_D60FA026.57113dce.1
              val pkg = eme.substring(0, eme.lastIndexOf("_"))
              val scrc = checkCrc(eme.substring(
                eme.lastIndexOf("_") + 1).split("\\.")(1))
              sb.append(";" + scrc + "," + pkg)
            } else {
              // com.qihoo.appstore.FE1E4867
              val pkg = eme.substring(0, lastIndex)
              val scrc = checkCrc(eme.substring(lastIndex + 1))
              sb.append(";" + scrc + "," + pkg)
            }
          } else if ("3".equals(processType)) {
            //com.dsi.ant.server&ANT HAL Service.00000000.ee0dcfb0.0;com.iqoo.secure&i 管家.00000000.c15e53c0.0;com.example.counter_plugin_test&counter_plugin_test.00000000.b0ba37ad.1;
            val pkg = eme.substring(0, eme.indexOf("&"))
            val ex_pkg = eme.substring(eme.indexOf("&") + 1) //ANT HAL Service.00000000.ee0dcfb0.0
            var str = ex_pkg.substring(0, ex_pkg.lastIndexOf(".")) //ANT HAL Service.00000000.ee0dcfb0
            val scrc = checkCrc(str.substring(str.lastIndexOf(".") + 1))
            str = str.substring(0, str.lastIndexOf(".")) //ANT HAL Service.00000000
            val appname = str.substring(0, str.lastIndexOf(".")) //ANT HAL Service
            sb.append(";" + scrc + "," + pkg + "," + appname)
          } else {
            val lastIndex = eme.lastIndexOf(".")
            // com.qihoo.appstore.FE1E4867
            val pkg = eme.substring(0, lastIndex)
            val scrc = checkCrc(eme.substring(lastIndex + 1))
            sb.append(";" + scrc + "," + pkg)
          }
        }
      } catch {
        case e: Exception =>
          println("解析签名crc和包名出错")
          e.printStackTrace()
      }
      var scrc_pkgs = ""
      if (sb.length > 0)
        scrc_pkgs = sb.substring(1)
      // D0|2|119.48.50.222|2016-10-31 12:47:58|0|1|869633027675964| | |D001|D001|Xiaomi|Redmi Note 2|d7a71299,com.wandoujia.phoenix2;36c205b4,com.funshion.video.player;fe6c660e,com.mtk.telephony|com.huohou.apps|b6737115|125.298329|43.86323|0| |66761558fd1679ab9733b6250a36a1d1|0|1477889123637
      result.append(if (StringUtils.isNotBlank(source)) source else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(processType)) processType else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(serverIp)) serverIp else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(optTime)) optTime else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(serviceProvider)) serviceProvider else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(netType)) netType else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(firstImei)) firstImei else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(secondImei)) secondImei else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(thirdImei)) thirdImei else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(channelNumber)) channelNumber else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(flag)) flag else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(phoneBrand)) phoneBrand else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(phoneModel)) phoneModel else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(scrc_pkgs)) scrc_pkgs else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(counterPkg)) counterPkg else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(counterCrc32)) counterCrc32 else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(longitude)) longitude else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(latitude)) latitude else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(simStatus)) simStatus else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(simSequence)) simSequence else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(identity)) identity else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(arriveTimes)) arriveTimes else " ")
      result.append("|")
      result.append(if (StringUtils.isNotBlank(acceptTimestamp)) acceptTimestamp else " ")

    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    (day, result.toString())

  }

  def checkCrc(crc: String): String = {
    var crc32 = crc
    if (crc.length() < 8)
    // 补零
      crc32 = crc.format("%1$0" + (8 - crc.length()) + "d", new Integer(0)) + crc
    crc32
  }
}