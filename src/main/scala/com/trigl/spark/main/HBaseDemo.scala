package com.trigl.spark.main

import com.trigl.spark.util.HbaseUtil
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 高效连接HBase示例
  * created by Trigl at 2017-05-20 15:34
  */
object HBaseDemo {

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("HBaseDemo")
    val sc = new SparkContext(sparkConf)

    val data = sc.textFile("/test/imei.txt").mapPartitions(getHBaseInfo)

    // 结果以 主键|包列表|时间 的格式存入HDFS
    data.map(l => {
      val rowKey = l._2._1
      val pkgList = l._2._2
      val time = l._1
      rowKey + "|" + pkgList + "|" + time // 用"|"分隔
    }).repartition(1).saveAsTextFile("/test/fenxi/cpz")

    sc.stop()
  }

  /**
    * 从HBase查询
    *
    * @param iter mapPartion算子的参数是Iterator
    * @return 返回的也是Iterator
    */
  def getHBaseInfo(iter: Iterator[String]): Iterator[(String, (String, String))] = {

    var pkgList = List[(String, (String, String))]() // 结果格式为(日期,(主键,包名集合))

    // 建立连接查询表
    val conn = HbaseUtil.getConnection(HbaseUtil.TABLE_NAME_CPZ_APP)
    val table = conn.getTable(TableName.valueOf(HbaseUtil.TABLE_NAME_CPZ_APP))

    // 新建Scan用于指定查询内容
    val scan = new Scan()
    scan.setCaching(10000)
    scan.setCacheBlocks(false)
    // 要查询的列
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "packagelist".getBytes)
    scan.addColumn(HbaseUtil.COLUMN_FAMILY.getBytes, "cdate".getBytes)

    while (iter.hasNext) {
      // 要查询的前缀
      val imei = iter.next()
      // HBase前缀查询
      scan.setRowPrefixFilter(imei.getBytes)
      // 查询结果
      val resultScanner = table.getScanner(scan)
      val it = resultScanner.iterator()
      if (it.hasNext) {
        val result: Result = it.next()

        // 主键
        val key = Bytes.toString(result.getRow)
        // 日期
        val cdate = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "cdate".getBytes))
        // 包列表
        val packagelist = Bytes.toString(result.getValue(HbaseUtil.COLUMN_FAMILY.getBytes, "packagelist".getBytes))

        // 添加到集合中
        pkgList.::=(cdate, (key, packagelist))
      }
    }

    // 关闭HBase连接
    table.close()
    conn.close()

    // 结果返回iterator
    pkgList.iterator
  }
}
