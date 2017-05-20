package com.trigl.spark.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}


object HbaseUtil {
  val HBASE_ZOOKEEPER_QUORUM = "fenxi-xlg,fenxi-mrg,fenxi-xxf,fenxi-ptd,fenxi-xz"
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"
  
  val COLUMN_FAMILY = "scf"
  val databytes = Bytes.toBytes(COLUMN_FAMILY)
  
  val TABLE_NAME_CPZ_SOURCE = "cpz_source"
  val TABLE_NAME_CPZ_APP = "cpz_app"
  val TABLE_NAME_CPZ_SOURCE_V2 = "cpz_source_v2"
  val TABLE_NAME_CPZ_APP_V2 = "cpz_app_v2"
  
  
   def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  /**
    * 获取HBase连接
    * @param tbName
    * @return
    */
  def getConnection(tbName: String): Connection ={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER_QUORUM)
    conf.set("hbase.zookeeper.property.clientPort", HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
    conf.set(TableInputFormat.INPUT_TABLE, tbName)
    conf.setInt("hbase.rpc.timeout", 120000)
    conf.setInt("hbase.client.operation.timeout", 120000)
    conf.setInt("hbase.client.scanner.timeout.period", 120000)
    val conn = ConnectionFactory.createConnection(conf)
    conn
  }
}