package com.trigl.spark.main

import java.text.SimpleDateFormat

import com.trigl.spark.util.{DataUtil, LauncherMultipleTextOutputFormat}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import scala.collection.mutable.ArrayBuffer

/**
  * 桌面日志实时ETL
  *
  * @author 白鑫
  */
object LauncherStreaming {

  val HDFS_DIR = "/changmi/launcher/click"

  def main(args: Array[String]) {

    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sparkConf = new SparkConf().setAppName("LauncherStreaming")

    //每60秒一个批次
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    //从Kafka中读取数据
    // kafka消息格式：val str = "183.160.102.250|1493169060.849|UEsDBBQACAgIAGBJmkoAAAAAAAAAAAAAAAABAAAAMGVVS3LrNhC8DVYvKmB+GJRXWaVygBwABCGLZYm0fi9xKofPUCJgO9mQNUVwPt09DRX2Ih41MgaSX*RrzOhOdfr77k48L7f6T1lOuzyPl2Uad+OUj*XiQmGikJMLlDD4BCmi5x+PKHhNrD5uEXBARukRBFbtkXof2n8QiUT9M7JWoockL2Xevd3*XHbvx*xhlccwesxArTKJtyQvX3vMx3w5leNS3hxmGQpQ7qcTA7XqyJE8bn1SDB6D6rdM13q7TfPr1VVKA8HYq6oPUfFHi5CVqU1vqZQ6FoAepUcIPqHvkWH4+R+yV+nfYhKvHUMktcZbpOwZ2klMiNh6CQQBCLbIsAHwlFqUBH0IjwkfBNvT+NyVfKqX7JBK2jOOfcaoIr074yJ56d0J8yffKYj10PsxglferMqtzqXOt935*D6drrXcL9XJyPshaackeU+4lbFmPcSwReTFMKG0SqDelns57Gox+c1jvjjjKw5YtacBNCS+sXc6XR0D7IsE7scic4RPMTJp+h8gr*loIv9wgylHsETXpkabu3GCEST0qSOKiQta496EjH0MiDYl*AeR4*SzOpQoPkjpFdj7FPseIUnc2Ftpj5T0a7PX22KAcg1xTywtR5KQoOdIUYOGliMYnKF9i2RvbtsQWUNITSsRwcTz1Eo55Pn1NO2O+T6Xg+1gkkHDkDZQ1yKJuPMvgWyxeoQm5KYbQkAb8RsSp5NjH3AkwpbvIQLfSUJk7v6hNoI2TJhAtOl7dQy1pW6oc1BO9L3WMkzHej67PZZxT2VsFZOBhk8h1NcuoF+P03v++O39sMzV5QJG1n7TbYBAZFi2tkjXPWxQQoIgW8tApjim7aRZobe2nuvRxLz743dnq2eoU3DNm4z8NrUtMZpWt*SmA8sO7ZsqRfOOR8Jrnt*ueVr1cbvn2RUdTVx1aEmVo4H2OHqePqbdz2msi8tjzTDsG*xiyU14rXurxLgJ2zxAUKXNkkjNUbrMo7fGnoAPeRrvZp*5Ug7D8pfjsVb1st0Xpry42n2DK9qkzcFQdRVQL8Fs5hb6Xq2oPDepLMvx8ThPLuwjQFXY0pulmAX7NoHRpNvinKfDsqD4TQnXvK9u3JsADBm3WZAttVVt6MoqwPhdRbebk1QFInjXSAAReSJ7y8uQl+3lcmXcC26ywbi+oE0XHwhSjyJ0c7IopdDARbWV4MQ9IqsNHTFApa82tl7b7*l9XVVz9aB5Q8aUEhPL0yYPH7v1jryWS62z0xKglBLaQbstgsLLv1BLBwjA8lu4twMAACkIAAA="
    val kafkaStream = KafkaUtils.createStream(
      ssc,
      "hxf:2181,cfg:2181,jqs:2181,jxf:2181,sxtb:2181", //Kafka集群使用的zookeeper
      "spark", // 该消费者使用的group.id
      Map[String, Int]("launcher_click" -> 0, "launcher_click" -> 1), //日志在Kafka中的topic及其分区
      StorageLevel.MEMORY_AND_DISK_SER).map(_._2) // 获取消息主体信息

    kafkaStream.foreachRDD((rdd: RDD[String], time: Time) => {
      // 解析日志
      val result = rdd.map(log => parseLog(log)).filter(t =>
        StringUtils.isNotBlank(t._1) && StringUtils.isNotBlank(t._2)
      )

      // 存入hdfs，自定义存储类LauncherMultipleTextOutputFormat，这个是重写源码得到的
      // 最后存入HDFS的数据格式：862206038607560;A000006D92387A;862206038771614|123.15.115.202|1493189263927|2017-04-26 14:47:43|huawei|huaweicaz-tl10|com.android.settings&f1135e7a&1493175762065,1493176092988,1493178531322,1493188005550,1493188128840,1493188926270&6;com.tencent.android.qqdownloader&9c0d488f&1493176381324,1493176908625&2;com.tencent.mm&5013d443&1493176706792,1493178513163,1493188645289,1493189072554&4;com.tencent.mobileqq&acbb2967&1493176791514&1;com.android.contacts&7c4346be&1493177706880,1493180828325,1493185635478,1493185827645,1493186011668,1493186417215&6;com.android.mms&451b71d7&1493178601517,1493178715372,1493178719645,1493185500378&4;cn.zdkj.ybt&086599b6&1493186556613&1;com.snda.wifilocating&b082af8e&1493188078302,1493188110929&2;com.huawei.camera&60ea4e6c&1493188192315,1493188236917,1493188619319&3
      // 存入以后的数据可以作为Hive表的数据来使用
      result.saveAsHadoopFile(HDFS_DIR, classOf[String], classOf[String], classOf[LauncherMultipleTextOutputFormat[String, String]])
    })

    // 启动
    ssc.start()
    // 等待输入
    ssc.awaitTermination()

  }

  /**
    * 分析日志
    * @param log
    * @return
    */
  def parseLog(log: String): Tuple2[String, String] = {

    var data = log
    var key = ""
    try {
      val sb = new StringBuilder
      var idx = 0
      // 接收服务器获取的IP
      val ip = data.substring(0, log.indexOf("|"))
      data = data.substring(log.indexOf("|") + 1)

      var tss = data.substring(0, data.indexOf("|"))
      tss = tss.replaceAll("\\.", "")
      // 服务器时间戳，13位
      val serverTimestamp = tss.toLong
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      // 服务器时间
      val serverTime = df.format(serverTimestamp)
      data = data.substring(data.indexOf("|") + 1)
      val keyDf = new SimpleDateFormat("yyyy/MM/dd/")
      key = keyDf.format(serverTimestamp)

      // 主体数据：imei列表&品牌&机型|包名&文件crc&时间戳列表;包名&文件crc&时间戳列表;...
      data = data.replaceAll("\\*", "\\/")
      data = DataUtil.unzip(data)

      // 解压后的数据格式
      // 865066038753146-865066038753153&meizu&m5note|com.android.dialer&1c5441a9&1493109297305,1493110895807,1493112513536,1493112521588,1493112580015,1493112744680,1493138770296;cn.kuwo.player&d1d03a24&1493109460580;com.android.alarmclock&3a6bc24a&1493109495248,1493113574037,1493147103188;com.android.settings&e49b42d4&1493109801783,1493109835854,1493110147845,1493110230365,1493110320930,1493110344154,1493110350860,1493110796087,1493112348524,1493112850520,1493113933383,1493114212423,1493146022049,1493146963011;
      val dataArr = data.split("\\|")
      val phoneData = dataArr(0).split("&")
      // imei列表
      val imeis = phoneData(0).split("-").mkString(";")
      // 品牌
      val phoneBrand = phoneData(1)
      // 机型
      val phoneModel = phoneData(2)

      val appData = dataArr(1).split(";")
      val appArr = ArrayBuffer[String]()
      for (app <- appData) {
        val appDetail = app.split("&")
        // 时间戳列表
        val timestamps = appDetail(2).split(",")
        // 点击次数
        val clickTimes = timestamps.length
        appArr += app + "&" + clickTimes
      }

      // imei有数据，app有数据
      if (imeis.length > 0 && appArr.length > 0) {
        sb.append(imeis)
        sb.append("|")
        sb.append(if (StringUtils.isNotBlank(ip)) ip else " ")
        sb.append("|")
        sb.append(serverTimestamp)
        sb.append("|")
        sb.append(if (StringUtils.isNotBlank(serverTime)) serverTime else " ")
        sb.append("|")
        sb.append(if (StringUtils.isNotBlank(phoneBrand)) phoneBrand else " ")
        sb.append("|")
        sb.append(if (StringUtils.isNotBlank(phoneModel)) phoneModel else " ")
        sb.append("|")
        sb.append(appArr.mkString(";"))
      }

      data = sb.toString()

    } catch {
      case e: Exception =>
        e.printStackTrace()
        data = ""
    }

    new Tuple2[String, String](key, data)
  }

}



