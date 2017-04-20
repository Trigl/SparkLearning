package com.trigl.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计hdfs文件行数
  * Created by Trigl on 2017/4/20.
  */
object SparkDemo {

  // args:/test/test.log
  def main(args: Array[String]) {

    // 设置Spark的序列化方式
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 初始化Spark
    val sparkConf = new SparkConf().setAppName("CountDemo")
    val sc = new SparkContext(sparkConf)

    // 读取文件
    val rdd = sc.textFile(args(0))

    println(args(0) + "的行数为：" + rdd.count())

    sc.stop()
  }
}
