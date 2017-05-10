package com.trigl.spark.main

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark MLlib协同过滤算法示例
  * 引用自官网：http://spark.apache.org/docs/latest/mllib-collaborative-filtering.html
  * created by Trigl at 2017-05-09 17:40
  */
object ALSDemo {

  def main(args: Array[String]) {

    // 设置Spark的序列化方式
    System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // 初始化Spark
    val sparkConf = new SparkConf().setAppName("ALSDemo")
    val sc = new SparkContext(sparkConf)

    // 加载解析数据
    val data = sc.textFile("file:/data/install/spark-2.0.0-bin-hadoop2.7/data/mllib/als/test.data")
    val ratings = data.map(_.split(',') match {
      case Array(user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble) // 注意用户和物品必须是整数，评分是双精度浮点数
    })

    // 使用ALS建立推荐模型
    val rank = 10 // 最后推荐的物品数
    val numIterations = 10 // 迭代次数
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    // 评测模型的准确度，正常情况下我们需要一个训练集产生一个模型，一个测试集评测模型
    // 这里我们的训练集和测试集使用了同一份数据
    // 用户物品集
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    // 预测集
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    // 实际评分和预测评分集
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)

    // 保存和加载模型
    val myModelPath = "file:/home/hadoop/model"
    model.save(sc, myModelPath)
    val sameModel = MatrixFactorizationModel.load(sc, myModelPath)
  }


}
