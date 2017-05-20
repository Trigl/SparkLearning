package com.trigl.spark.main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql._

/**
  * 评测ALS算法，得出最优参数
  * created by Trigl at 2017-05-11 13:48
  */
object ALSEvaluation {

  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("ALSEvaluation")
    val spark = SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("use arrival")
    // 从Hive表中查出原始数据
    val rawUserAppData = sql("select imei as user,pkg as app,rating,lasttimestamp from mid_recommend_rating where year='2017' and month='05' and day='10'")

    val ratings = rawUserAppData.map(row => {
      val userid = row.getString(0).hashCode
      val itemid = row.getString(1).hashCode
      val rating = row.getDouble(2)
      val time = row.getLong(3) % 10
      (userid, itemid, rating, time)
    }).toDF("userid", "itemid", "rating", "time")

    // 将评分数据根据时间戳最后一位分成两份份： train (80%), validation (20%)，两份数据都cache，因为后面会多次用到

    val training = ratings.filter("time < 8").cache()
    val numTraining = training.count()
    val validation = ratings.filter("time >= 8").cache()
    val numValidation = validation.count()

    println("Training: " + numTraining + ", validation: " + numValidation)

    getBestModel(training, validation)

    spark.stop()

  }

  /**
    * 调参得出最优模型
 *
    * @param training
    * @param validation
    * @return
    */
  def getBestModel(training: DataFrame, validation: DataFrame): Option[ALSModel] = {

    // 训练不同参数下的模型，并在校验集中验证，获取最佳参数下的模型
    val ranks = List(8, 12)
    val lambdas = List(0.01, 0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel: Option[ALSModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val als = new ALS()
        .setRank(rank)
        .setMaxIter(numIter)
        .setRegParam(lambda)
        .setUserCol("userid")
        .setItemCol("itemid")
        .setRatingCol("rating")
      val model = als.fit(training)
      // 必须过滤，因为会产生NaN预测数据，详情见：https://issues.apache.org/jira/browse/SPARK-14489
      val predictions = model.transform(validation).filter("prediction < 1000")
      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("rating")
        .setPredictionCol("prediction")
      val rmse = evaluator.evaluate(predictions)
      println("RMSE (validation) = " + rmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      // rmse越小预测结果越准确
      if (rmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = rmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }
    val predictions = bestModel.get.transform(validation).filter("prediction < 1000")

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val validationRmse = evaluator.evaluate(predictions)


    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda  + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + validationRmse + ".")

    val meanRating = training.union(validation).rdd.map(_(2).toString().toFloat).mean
    val baselineRmse =
      math.sqrt(validation.rdd.map(x => (meanRating - x(2).toString().toFloat) * (meanRating - x(2).toString().toFloat)).mean)
    val improvement = (baselineRmse - validationRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    bestModel
  }

}
