package com.epam.ki

import com.epam.ki.Utils.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Lev_Khacheresiantc on 8/15/2016.
  */


object MLTest {
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

  val LOG: Logger = Logger.getLogger(MLTest.getClass)

  val FEATURES_COL = "features"
  val CATEGORICAL_COLUMNS = (2 to 4).toArray[Int] ++ (7 to 14).toArray[Int] ++ (16 to 20).toArray[Int]

  val FEATURES_SCHEMA = StructType((1 to 51).map(index => StructField(s"C$index", DoubleType, nullable = true)))
  val LABELS_SCHEMA = StructType(Seq(StructField("label", DoubleType, nullable = false)))

  val DATA_FILE_NAME = "Objects.csv"
  val LABELS_FILE_NAME = s"Target.csv"


  def main(args: Array[String]) {

    require(args.length == 2, "Application requered 2 parameters: \n 1st - files folder \n 2nd - result folder")

    val filesLocation = args(0)
    val resultLocation = args(1)


    val conf = new SparkConf().setAppName("ML App (HW-3)")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val utils = new CommonUtils(sqlContext)


    val features = utils.getDataFrameFrom(s"$filesLocation/$DATA_FILE_NAME", FEATURES_SCHEMA)
    LOG.info(s" Features size = ${features.count()}")

    val labels = utils.getDataFrameFrom(s"$filesLocation/$LABELS_FILE_NAME", LABELS_SCHEMA)
    LOG.info(s"Labels size = ${labels.count()}")

    val (trainingDF, testDF) = utils.preparedDataForModel(features, labels, CATEGORICAL_COLUMNS)
    LOG.info(s"Training size = ${trainingDF.count()} and testDF size = ${testDF.count()}")


    testDF.cache
    trainingDF.cache


    val algorithm = new LogisticRegression().setMaxIter(10)



    val pipeline = new Pipeline().setStages(Array(columnsToVector, algorithm))

    val paramGrid = new ParamGridBuilder()
      .addGrid(algorithm.regParam, Array(0.1, 0.01))
      .addGrid(algorithm.fitIntercept)
      .addGrid(algorithm.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    // Run cross-validation, and choose the best set of parameters.
    LOG.info("Train model")
    val cvModel = cv.fit(trainingDF)

    LOG.info("Test model")
    val result = cvModel.transform(testDF)

    result.select("id", "label", "probability", "rawPrediction", "prediction").show(false)

    val scoreAndLabels = result.select("rawPrediction", "label")
      .map { case Row(rawPrediction: Vector, label: Double) =>
        (rawPrediction(1), label)
      }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    LOG.info(s"Area under ROC vie BCM = ${metrics.areaUnderROC()}")

    metrics.roc.map({ case (x, y) => s"$x,$y" })
      .repartition(1)
      .saveAsTextFile(s"$resultLocation/result")

  }

  def columnsToVector = new VectorAssembler().setInputCols((1 to 51).toArray.map(i => {
    if (CATEGORICAL_COLUMNS.contains(i))
      s"C${i}_OME"
    else
      s"C$i"
  })).setOutputCol(FEATURES_COL)


}

