package com.epam.ki.Utils

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}

/**
  * Created by Lev_Khacheresiantc on 9/1/2016.
  */
class CommonUtils(sqlContext: SQLContext) {

  val CSV: String = "com.databricks.spark.csv"
  val OPTIONS = Map("header" -> "false", "nullValue" -> "NaN", "delimiter" -> ";")

  def getDataFrameFrom(fileLocation: String, schema: StructType): DataFrame = {

    sqlContext.read
      .format(CSV)
      .schema(schema)
      .options(OPTIONS)
      .load(fileLocation)

  }


  def preparedDataForModel(features: DataFrame, labels: DataFrame,
                           featureColumnsCategoricalType: Array[Int]): (DataFrame, DataFrame) = {

    val indexedFeatures = createIndex(features, features.schema)
    indexedFeatures.show(false)
    val indexedLabels = createIndex(labels, labels.schema)
    indexedLabels.show(false)

    val featuresWithEncodedColumns = encodedCategorialColumns(
      featureColumnsCategoricalType, indexedFeatures.na.fill(0.0))
      .join(indexedLabels, "id")

    randomSplit(featuresWithEncodedColumns)
  }

  private[this] def createIndex(dataFrame: DataFrame, schema: StructType): DataFrame = {
    val indexedRdd = dataFrame
      .rdd.zipWithIndex()
      .map({ case (row: Row, id: Long) => Row.fromSeq(row.toSeq :+ id) })

    val schemaWithIndex = schema.add(StructField("id", LongType, nullable = false))

    sqlContext.createDataFrame(indexedRdd, schemaWithIndex)
  }


  private[this] def randomSplit(dataFrame: DataFrame): (DataFrame, DataFrame) = {

    val data = dataFrame.randomSplit(Array(0.7, 0.3))
    (data(0), data(1))
  }

  private[this] def encodedCategorialColumns(indeces: Array[Int], dataFrame: DataFrame): DataFrame = {

    var df = dataFrame

    for (index <- indeces) {
      val encoder = new OneHotEncoder()
        .setInputCol(s"C${index}")
        .setOutputCol(s"C${index}_OME")

      df = encoder.transform(df)
    }
    df
  }


}
