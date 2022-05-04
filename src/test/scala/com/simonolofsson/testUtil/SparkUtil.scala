package com.simonolofsson.testUtil

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkUtil {

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .getOrCreate()

  def createDataFrame(rows: Seq[Row], schema: StructType): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )

  def assertDataFramesEqual(actual: DataFrame, expected: DataFrame): Unit = {
    val collectedActual = collectAndSort(actual)
    val collectedExpected = collectAndSort(expected)

    assert(collectedActual == collectedExpected)
  }

  def readBronzeTable(tableName: String): DataFrame = spark.read.format("delta").load(s"${PathUtil.bronzePath}/$tableName")

  def readSilverTable(tableName: String): DataFrame = spark.read.format("delta").load(s"${PathUtil.silverPath}/$tableName")

  def writeBronzeTable(df: DataFrame, tableName: String): Unit =
    df
      .write
      .format("delta")
      .save(s"${PathUtil.bronzePath}/$tableName")

  private def collectAndSort(df: DataFrame): Seq[Row] = df.collect().toSeq.sortBy(_.toSeq.mkString(""))
}
