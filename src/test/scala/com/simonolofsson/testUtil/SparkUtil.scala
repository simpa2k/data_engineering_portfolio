package com.simonolofsson.testUtil

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkUtil {

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .config("spark.default.parallelism", "1") // To speed up tests and simplify test assertions on id columns
      .getOrCreate()

  def createDataFrame(rows: Seq[Row], schema: StructType): DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )

  def assertDataFramesEqual(actual: DataFrame, expected: DataFrame, ignoreColumns: Set[String] = Set.empty): Unit = {
    val collectedActual = collectAndSort(actual, ignoreColumns)
    val collectedExpected = collectAndSort(expected, ignoreColumns)

    assert(collectedActual == collectedExpected)
  }

  def readBronzeTable(tableName: String): DataFrame = spark.read.format("delta").load(s"${PathUtil.bronzePath}/$tableName")

  def readSilverTable(tableName: String): DataFrame = spark.read.format("delta").load(s"${PathUtil.silverPath}/$tableName")

  def writeBronzeTable(df: DataFrame, tableName: String): Unit =
    df
      .write
      .format("delta")
      .save(s"${PathUtil.bronzePath}/$tableName")

  private def collectAndSort(df: DataFrame, columnsToIgnore: Set[String]): Seq[Row] = {
    val columnsToCollect = df
      .schema
      .map(_.name)
      .filter(column => !columnsToIgnore.contains(column))
      .map(column => df(column))

    df
      .select(columnsToCollect:_*)
      .collect()
      .toSeq
      .sortBy(_.toSeq.mkString(""))
  }
}
