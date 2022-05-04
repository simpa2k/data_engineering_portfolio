package com.simonolofsson

import com.simonolofsson.util.PathUtil
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

package object pipeline {

  /**
   * Adds a set of utility methods to a Spark data frame.
   *
   * The reason for using an implicit class is that we then can continue to use the fluid interface
   * of the original Spark DataFrame which looks nice and is very readable. We would lose a measure
   * of this readability if we instead used utility methods that took a Data Frame as an argument.
   *
   * However, implicit classes are sort of an advanced feature of Scala and it's not always clear where
   * the added methods are coming from, especially when defining the implicit class in a package object.
   * Therefore, I would only use this approach in a setting where I can count on the other members of
   * the team to understand the code properly.
   *
   * @param df The DataFrame to be extended with utility methods
   */
  implicit class ExtendedDataFrame(val df: DataFrame) {
    def withColumnFormattedAsDate(column: String): DataFrame =
      df.withColumn(
        column,
        regexp_replace(
          col(column).cast("string"),
          "([0-9]{4})([0-9]{2})([0-9]{2})",
          "$1-$2-$3"
        ).cast("date")
      )

    def mergeIntoSilver(dataLakeRootPath: String, tableName: String, keyColumns: Seq[String], maybeWhenMatchedCondition: Option[String] = None): Unit = {
      val silverTablePath = s"${PathUtil.silverPath(dataLakeRootPath)}/$tableName"
      if (DeltaTable.isDeltaTable(silverTablePath)) {
        val existingTable = DeltaTable.forPath(silverTablePath)
        val deltaMergeBuilder = existingTable
          .as("existing")
          .merge(df.alias("incoming"), keyColumns.map(c => s"existing.$c = incoming.$c").mkString(" AND "))
          .whenNotMatched().insertAll()

        val deltaMergeMatchedActionBuilder = maybeWhenMatchedCondition match {
          case Some(whenMatchedCondition) => deltaMergeBuilder.whenMatched(whenMatchedCondition)
          case None => deltaMergeBuilder.whenMatched()
        }

        deltaMergeMatchedActionBuilder
          .updateAll()
          .execute()

      } else {
        df.writeSilver(dataLakeRootPath, tableName)
      }
    }

    def writeSilver(dataLakeRootPath: String, tableName: String): Unit =
      df
        .write
        .format("delta")
        .mode("append")
        .save(s"${PathUtil.silverPath(dataLakeRootPath)}/$tableName")
  }

  /**
   * Adds a set of utility methods to a SparkSession.
   *
   * The reason for using an implicit class is that we then can continue to use the fluid interface
   * of the original SparkSession which looks nice and is very readable. We would lose a measure
   * of this readability if we instead used utility methods that took a SparkSession as an argument.
   *
   * However, implicit classes are sort of an advanced feature of Scala and it's not always clear where
   * the added methods are coming from, especially when defining the implicit class in a package object.
   * Therefore, I would only use this approach in a setting where I can count on the other members of
   * the team to understand the code properly.
   *
   * @param spark The SparkSession to be extended with utility methods
   */
  implicit class ExtendedSparkSession(val spark: SparkSession) {
    def readBronzeStreamIfExists(dataLakeRootPath: String, tableName: String): Option[DataFrame] = {
      val bronzeTablePath = s"${PathUtil.bronzePath(dataLakeRootPath)}/$tableName"
      if (DeltaTable.isDeltaTable(bronzeTablePath)) {
        Some(readBronzeStream(dataLakeRootPath, tableName))
      } else
        None
    }

    def readBronze(dataLakeRootPath: String, tableName: String): DataFrame =
      spark
        .read
        .format("delta")
        .load(s"${PathUtil.bronzePath(dataLakeRootPath)}/$tableName")

    def readBronzeStream(dataLakeRootPath: String, tableName: String): DataFrame =
      spark
        .readStream
        .format("delta")
        .load(s"${PathUtil.bronzePath(dataLakeRootPath)}/$tableName")
  }

  /**
   * Adds a set of utility methods to a DataStreamWriter
   *
   * The reason for using an implicit class is that we then can continue to use the fluid interface
   * of the original DataStreamWriter which looks nice and is very readable. We would lose a measure
   * of this readability if we instead used utility methods that took a DataStreamWriter as an argument.
   *
   * However, implicit classes are sort of an advanced feature of Scala and it's not always clear where
   * the added methods are coming from, especially when defining the implicit class in a package object.
   * Therefore, I would only use this approach in a setting where I can count on the other members of
   * the team to understand the code properly.
   *
   * @param dataStreamWriter The DataStreamWriter to be extended with utility methods
   */
  implicit class ExtendedDataStreamWriter(val dataStreamWriter: DataStreamWriter[Row]) {
    def mergeIntoSilver(dataLakeRootPath: String, tableName: String, keyColumns: String*): DataStreamWriter[Row] = {
      dataStreamWriter
        .format("delta")
        .option("checkpointLocation", s"${PathUtil.silverPath(dataLakeRootPath)}/${tableName}_checkpoint")
        .foreachBatch((newRows: DataFrame, batchId: Long) => {
          newRows.mergeIntoSilver(dataLakeRootPath, tableName, keyColumns)
        })
    }
  }
}
