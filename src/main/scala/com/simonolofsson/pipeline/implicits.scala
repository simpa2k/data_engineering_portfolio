package com.simonolofsson.pipeline

import com.simonolofsson.util.{PathUtil, SilverTable}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.{col, expr, regexp_replace}
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.util.UUID

object implicits {

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

    def mergeIntoSilver(silverTable: SilverTable, keyColumns: Seq[String], maybeWhenMatchedCondition: Option[String] = None, doNotUpdateColumns: Set[String] = Set.empty): Unit = {
      val silverTablePath = silverTable.path
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

        // Remove the set of columns to not update from all columns
        val columnsToUpdate = df.schema.map(_.name).filter(column => !doNotUpdateColumns.contains(column))
        // The merge API expects mappings of the columns in the delta table to expressions when updating rows.
        // Since we only want to set the existing values to the values of the incoming rows, we
        // simply create pairs of the same column but with different aliases.
        val columnMappings = columnsToUpdate.map(column => s"existing.$column" -> s"incoming.$column").toMap

        deltaMergeMatchedActionBuilder
          .updateExpr(columnMappings)
          .execute()

      } else {
        df.writeSilver(silverTable)
      }
    }

    def writeSilver(silverTable: SilverTable): Unit =
      df
        .write
        .format("delta")
        .mode("append")
        .save(silverTable.path)
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
    def readBronze(dataLakeRootPath: String, tableName: String): DataFrame =
      spark
        .read
        .format("delta")
        .load(s"${PathUtil.bronzePath(dataLakeRootPath)}/$tableName")

    def readBronzeStream(dataLakeRootPath: String, company: String, source: String, tableName: String): DataFrame =
      spark
        .readStream
        .format("delta")
        .load(s"${PathUtil.bronzePath(dataLakeRootPath)}/$company/$source/$tableName")
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
    def mergeIntoSilver(silverTable: SilverTable, pipelineName: String, keyColumns: Seq[String], doNotUpdateColumns: Set[String] = Set.empty): DataStreamWriter[Row] =
      checkpointedSilverStream(silverTable, pipelineName)
        .foreachBatch {
          (newRows: DataFrame, _: Long) =>
            newRows.mergeIntoSilver(silverTable, keyColumns, doNotUpdateColumns = doNotUpdateColumns)
        }

    def mergeIntoSilverWithSurrogateKey(silverTable: SilverTable, pipelineName: String, keyColumns: Seq[String], doNotUpdateColumns: Set[String] = Set.empty): DataStreamWriter[Row] =
      checkpointedSilverStream(silverTable, pipelineName)
        .foreachBatch {
          (newRows: DataFrame, _: Long) =>
            newRows
              .withColumn("id", expr("uuid()")) // TODO: This doesn't need to be here any longer, was only required with monotonically_increasing_id
              .mergeIntoSilver(silverTable, keyColumns, doNotUpdateColumns = doNotUpdateColumns ++ Set("id"))
        }

    private def checkpointedSilverStream(silverTable: SilverTable, pipelineName: String): DataStreamWriter[Row] =
      dataStreamWriter
        .format("delta")
        .option("checkpointLocation", silverTable.checkpointLocation(pipelineName))
  }
}
