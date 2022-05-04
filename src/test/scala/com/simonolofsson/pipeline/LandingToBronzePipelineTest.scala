package com.simonolofsson.pipeline

import com.simonolofsson.testUtil.{SparkUtil, PathUtil}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AnyFreeSpec

class LandingToBronzePipelineTest extends AnyFreeSpec with BeforeAndAfterEach {

  override protected def afterEach(): Unit = PathUtil.removeBronze()

  "A CSV pipeline run" - {
    "should infer schema" in {
      val actualDataFrame = runPipeline("input.csv")

      val expectedDataFrame = getExpectedDataFrame(
        Seq(Row(1, 2, 3)),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType),
        ))
      )

      assertDataFramesEqual(actualDataFrame, expectedDataFrame)
    }

    "should handle different delimiters than comma" in {
      val actualDataFrame = runPipeline("input_semicolon.csv", Map("delimiter" -> ";"))

      val expectedDataFrame = getExpectedDataFrame(
        Seq(Row(1, 2, 3)),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType),
        ))
      )

      assertDataFramesEqual(actualDataFrame, expectedDataFrame)
    }

    "should handle files without header" in {
      val actualDataFrame = runPipeline("input_no_header.csv", Map("header" -> "false"))

      val expectedDataFrame = getExpectedDataFrame(
        Seq(Row(1, 2, 3)),
        StructType(Seq(
          StructField("_1", IntegerType),
          StructField("_2", IntegerType),
          StructField("_3", IntegerType),
        ))
      )

      assertDataFramesEqual(actualDataFrame, expectedDataFrame)
    }

    "should allow columns to be added in subsequent runs" in {

    }
  }

  "A JSON pipeline run" - {
    "should write JSON as delta" in {
      val actualDataFrame = runPipeline("input.json")

      val expectedDataFrame = getExpectedDataFrame(Seq(
        Row(1, "test_value", Row(Seq(1, 2, 3)))
      ),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType),
          StructField("c", StructType(Seq(
            StructField("d", ArrayType(IntegerType))
          )))
        ))
      )

      assertDataFramesEqual(actualDataFrame, expectedDataFrame)
    }
  }

  "A pipeline for an unsupported file format" - {
    "should crash" in {
      val e = intercept[IllegalArgumentException] {
        new LandingToBronzePipeline().apply(SparkUtil.spark, PathUtil.dataLakeRootPath, "input.txt", Map.empty)
      }

      assert(e.getMessage == "Unsupported file format detected. The extension of the file passed was txt but the supported ones are csv,json")
    }
  }

  private def getExpectedDataFrame(rows: Seq[Row], schema: StructType): DataFrame = {
    val schemaWithMetadataColumns = StructType(
      schema.fields ++ Seq(
        StructField("meta_pipeline_run_id", StringType)
      )
    )

    val rowsWithMetadata = rows.map(row => Row((row.toSeq :+ "test_pipeline"): _*))

    SparkUtil.createDataFrame(rowsWithMetadata, schemaWithMetadataColumns)
  }

  private def assertDataFramesEqual(actualDataFrame: DataFrame, expectedDataFrame: DataFrame): Unit = {
    val actualWithoutMetadataColumns = removeMetadataColumns(actualDataFrame)
    val expectedWithoutMetadataColumns = removeMetadataColumns(expectedDataFrame)

    SparkUtil.assertDataFramesEqual(actualWithoutMetadataColumns, expectedWithoutMetadataColumns)

    val actualMetadataColumns = selectMetadataColumns(actualDataFrame).collect
    actualMetadataColumns.foreach(row => {
      // Make sure meta_pipeline_run_id is valid UUID, otherwise we'll get an exception
      val pipelineRunId = row.getAs[String]("meta_pipeline_run_id")
      java.util.UUID.fromString(pipelineRunId) // TODO: Better message if this fails
    })
  }

  private def removeMetadataColumns(df: DataFrame): DataFrame =
    df.drop("meta_pipeline_run_id")

  private def selectMetadataColumns(df: DataFrame): DataFrame =
    df.select("meta_pipeline_run_id")

  private def runPipeline(filename: String, metadata: Map[String, String] = Map.empty): DataFrame = {
    new LandingToBronzePipeline().apply(SparkUtil.spark, PathUtil.dataLakeRootPath, filename, metadata)
    SparkUtil.readBronzeTable(PathUtil.removeExtension(filename))
  }
}
