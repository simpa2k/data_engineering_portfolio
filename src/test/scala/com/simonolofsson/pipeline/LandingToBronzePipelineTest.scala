package com.simonolofsson.pipeline

import com.simonolofsson.testUtil.{PathUtil, SparkUtil}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AnyFreeSpec

import java.nio.file.{Files, Paths, StandardCopyOption}

class LandingToBronzePipelineTest extends AnyFreeSpec with BeforeAndAfterEach {

  override protected def afterEach(): Unit = {
    PathUtil.removeBronze()
    PathUtil.emptyLandingFolder("generic")
  }

  "A CSV pipeline run" - {
    "should infer schema" in {
      val actualDataFrame = copyFilesAndRunPipeline("input.csv")

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
      val actualDataFrame = copyFilesAndRunPipeline("input_semicolon.csv", Map("delimiter" -> ";"))

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
      val actualDataFrame = copyFilesAndRunPipeline("input_no_header.csv", Map("header" -> "false"))

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
      // TODO: Implement
    }
  }

  "A JSON pipeline run" - {
    "should write JSON as delta" in {
      val actualDataFrame = copyFilesAndRunPipeline("input.json")

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
        copyFilesAndRunPipeline("input.txt")
      }

      assert(e.getMessage == "Unsupported file format detected. The extension of the file passed was txt but the supported ones are csv,json")
    }
  }

  "A pipeline for any format" - {
    "should add fields from companion metadata file" in {
      val actualDataFrame = copyFilesAndRunPipeline("input.csv")
      val expectedMetadataRows = SparkUtil.createDataFrame(Seq(
        Row("company_a", "system_a")
      ),
        StructType(Seq(
          StructField("meta_company", StringType),
          StructField("meta_source", StringType)
        ))
      )

      val actualMetadataRows = actualDataFrame.select("meta_company", "meta_source")
      SparkUtil.assertDataFramesEqual(actualMetadataRows, expectedMetadataRows)
    }

    "should fail if required metadata fields are not provided" in {
      val e = intercept[IllegalArgumentException] { // TODO: Maybe not the most appropriate exception class
        copySourceFileFromStaging("input.csv")
        copyMetadataCompanionFileFromStaging("invalid_metadata.json")
        runPipeline("input.csv")
      }

      assert(e.getMessage == "Missing required metadata fields [company,source]. Please add these fields to the metadata.json file in the landing folder")
    }
  }

  private def getExpectedDataFrame(rows: Seq[Row], schema: StructType): DataFrame =
    SparkUtil.createDataFrame(rows, schema)

  private def assertDataFramesEqual(actualDataFrame: DataFrame, expectedDataFrame: DataFrame): Unit = {
    val actualWithoutMetadataColumns = removeMetadataColumns(actualDataFrame)
    val expectedWithoutMetadataColumns = removeMetadataColumns(expectedDataFrame)

    SparkUtil.assertDataFramesEqual(actualWithoutMetadataColumns, expectedWithoutMetadataColumns)
  }

  private def removeMetadataColumns(df: DataFrame): DataFrame =
    df.drop("meta_company", "meta_source")

  private def copySourceFileFromStaging(filename: String): Unit = {
    Files.copy(
      Paths.get(s"${PathUtil.dataLakeRootPath}/testDataStaging/$filename"),
      Paths.get(s"${PathUtil.dataLakeRootPath}/landing/generic/$filename"),
      StandardCopyOption.REPLACE_EXISTING
    )
  }

  private def copyMetadataCompanionFileFromStaging(sourceFilename: String = "metadata.json"): Unit =
    Files.copy(
      Paths.get(s"${PathUtil.dataLakeRootPath}/testDataStaging/$sourceFilename"),
      Paths.get(s"${PathUtil.dataLakeRootPath}/landing/generic/metadata.json"),
      StandardCopyOption.REPLACE_EXISTING
    )

  private def runPipeline(filename: String, metadata: Map[String, String] = Map.empty): DataFrame = {
    new LandingToBronzePipeline().apply(SparkUtil.spark, PathUtil.dataLakeRootPath, "generic", filename, metadata)
    SparkUtil.readBronzeTable(s"company_a/system_a/${PathUtil.removeExtension(filename)}")
  }

  private def copyFilesAndRunPipeline(filename: String, metadata: Map[String, String] = Map.empty): DataFrame = {
    copySourceFileFromStaging(filename)
    copyMetadataCompanionFileFromStaging()

    runPipeline(filename, metadata)
  }
}
