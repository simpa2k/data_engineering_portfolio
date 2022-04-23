import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AnyFreeSpec
import pipeline.LandingToBronzePipeline

class LandingToBronzePipelineTest extends AnyFreeSpec with BeforeAndAfterEach {

  override protected def afterEach(): Unit = PathUtil.removeBronze()

  "A CSV pipeline run" - {
    "should infer schema" in {
      val actualDataFrame = runPipeline("input.csv")

      val expectedDataFrame = SparkUtil.createDataFrame(
        Seq(Row(1, 2, 3)),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType),
        ))
      )

      SparkUtil.assertDataFramesEqual(actualDataFrame, expectedDataFrame)
    }

    "should handle different delimiters than comma" in {
      val actualDataFrame = runPipeline("input_semicolon.csv", Map("delimiter" -> ";"))

      val expectedDataFrame = SparkUtil.createDataFrame(
        Seq(Row(1, 2, 3)),
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", IntegerType),
          StructField("c", IntegerType),
        ))
      )

      SparkUtil.assertDataFramesEqual(actualDataFrame, expectedDataFrame)
    }

    "should handle files without header" in {
      val actualDataFrame = runPipeline("input_no_header.csv", Map("header" -> "false"))

      val expectedDataFrame = SparkUtil.createDataFrame(
        Seq(Row(1, 2, 3)),
        StructType(Seq(
          StructField("_1", IntegerType),
          StructField("_2", IntegerType),
          StructField("_3", IntegerType),
        ))
      )

      SparkUtil.assertDataFramesEqual(actualDataFrame, expectedDataFrame)
    }

    "should allow columns to be added in subsequent runs" in {

    }
  }

  "A JSON pipeline run" - {
    "should write JSON as delta" in {
      val actualDataFrame = runPipeline("input.json")

      val expectedDataFrame = SparkUtil.createDataFrame(Seq(
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

      SparkUtil.assertDataFramesEqual(actualDataFrame, expectedDataFrame)
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

  private def runPipeline(filename: String, metadata: Map[String, String] = Map.empty): DataFrame = {
    new LandingToBronzePipeline().apply(SparkUtil.spark, PathUtil.dataLakeRootPath, filename, metadata)
    SparkUtil.readBronzeTable(PathUtil.removeExtension(filename))
  }

}
