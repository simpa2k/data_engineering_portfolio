package com.simonolofsson.pipeline

import com.simonolofsson.testUtil.{SparkUtil, PathUtil}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AnyFreeSpec

class TimeseriesExamplePipelineTest extends AnyFreeSpec with BeforeAndAfterEach {

  override protected def afterEach(): Unit = {
    PathUtil.removeBronze()
    PathUtil.removeSilver()
  }

  "A timeseries example pipeline" - {
    "should zip and flatten dates and values" in {
      val bronzeTable = SparkUtil.createDataFrame(
        Seq(
          Row(
            Seq(
              "2022-04-01", "2022-04-02", "2022-04-03", "2022-04-04", "2022-04-05",
              "2022-04-06", "2022-04-07", "2022-04-08", "2022-04-09", "2022-04-10"
            ),
            Seq(
              0.50, 1.20, 5.60, 2.30, 0.90,
              4.70, 1.23, 8.09, 6.70, 9.50
            ),
            "2022-04-24"
          )
        ),
        StructType(Seq(
          StructField("dates", ArrayType(StringType)),
          StructField("values", ArrayType(DoubleType)),
          StructField("date_loaded", StringType),
        ))
      )

      SparkUtil.writeBronzeTable(bronzeTable, "timeseries_example")

      val actualDataFrame = runPipeline
      val expectedDataFrame = SparkUtil.createDataFrame(
        Seq(
          Row("2022-04-01", 0.50, "2022-04-24"), Row("2022-04-02", 1.20, "2022-04-24"), Row("2022-04-03", 5.60, "2022-04-24"), Row("2022-04-04", 2.30, "2022-04-24"), Row("2022-04-05", 0.90, "2022-04-24"),
          Row("2022-04-06", 4.70, "2022-04-24"), Row("2022-04-07", 1.23, "2022-04-24"), Row("2022-04-08", 8.09, "2022-04-24"), Row("2022-04-09", 6.70, "2022-04-24"), Row("2022-04-10", 9.50, "2022-04-24")
        ),
        StructType(Seq(
          StructField("date", StringType),
          StructField("value", DoubleType),
          StructField("date_loaded", StringType)
        ))
      )

      SparkUtil.assertDataFramesEqual(actualDataFrame, expectedDataFrame)
    }

    "should upsert new and corrected values" in {
      val firstBronzeTable = SparkUtil.createDataFrame(
        Seq(
          Row(
            Seq(
              "2022-04-01", "2022-04-02", "2022-04-03", "2022-04-04", "2022-04-05",
              "2022-04-06", "2022-04-07", "2022-04-08", "2022-04-09", "2022-04-10"
            ),
            Seq(
              0.50, 1.20, 5.60, 2.30, 0.90,
              4.70, 1.23, 8.09, 6.70, 9.50
            ),
            "2022-04-24" // TODO: Should we have this in bronze already?
          )
        ),
        StructType(Seq(
          StructField("dates", ArrayType(StringType)),
          StructField("values", ArrayType(DoubleType)),
          StructField("date_loaded", StringType)
        ))
      )

      SparkUtil.writeBronzeTable(firstBronzeTable, "timeseries_example")

      val firstActualDataFrame = runPipeline
      val firstExpectedDataFrame = SparkUtil.createDataFrame(
        Seq(
          Row("2022-04-01", 0.50, "2022-04-24"), Row("2022-04-02", 1.20, "2022-04-24"), Row("2022-04-03", 5.60, "2022-04-24"), Row("2022-04-04", 2.30, "2022-04-24"), Row("2022-04-05", 0.90, "2022-04-24"),
          Row("2022-04-06", 4.70, "2022-04-24"), Row("2022-04-07", 1.23, "2022-04-24"), Row("2022-04-08", 8.09, "2022-04-24"), Row("2022-04-09", 6.70, "2022-04-24"), Row("2022-04-10", 9.50, "2022-04-24")
        ),
        StructType(Seq(
          StructField("date", StringType),
          StructField("value", DoubleType),
          StructField("date_loaded", StringType)
        ))
      )

      SparkUtil.assertDataFramesEqual(firstActualDataFrame, firstExpectedDataFrame)

      val secondBronzeTable = SparkUtil.createDataFrame(
        Seq(
          Row(
            Seq(
              "2022-04-01", "2022-04-02", "2022-04-03", "2022-04-04", "2022-04-05",
              "2022-04-06", "2022-04-07", "2022-04-08", "2022-04-09", "2022-04-10",
              "2022-04-11", "2022-04-12"
            ),
            Seq(
              0.50, 1.20, 5.60, 2.30, 1.90,
              4.70, 1.23, 8.09, 6.80, 9.50,
              3.54, 9.76
            ),
            "2022-04-25"
          )
        ),
        StructType(Seq(
          StructField("dates", ArrayType(StringType)),
          StructField("values", ArrayType(DoubleType)),
          StructField("date_loaded", StringType)
        ))
      )

      // TODO: This second table should probably be more realistic, so that we know that we only process the relevant data
      PathUtil.removeBronze()
      SparkUtil.writeBronzeTable(secondBronzeTable, "timeseries_example")

      val secondActualDataFrame = runPipeline
      val secondExpectedDataFrame = SparkUtil.createDataFrame(
        Seq(
          Row("2022-04-01", 0.50, "2022-04-24"), Row("2022-04-02", 1.20, "2022-04-24"), Row("2022-04-03", 5.60, "2022-04-24"), Row("2022-04-04", 2.30, "2022-04-24"), Row("2022-04-05", 1.90, "2022-04-25"),
          Row("2022-04-06", 4.70, "2022-04-24"), Row("2022-04-07", 1.23, "2022-04-24"), Row("2022-04-08", 8.09, "2022-04-24"), Row("2022-04-09", 6.80, "2022-04-25"), Row("2022-04-10", 9.50, "2022-04-24"),
          Row("2022-04-11", 3.54, "2022-04-25"), Row("2022-04-12", 9.76, "2022-04-25")
        ),
        StructType(Seq(
          StructField("date", StringType),
          StructField("value", DoubleType),
          StructField("date_loaded", StringType)
        ))
      )

      SparkUtil.assertDataFramesEqual(secondActualDataFrame, secondExpectedDataFrame)
    }
  }

  private def runPipeline: DataFrame = {
    new TimeseriesExamplePipeline().apply(SparkUtil.spark, PathUtil.dataLakeRootPath)
    SparkUtil.readSilverTable("timeseries_example")
  }
}
