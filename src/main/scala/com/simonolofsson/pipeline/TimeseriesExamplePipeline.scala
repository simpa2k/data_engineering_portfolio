package com.simonolofsson.pipeline

import com.simonolofsson.pipeline.TimeseriesExamplePipeline.TimeseriesExampleTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{arrays_zip, explode}

class TimeseriesExamplePipeline {

  def apply(spark: SparkSession, dataLakeRootPath: String): Unit = {
    import spark.implicits._
    spark
      .readBronze(dataLakeRootPath, TimeseriesExampleTable)
      .withColumn("zipped", arrays_zip($"dates", $"values"))
      .select(explode($"zipped").as("pairs"), $"date_loaded")
      .withColumn("date", $"pairs.dates")
      .withColumn("value", $"pairs.values")
      .select("date", "value", "date_loaded")
      .mergeIntoSilver(dataLakeRootPath, TimeseriesExampleTable, Seq("date"), Some("existing.value != incoming.value"))
  }
}

object TimeseriesExamplePipeline {
  final val TimeseriesExampleTable = "timeseries_example"
}
