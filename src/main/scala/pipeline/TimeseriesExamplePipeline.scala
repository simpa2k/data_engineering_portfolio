package pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{arrays_zip, explode}
import io.delta.tables._

class TimeseriesExamplePipeline {

  def apply(spark: SparkSession, dataLakeRootPath: String): Unit = {
    import spark.implicits._
    val zippedAndExploded = spark
      .read
      .format("delta")
      .load(s"${PathUtil.bronzePath(dataLakeRootPath)}/timeseries_example")
      .withColumn("zipped", arrays_zip($"dates", $"values"))
      .select(explode($"zipped").as("pairs"), $"date_loaded")
      .withColumn("date", $"pairs.dates")
      .withColumn("value", $"pairs.values")
      .select("date", "value", "date_loaded")

    val silverTablePath = s"${PathUtil.silverPath(dataLakeRootPath)}/timeseries_example"
    if (DeltaTable.isDeltaTable(silverTablePath)) {
      val existingTable = DeltaTable.forPath(silverTablePath)
      existingTable
        .as("existing")
        .merge(zippedAndExploded.alias("incoming"), "existing.date = incoming.date")
        .whenMatched("existing.value != incoming.value").updateAll()
        .whenNotMatched().insertAll()
        .execute()
    } else {
      zippedAndExploded
        .write
        .format("delta")
        .save(silverTablePath)
    }
  }
}
