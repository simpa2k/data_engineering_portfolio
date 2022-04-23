package pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class StreamingPipeline {

  def apply(spark: SparkSession, dataLakeRootPath: String): Unit = {
    val schema = StructType(Seq(
      StructField("a", StringType),
      StructField("b", IntegerType)
    ))

    val df = spark
      .readStream
      .schema(schema)
      .json(s"$dataLakeRootPath/first_pipeline")
      .withColumn("date_col", col("a").cast("timestamp"))
      .withColumn("rolling_average", window(col("date_col"), "1 second"))

    val query = df
      .writeStream
      .format("console")
      .option("truncate", "false")
//      .format("parquet")
//      .option("path", "first_pipeline_output")
//      .option("checkpointLocation", "_checkpoint")
      .trigger(Trigger.Once())
      .start()

    query.awaitTermination()
  }
}
