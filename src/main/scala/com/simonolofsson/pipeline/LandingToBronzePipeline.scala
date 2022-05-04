package com.simonolofsson.pipeline
import com.simonolofsson.util.PathUtil
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr

class LandingToBronzePipeline {

  private def readMethodsByFileExtension = Map(
    "csv" -> readCsv _,
    "json" -> readJson _
  )

  def apply(spark: SparkSession, dataLakeRootPath: String, filename: String, metadata: Map[String, String] = Map.empty): Unit = {
    val (filenameWithoutExtension, extension) = splitFilenameAndExtension(filename)
    val targetTable = metadata.getOrElse("target_table", filenameWithoutExtension) // TODO: Constant for target_table
    readMethodsByFileExtension
      .get(extension)
      .map(readFunction => readFunction(spark, s"${PathUtil.landingPath(dataLakeRootPath)}/$filename", metadata))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Unsupported file format detected. The extension of the file passed was $extension but the supported ones are ${readMethodsByFileExtension.keys.mkString(",")}"
        )
      )
//      .withColumn("meta_pipeline_run_id", expr("uuid()")) TODO: This needs to be the same literal for all rows
      .write
      .format("delta")
      .mode("append")
      .save(s"${PathUtil.bronzePath(dataLakeRootPath)}/$targetTable")
  }

  private def splitFilenameAndExtension(filename: String): (String, String) = {
    val splitFilename = filename.split("\\.")
    (splitFilename(splitFilename.length - 2), splitFilename.last)
  }

  private def readCsv(spark: SparkSession, path: String, metadata: Map[String, String]): DataFrame =
    spark
      .read
      .option("inferSchema", "true")
      .option("header", metadata.getOrElse("header", "true"))
      .option("delimiter", metadata.getOrElse("delimiter", ","))
      .csv(path)

  private def readJson(spark: SparkSession, path: String, metadata: Map[String, String]): DataFrame =
    spark
      .read
      .json(path)
}
