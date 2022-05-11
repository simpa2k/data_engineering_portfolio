package com.simonolofsson.pipeline

import com.simonolofsson.util.PathUtil
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class LandingToBronzePipeline {

  private def readMethodsByFileExtension = Map(
    "csv" -> readCsv _,
    "json" -> readJson _
  )

  def apply(spark: SparkSession, dataLakeRootPath: String, landingSubfolder: String, filename: String, metadata: Map[String, String] = Map.empty): Unit = {
    val (filenameWithoutExtension, extension) = splitFilenameAndExtension(filename)
    val targetTable = metadata.getOrElse("target_table", filenameWithoutExtension) // TODO: Constant for target_table
    val metadataMap = readMetadataCompanionFile(spark, dataLakeRootPath, landingSubfolder)

    val company = metadataMap("meta_company")
    val source = metadataMap("meta_source")
    val outputPath = s"${PathUtil.bronzePath(dataLakeRootPath)}/$company/$source/$targetTable"

    val readDF = readMethodsByFileExtension
      .get(extension)
      .map(readFunction => readFunction(spark, s"${PathUtil.landingPath(dataLakeRootPath)}/$landingSubfolder/$filename", metadata))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Unsupported file format detected. The extension of the file passed was $extension but the supported ones are ${readMethodsByFileExtension.keys.mkString(",")}"
        )
      )

    val withMetadataColumns = metadataMap.foldLeft(readDF) {
      case (accDF, (key, value)) =>
        accDF.withColumn(key, lit(value))
    }

    withMetadataColumns
      .write
      .format("delta")
      .mode("append")
      .save(outputPath)
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

  private def readMetadataCompanionFile(spark: SparkSession, dataLakeRootPath: String, landingSubfolder: String): Map[String, String] = {
    val metadataDF = spark
      .read
      .json(s"$dataLakeRootPath/landing/$landingSubfolder/metadata.json")

    val metadataColumns = metadataDF.schema.fields.map(_.name).toSet
    val requiredMetadataColumns = Set("company", "source")
    val missingMetadataColumns = requiredMetadataColumns -- metadataColumns
    if (missingMetadataColumns.nonEmpty) {
      throw new IllegalArgumentException(s"Missing required metadata fields [${missingMetadataColumns.mkString(",")}]. Please add these fields to the metadata.json file in the landing folder")
    }

    // Prefix all metadata columns with 'meta'
    val withPrefixedMatadataColumns = metadataDF.schema.fields.foldLeft(metadataDF) {
      case (accDF, field) =>
        accDF.withColumnRenamed(field.name, s"meta_${field.name}")
    }

    // Collect and convert to map
    val collected = withPrefixedMatadataColumns.collect
    if (collected.length > 1) {
      throw new IllegalArgumentException(s"Too many metadata rows. Expected one, got ${collected.length}")
    }

    // TODO: meta_$c is repeated three times, reduce code repetition
    val metadataRow = collected(0)
    metadataColumns.map(c => (s"meta_$c", metadataRow.getAs[String](s"meta_$c"))).toMap
  }
}
