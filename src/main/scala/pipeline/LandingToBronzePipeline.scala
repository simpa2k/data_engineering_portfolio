package pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}

class LandingToBronzePipeline {

  private def readMethodsByFileExtension = Map(
    "csv" -> readCsv _,
    "json" -> readJson _
  )

  def apply(spark: SparkSession, dataLakeRootPath: String, filename: String, metadata: Map[String, String]): Unit = {
    val (filenameWithoutExtension, extension) = splitFilenameAndExtension(filename)
    readMethodsByFileExtension
      .get(extension)
      .map(readFunction => readFunction(spark, s"${PathUtil.landingPath(dataLakeRootPath)}/$filename", metadata))
      .getOrElse(
        throw new IllegalArgumentException(
          s"Unsupported file format detected. The extension of the file passed was $extension but the supported ones are ${readMethodsByFileExtension.keys.mkString(",")}"
        )
      )
      .write
      .format("delta")
      .save(s"${PathUtil.bronzePath(dataLakeRootPath)}/$filenameWithoutExtension")
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
