package pipeline

import org.apache.spark.sql.SparkSession

trait Pipeline {

  def apply(spark: SparkSession, dataLakeRootPath: String): Unit
}
