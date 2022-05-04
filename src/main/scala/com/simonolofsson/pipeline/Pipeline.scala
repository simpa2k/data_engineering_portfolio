package com.simonolofsson.pipeline

import org.apache.spark.sql.SparkSession

trait Pipeline {

  def apply(spark: SparkSession, dataLakeRootPath: String, filename: String, metadata: Map[String, String] = Map.empty): Unit
}
