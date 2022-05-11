package com.simonolofsson.pipeline.policy

import com.simonolofsson.pipeline.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

class PolicyCompanyBPipeline {

  def apply(spark: SparkSession, dataLakeRootPath: String): StreamingQuery = {
    val preparedPolicies = spark
      .readBronzeStream(dataLakeRootPath, "company_b", "source_system_a", "policy")
      .withColumnRenamed("start_date", "valid_from")
      .withColumnRenamed("end_date", "valid_through")

    new PolicyPipeline().apply(spark, preparedPolicies, dataLakeRootPath, getClass.getName)
  }
}