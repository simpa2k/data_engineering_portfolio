package com.simonolofsson.pipeline.policy

import com.simonolofsson.pipeline.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

class PolicyCompanyAPipeline {

  def apply(spark: SparkSession, dataLakeRootPath: String): StreamingQuery = {
    val preparedPolicies = spark
      .readBronzeStream(dataLakeRootPath, "company_a", "source_system_a", "policy")

    new PolicyPipeline().apply(spark, preparedPolicies, dataLakeRootPath, getClass.getName) // TODO: getClass.getName gets repeated a lot, see if this can be abstracted away
  }
}