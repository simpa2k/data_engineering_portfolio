package com.simonolofsson.pipeline

import org.apache.spark.sql.functions.{lit, regexp_replace}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PolicyBronzeToSilverPipeline {

  def apply(spark: SparkSession, dataLakeRootPath: String): StreamingQuery = {
    import spark.implicits._

    val companyAPolicies = companyAPolicyPipeline(spark, dataLakeRootPath)
    val maybeCompanyBPolicies = companyBPolicyPipeline(spark, dataLakeRootPath)

    val allPolicies = maybeCompanyBPolicies match {
      case Some(companyBPolicies) => companyAPolicies.union(companyBPolicies)
      case None => companyAPolicies
    }

    allPolicies
      .withColumn("customer_number", regexp_replace($"customer_number", "^([0-9]{6})([0-9]{3})$", "$1-$2"))
      .withColumnFormattedAsDate("sign_date")
      .withColumnFormattedAsDate("valid_from")
      .withColumnFormattedAsDate("valid_through")
      .writeStream
      .outputMode("append")
      .trigger(Trigger.Once())
      .mergeIntoSilver(dataLakeRootPath, "policy", "customer_number", "product")
      .start()
  }

  private def companyAPolicyPipeline(spark: SparkSession, dataLakeRootPath: String): DataFrame =
    spark
      .readBronzeStream(dataLakeRootPath, "policy_company_a")
      .withColumn("company", lit("company_a"))

  private def companyBPolicyPipeline(spark: SparkSession, dataLakeRootPath: String): Option[DataFrame] = {
    spark
      .readBronzeStreamIfExists(dataLakeRootPath, "policy_company_b")
      .map(_
        .withColumn("company", lit("company_b"))
        .withColumnRenamed("start_date", "valid_from")
        .withColumnRenamed("end_date", "valid_through"))
  }
}