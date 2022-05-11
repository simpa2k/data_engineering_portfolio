package com.simonolofsson.pipeline.policy

import com.simonolofsson.pipeline.implicits._
import com.simonolofsson.util.SilverTable
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PolicyPipeline {

  def apply(spark: SparkSession, preparedPolicyDF: DataFrame, dataLakeRootPath: String, pipelineName: String): StreamingQuery = {
    import spark.implicits._
    preparedPolicyDF
      .withColumn("customer_number", regexp_replace($"customer_number", "^([0-9]{6})([0-9]{3})$", "$1-$2"))
      .withColumnFormattedAsDate("sign_date")
      .withColumnFormattedAsDate("valid_from")
      .withColumnFormattedAsDate("valid_through")
      .writeStream
      .outputMode("append")
      .trigger(Trigger.Once())
      .mergeIntoSilver(SilverTable(dataLakeRootPath, "policy"), pipelineName, Seq("customer_number", "product"))
      .start()
  }
}
