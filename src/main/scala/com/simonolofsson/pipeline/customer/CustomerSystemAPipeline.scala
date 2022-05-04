package com.simonolofsson.pipeline.customer

import com.simonolofsson.pipeline.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

class CustomerSystemAPipeline {

  def apply(spark: SparkSession, dataLakeRootPath: String): StreamingQuery =
      spark
      .readBronzeStream(dataLakeRootPath, "customer_system_a")
      .withColumn("meta_source_system", lit("system_a"))
      .withColumn("meta_company", lit("company_a"))
      .writeStream
      .outputMode("append")
      .trigger(Trigger.Once())
      .mergeIntoSilverWithSyntheticId(dataLakeRootPath, "customer", Seq("ssn"))
      .start()
}
