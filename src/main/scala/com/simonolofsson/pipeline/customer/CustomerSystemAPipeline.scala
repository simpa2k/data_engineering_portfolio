package com.simonolofsson.pipeline.customer

import com.simonolofsson.pipeline.implicits._
import com.simonolofsson.util.SilverTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

class CustomerSystemAPipeline {

  def apply(spark: SparkSession, dataLakeRootPath: String): StreamingQuery =
    spark
      .readBronzeStream(dataLakeRootPath, "company_a", "source_system_a", "customer")
      .writeStream
      .outputMode("append")
      .trigger(Trigger.Once())
      .mergeIntoSilverWithSurrogateKey(SilverTable(dataLakeRootPath, "customer"), getClass.getName, Seq("ssn"))
      .start()
}
