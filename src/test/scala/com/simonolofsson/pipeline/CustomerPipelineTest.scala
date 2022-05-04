package com.simonolofsson.pipeline

import com.simonolofsson.pipeline.customer.CustomerSystemAPipeline
import com.simonolofsson.testUtil.{PathUtil, SparkUtil}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AnyFreeSpec

class CustomerPipelineTest extends AnyFreeSpec with BeforeAndAfterEach {

  "A customer pipeline" - {
    "should add a synthetic key and perform upsert when writing from landing to bronze to silver" in {
      val landingToBronzePipeline = new LandingToBronzePipeline
      landingToBronzePipeline(SparkUtil.spark, PathUtil.dataLakeRootPath, "customer_system_a_1.csv", Map("target_table" -> "customer_system_a"))

      val customerBronzeToSilverPipeline = new CustomerSystemAPipeline
      customerBronzeToSilverPipeline(SparkUtil.spark, PathUtil.dataLakeRootPath).processAllAvailable()

      val actualDataFrameA = SparkUtil.readSilverTable("customer")

      val expectedSchema = StructType(Seq(
        StructField("customer_number", StringType, nullable = false),
        StructField("ssn", StringType, nullable = false),
        StructField("name", StringType, nullable = false),
        StructField("meta_source_system", StringType, nullable = false),
        StructField("meta_company", StringType, nullable = false),
        StructField("id", StringType, nullable = false),
      ))
      val expectedDataFrameA = SparkUtil.createDataFrame(
        Seq(
          Row("123456-789", "sensitive_value_1", "name_a", "system_a", "company_a", "dummy_value"),
          Row("234567-890", "sensitive_value_2", "name_b", "system_a", "company_a", "dummy_value"),
        ),
        expectedSchema
      )

      SparkUtil.assertDataFramesEqual(actualDataFrameA, expectedDataFrameA, ignoreColumns = Set("id"))

      landingToBronzePipeline(SparkUtil.spark, PathUtil.dataLakeRootPath, "customer_system_a_2.csv", Map("target_table" -> "customer_system_a"))
      customerBronzeToSilverPipeline(SparkUtil.spark, PathUtil.dataLakeRootPath).processAllAvailable()

      val actualDataFrameB = SparkUtil.readSilverTable("customer")
      val expectedDataFrameB = SparkUtil.createDataFrame(
        Seq(
          Row("345678-901", "sensitive_value_3", "name_c", "system_a", "company_a", "dummy_value"),
          Row("123456-789", "sensitive_value_1", "name_d", "system_a", "company_a", "dummy_value"),
          Row("234567-890", "sensitive_value_2", "name_b", "system_a", "company_a", "dummy_value"),
        ),
        expectedSchema
      )

      SparkUtil.assertDataFramesEqual(actualDataFrameB, expectedDataFrameB, ignoreColumns = Set("id"))

      // TODO: This is very cryptic. Make it more readable
      val sortedA = actualDataFrameA.select("ssn", "id").collect().sortBy(_.getAs[String]("ssn"))
      val sortedB = actualDataFrameA.select("ssn", "id").collect().sortBy(_.getAs[String]("ssn"))

      sortedA.foreach(row => java.util.UUID.fromString(row.getAs[String]("id")))
      sortedB.foreach(row => java.util.UUID.fromString(row.getAs[String]("id")))

      sortedA.zip(sortedB).foreach { case (rowA, rowB) => assert(rowA.getAs[String]("id") == rowB.getAs[String]("id")) }
    }
  }

  override protected def afterEach(): Unit = {
    PathUtil.removeBronze()
    PathUtil.removeSilver()
  }
}
