package com.simonolofsson.pipeline

import com.simonolofsson.pipeline.policy.{PolicyCompanyAPipeline, PolicyCompanyBPipeline}
import com.simonolofsson.testUtil.{PathUtil, SparkUtil}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.freespec.AnyFreeSpec

class PolicyPipelineTest extends AnyFreeSpec with BeforeAndAfterEach {

  private val expectedSilverSchema =
    StructType(Seq(
      StructField("customer_number", StringType, nullable = false),
      StructField("product", StringType, nullable = false),
      StructField("sign_date", DateType, nullable = false),
      StructField("valid_from", DateType, nullable = false),
      StructField("valid_through", DateType, nullable = true),
      StructField("meta_company", StringType, nullable = false),
      StructField("meta_source_system", StringType, nullable = false)
    ))

  "A policy pipeline" - {
    "should write from landing to bronze to silver" in {
      val landingToBronzePipeline = new LandingToBronzePipeline
      landingToBronzePipeline(SparkUtil.spark, PathUtil.dataLakeRootPath, "company_a/source_system_a/policy", "policy_company_a_1.csv", Map("target_table" -> "policy"))

      landingToBronzePipeline(SparkUtil.spark, PathUtil.dataLakeRootPath, "company_b/source_system_a/policy", "policy_company_b.csv", Map("target_table" -> "policy"))

      val policyCompanyAPipeline = new PolicyCompanyAPipeline
      policyCompanyAPipeline(SparkUtil.spark, PathUtil.dataLakeRootPath).processAllAvailable()
      val policyCompanyBPipeline = new PolicyCompanyBPipeline
      policyCompanyBPipeline(SparkUtil.spark, PathUtil.dataLakeRootPath).processAllAvailable()

      val actualDataFrame = SparkUtil.readSilverTable("policy")
      val expectedDataFrame = SparkUtil.createDataFrame(
        Seq(
          Row("123456-789", "POLICY_TYPE1", java.sql.Date.valueOf("2022-04-25"), java.sql.Date.valueOf("2022-08-19"), null, "company_a", "source_system_a"),
          Row("234567-890", "POLICY_TYPE1", java.sql.Date.valueOf("2020-01-01"), java.sql.Date.valueOf("2020-03-01"), java.sql.Date.valueOf("2022-01-01"), "company_a", "source_system_a"),
          Row("123456-789", "POLICY_TYPE2", java.sql.Date.valueOf("2022-04-25"), java.sql.Date.valueOf("2022-08-19"), null, "company_b", "source_system_a"),
          Row("234567-890", "POLICY_TYPE2", java.sql.Date.valueOf("2020-01-01"), java.sql.Date.valueOf("2020-03-01"), java.sql.Date.valueOf("2022-01-01"), "company_b", "source_system_a"),
        ),
        expectedSilverSchema
      )

      SparkUtil.assertDataFramesEqual(actualDataFrame, expectedDataFrame)
    }

    "should upsert rows into silver" in {
      val landingToBronzePipeline = new LandingToBronzePipeline
      landingToBronzePipeline(SparkUtil.spark, PathUtil.dataLakeRootPath, "company_a/source_system_a/policy", "policy_company_a_1.csv", Map("target_table" -> "policy"))

      val policyCompanyAPipeline = new PolicyCompanyAPipeline
      policyCompanyAPipeline(SparkUtil.spark, PathUtil.dataLakeRootPath).processAllAvailable()

      val expectedDataFrameA = SparkUtil.createDataFrame(
        Seq(
          Row("123456-789", "POLICY_TYPE1", java.sql.Date.valueOf("2022-04-25"), java.sql.Date.valueOf("2022-08-19"), null, "company_a", "source_system_a"),
          Row("234567-890", "POLICY_TYPE1", java.sql.Date.valueOf("2020-01-01"), java.sql.Date.valueOf("2020-03-01"), java.sql.Date.valueOf("2022-01-01"), "company_a", "source_system_a"),
        ),
        expectedSilverSchema
      )

      val actualDataFrameA = SparkUtil.readSilverTable("policy")
      SparkUtil.assertDataFramesEqual(actualDataFrameA, expectedDataFrameA)

      landingToBronzePipeline(SparkUtil.spark, PathUtil.dataLakeRootPath, "company_a/source_system_a/policy", "policy_company_a_2.csv", Map("target_table" -> "policy"))
      policyCompanyAPipeline(SparkUtil.spark, PathUtil.dataLakeRootPath).processAllAvailable()

      val expectedDataFrameB = SparkUtil.createDataFrame(
        Seq(
          Row("123456-789", "POLICY_TYPE1", java.sql.Date.valueOf("2022-04-25"), java.sql.Date.valueOf("2022-08-19"), java.sql.Date.valueOf("2023-08-19"), "company_a", "source_system_a"),
          Row("234567-890", "POLICY_TYPE1", java.sql.Date.valueOf("2020-01-01"), java.sql.Date.valueOf("2020-03-01"), java.sql.Date.valueOf("2022-01-01"), "company_a", "source_system_a"),
        ),
        expectedSilverSchema
      )

      val actualDataFrameB = SparkUtil.readSilverTable("policy")
      SparkUtil.assertDataFramesEqual(actualDataFrameB, expectedDataFrameB)

    }
  }

  override protected def afterEach(): Unit = {
    PathUtil.removeBronze()
    PathUtil.removeSilver()
  }
}
