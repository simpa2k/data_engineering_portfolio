package com.simonolofsson

import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

  }

  def spark: SparkSession =
    SparkSession
      .builder()
      .getOrCreate()
}
