package com.simonolofsson.util

case class SilverTable(dataLakeRootPath: String, tableName: String) {
  def path: String = s"${PathUtil.silverPath(dataLakeRootPath)}/$tableName"
  def checkpointLocation(pipelineName: String) = s"${path}_${pipelineName}_checkpoint"
}
