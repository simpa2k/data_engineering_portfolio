package com.simonolofsson.util

object PathUtil {
  def silverPath(dataLakeRootPath: String): String = s"$dataLakeRootPath/silver"

  def bronzePath(dataLakeRootPath: String): String = s"$dataLakeRootPath/bronze"

  def landingPath(dataLakeRootPath: String): String = s"$dataLakeRootPath/landing"
}
