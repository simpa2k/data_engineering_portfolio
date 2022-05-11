package com.simonolofsson.testUtil

import java.io.File
import java.nio.file.Paths
import scala.reflect.io.Directory

object PathUtil {

  val dataLakeRootPath: String = s"${System.getProperty("user.dir")}/testData"
  val landingPath: String = s"$dataLakeRootPath/landing"
  val bronzePath: String = s"$dataLakeRootPath/bronze"
  val silverPath: String = s"$dataLakeRootPath/silver"

  def getResourceParentFolderPath(resourceName: String): String = {
    val url = getClass.getClassLoader.getResource(resourceName)
    Paths.get(url.toURI).getParent.toString
  }

  def removeBronze(): Unit = new Directory(new File(bronzePath)).deleteRecursively()

  def removeSilver(): Unit = new Directory(new File(silverPath)).deleteRecursively()

  def emptyLandingFolder(landingSubfolder: String): Unit = new Directory(new File(s"$landingPath/$landingSubfolder")).files.foreach(_.delete())

  def removeExtension(filename: String): String = {
    val splitFilename = filename.split("\\.")
    splitFilename(splitFilename.length - 2)
  }
}
