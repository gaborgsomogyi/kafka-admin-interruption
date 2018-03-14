package com.kafka

import java.io.{File, IOException}
import java.util.UUID

import org.apache.commons.io.FileUtils

object Utils {
  private val MAX_DIR_CREATION_ATTEMPTS: Int = 10

  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }

    dir.getCanonicalFile
  }

  def deleteRecursively(file: File): Unit = {
    if (file != null) {
      FileUtils.deleteDirectory(file)
    }
  }

  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    dir
  }
}
