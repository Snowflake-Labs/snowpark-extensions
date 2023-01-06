package com.snowflake.snowpark_extensions.testutils

import java.io.File
import java.nio.file.Files
import java.net.URI
import java.io.IOException
import java.util.UUID

object Utils {

  val tableInputFormat: String = ""
  val tableOutputFormat: String = ""
  val defaultProvider: String = ""
  





  def createDirectory(dir: File): Boolean = {
    try {
      Files.createDirectories(dir.toPath)
      if ( !dir.exists() || !dir.isDirectory) {
        println(s"Failed to create directory " + dir)
      }
      dir.isDirectory
    } catch {
      case e: Exception =>
        println(s"Failed to create directory " + dir, e)
        false
    }
  }
    def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        // SPARK-35907:
        // This could throw more meaningful exception information if directory creation failed.
        Files.createDirectories(dir.toPath)
      } catch {
        case e @ (_ : IOException | _ : SecurityException) =>
          println(s"Failed to create directory $dir", e)
          dir = null
      }
    }

    dir.getCanonicalFile
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    //ShutdownHookManager.registerShutdownDeleteDir(dir)
    dir
  }
//#endregion Process
   def newUriForDatabase(): URI = new URI(createTempDir().toURI.toString.stripSuffix("/"))

}