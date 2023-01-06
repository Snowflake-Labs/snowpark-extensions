name := "SnowparkExtensions"


// Compile / scalacOptions ++= Seq("-doc-root-content", "rootdoc.txt")
// Compile / scalacOptions ++= Seq("-doc-title", "Snowpark Extensions")


(sys.env.get("MAVEN_USERNAME"), sys.env.get("MAVEN_PASSWORD")) match {
  case (Some(username), Some(password)) => 
    println(s"CREDENTIALS PROVIDED")
    credentials += Credentials("Sonatype Nexus Repository Manager", "s01.oss.sonatype.org", username, password)
  case _ => 
    println("USERNAME and/or PASSWORD is missing")
    credentials ++= Seq()
}

ThisBuild / scalaVersion := "2.12.10"
ThisBuild / organization := "net.mobilize"

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / organization := "net.mobilize.snowpark-extensions"
ThisBuild / organizationName := "mobilize"
ThisBuild / organizationHomepage := Some(url("http://mobilize.net/"))

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/MobilizeNet/snowpark-extensions"),
    "scm:url:https://github.com/MobilizeNet/snowpark-extensions.git"
  )
)

ThisBuild / developers := List(
  Developer(
    id    = "orellabac",
    name  = "Mauricio Rojas",
    email = "mauricio.rojas@mobilize.net",
    url   = url("http://github.com/orellabac")
  )
)

ThisBuild / description := "Extensions for Snowpark"
ThisBuild / licenses := List("MIT" -> new URL("https://www.mit.edu/~amini/LICENSE.md"))
ThisBuild / homepage := Some(url("https://github.com/MobilizeNet/snowpark-extensions"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ => false }
ThisBuild / publishTo := {
  val nexus = "https://s01.oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
ThisBuild / publishMavenStyle := true



// disable publishw ith scala version, otherwise artifact name will include scala version 
crossPaths := false

version := sys.env.getOrElse("GITHUB_REF_NAME","0.0.0")
// Compile / scalacOptions ++= Seq("-doc-version", version.value)


val snowparkVersion = "1.6.2"
val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.ini4j" % "ini4j" % "0.5.4",
  "org.scala-lang" % "scala-library" % "2.12.10",
  "com.snowflake" % "snowpark" % snowparkVersion,  
  "org.scala-lang" % "scala-library" % "2.12.10" % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
)