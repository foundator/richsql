name := "foundator-richsql"
organization := "org.foundator"
version := "1.0-SNAPSHOT"
scalaVersion := "2.11.12"
crossScalaVersions := Seq(scalaVersion.value, "2.12.4")
libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "joda-time" % "joda-time" % "1.6.2",
    "com.intellij" % "annotations" % "9.0.4"
)