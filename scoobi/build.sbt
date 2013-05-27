organization := "com.nicta"

name := "driskill-scoobi"

version := "0.1.0"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
  "com.nicta" %% "scoobi" % "0.7.0-RC1-cdh4",
  "driskill" % "driskill-core" % "0.1.0"
)

scalacOptions ++= Seq("-deprecation", "-feature", "-language:_")

resolvers += "maven-repo" at "file://"+Path.userHome.absolutePath+"/.m2/repository"