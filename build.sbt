ThisBuild / name := "Spark Status Store Connector"
ThisBuild / organization := "com.github.yaooqinn"
ThisBuild / scalaVersion := "2.13.16"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / description := "A Spark DSv2 connector for querying its runtime status"
ThisBuild / licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.txt"))

lazy val root = (project in file("."))
  .settings(
    name := "spark-status-connector",
    resolvers += Resolver.mavenLocal,
    resolvers += Resolver.jcenterRepo,
    resolvers += Resolver.typesafeRepo("releases"),
    resolvers += "Apache Snapshots" at "https://repository.apache.org/snapshots/",
    resolvers += "Apache Releases" at "https://repository.apache.org/content/repositories/releases/",
    resolvers += "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",

  )

lazy val sparkVersion = "4.1.0-SNAPSHOT"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.18.1" % Test
//libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion classifier "tests"
