import Dependencies._

name := "scala-spark-word2vec-recommender"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions += "-target:jvm-1.8"

mainClass in(Compile, run) := Some("Main")

libraryDependencies += sparkCore
libraryDependencies += sparkMLlib
libraryDependencies += scalaChart
