name := "lucenecul"

version := "1.0"

scalaVersion := "2.11.7"

val luceneVersion: String = "5.3.0"

libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.apache.lucene" % "lucene-core" % luceneVersion,
  "org.apache.lucene" % "lucene-codecs" % luceneVersion,
  "org.apache.lucene" % "lucene-facet" % luceneVersion,
  "org.apache.lucene" % "lucene-analyzers-common" % luceneVersion,
  "org.scalaj" %% "scalaj-http" % "1.1.5",
  "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "com.jsuereth" %% "scala-arm" % "1.4"
)

