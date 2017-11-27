name := "GongAnCounter"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/repo/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
  "org.apache.hbase" % "hbase-client" % "0.96.1.1-cdh5.0.2" ,
  "org.apache.hbase" % "hbase-common" % "0.96.1.1-cdh5.0.2" ,
  "org.apache.hbase" % "hbase-server" % "0.96.1.1-cdh5.0.2"
)


assemblyMergeStrategy in assembly := {

case PathList("javax", "servlet", xs@_*) => MergeStrategy.last

case PathList("javax", "activation", xs@_*) => MergeStrategy.last

// case PathList("org", "apache", xs@_*) => MergeStrategy.last

case PathList("org", "w3c", xs@_*) => MergeStrategy.last

case PathList("com", "google", xs@_*) => MergeStrategy.last

case PathList("com", "codahale", xs@_*) => MergeStrategy.last

case PathList(ps@_*) if ps.last endsWith ".properties" => MergeStrategy.first

case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first

case x =>

val oldStrategy = (assemblyMergeStrategy in assembly).value

oldStrategy(x)

}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "shadeio.@1").inAll
)
