val ScalatraVersion = "2.5.1"

organization := "com.sibat"

name := "GongAnApiServer"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.scalatra" %% "scalatra" % ScalatraVersion,
  "org.scalatra" %% "scalatra-scalatest" % ScalatraVersion % "test",
  "org.scalatra" %% "scalatra-json" % ScalatraVersion,
  "org.json4s"   %% "json4s-jackson" % "3.5.0",
  "ch.qos.logback" % "logback-classic" % "1.1.5" % "runtime",
  "org.eclipse.jetty" % "jetty-webapp" % "9.2.15.v20160210" % "container;compile",
  "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",
  "org.apache.hbase" % "hbase-client" % "0.96.1.1-cdh5.0.2" ,
  "org.apache.hbase" % "hbase-common" % "0.96.1.1-cdh5.0.2" ,
  "org.elasticsearch" % "elasticsearch" % "2.4.2",
  "org.apache.hadoop" % "hadoop-client" % "2.3.0-cdh5.0.2"
)

enablePlugins(SbtTwirl)
enablePlugins(ScalatraPlugin)

assemblyMergeStrategy in assembly := {

// case PathList("javax", "servlet", xs@_*) => MergeStrategy.last

case PathList("javax", "activation", xs@_*) => MergeStrategy.last

// case PathList("org", "apache", xs@_*) => MergeStrategy.last

case PathList("org", "w3c", xs@_*) => MergeStrategy.last

case PathList("com", "google", xs@_*) => MergeStrategy.last

case PathList("com", "codahale", xs@_*) => MergeStrategy.last

case PathList(ps@_*) if ps.last endsWith ".properties" => MergeStrategy.first

case PathList(ps @ _*) if ps.last endsWith ".class" => MergeStrategy.first

case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first

case x =>

val oldStrategy = (assemblyMergeStrategy in assembly).value

oldStrategy(x)

}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "shadeio.@1").inAll
)
