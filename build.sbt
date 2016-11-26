name := "grokking-data-streaming"

assemblyJarName := "grokking-data-streaming.jar"

version := "1.0"

scalaVersion := "2.11.8"

unmanagedBase := baseDirectory.value / "unlib"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.1" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.7.1" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.0.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.11" % "2.0.1" % "provided",
  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.0.1",
  "com.yammer.metrics" % "metrics-core" % "2.2.0",
  "net.liftweb" % "lift-json_2.11" % "3.0",
  "redis.clients" % "jedis" % "2.9.0"
)

val excludedJarsName = Seq( 
	"scala-library-2.11.8.jar",
	"unused-1.0.0.jar"
)

excludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {
	c => excludedJarsName exists { c.data.getName contains _ }
  }
}