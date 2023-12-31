name := "favourite-color-using-scala"
organization        := "com.udemy.kafka.streams.course"
version             := "2.1.0-SNAPSHOT"
scalaVersion := "2.13.11"

// needed to resolve weird dependency
libraryDependencies += "javax.ws.rs" % "javax.ws.rs-api" % "2.1" artifacts(
  Artifact("javax.ws.rs-api", "jar", "jar"))

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % "2.1.0",
  "org.slf4j" %  "slf4j-api" % "1.7.25",
  "org.slf4j" %  "slf4j-log4j12" % "1.7.25"
)

// leverage java 8
//javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")
//scalacOptions := Seq("-target:jvm-1.8")
//initialize := {
//  val _ = initialize.value
//  if (sys.props("java.specification.version") != "1.8")
//    sys.error("Java 8 is required for this project.")
//}