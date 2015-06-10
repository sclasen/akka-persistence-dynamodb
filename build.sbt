organization := "com.sclasen"

name := "akka-persistence-dynamodb"

version := "0.3.4-SNAPSHOT"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.11.6", "2.10.4")

parallelExecution in Test := false

val akkaVersion = "2.4-M1"

//resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "spray repo" at "http://repo.spray.io"

libraryDependencies ++= Seq(
  "com.sclasen" %% "spray-dynamodb" % "0.3.2",
  "com.typesafe.akka" %% "akka-persistence-experimental" % akkaVersion % "compile",
  "org.scalatest" %% "scalatest" % "2.1.7" % "test,it",
  "com.typesafe.akka" %% "akka-persistence-experimental-tck" % akkaVersion % "test,it",
  "com.github.dnvriend" %% "akka-persistence-inmemory" % "1.0.0" % "test,it"
)

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

pomExtra := (
  <url>http://github.com/sclasen/akka-persistence-dynamodb</url>
    <licenses>
      <license>
        <name>The Apache Software License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:sclasen/akka-persistence-dynamodb.git</url>
      <connection>scm:git:git@github.com:sclasen/akka-persistence-dynamodb.git</connection>
    </scm>
    <developers>
      <developer>
        <id>sclasen</id>
        <name>Scott Clasen</name>
        <url>http://github.com/sclasen</url>
      </developer>
    </developers>)


publishTo <<= version {
  (v: String) =>
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}


val root = Project("akka-persistence-dynamodb", file(".")).configs(IntegrationTest).settings(Defaults.itSettings: _*)
