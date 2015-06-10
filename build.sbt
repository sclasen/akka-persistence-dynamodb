organization := "com.sclasen"

name := "akka-persistence-dynamodb"

version := "0.3.4-SNAPSHOT"

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.11.6", "2.10.4")

parallelExecution in Test := false

//resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

resolvers += "spray repo" at "http://repo.spray.io"

libraryDependencies += "com.sclasen" %% "spray-dynamodb" % "0.3.2" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-persistence-experimental" % "2.3.4" % "compile"

libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % "2.3.4" % "test,it"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.1.7" % "test,it"

libraryDependencies += "commons-io" % "commons-io" % "2.4" % "test,it"

resolvers += "krasserm at bintray" at "http://dl.bintray.com/krasserm/maven"

libraryDependencies += "com.github.krasserm" %% "akka-persistence-testkit" % "0.3.2" % "test"

parallelExecution in Test := false

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


val root = Project("akka-persistence-dynamodb", file(".")).configs(IntegrationTest).settings(Defaults.itSettings:_*)
