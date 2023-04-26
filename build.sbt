ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
        name := "untitled",
        fork := true,
        javaOptions += "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        javaOptions += "-Djdk.attach.allowAttachSelf=true",
        libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0",
        libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0",
        libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.4.0",
        libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.4",
        libraryDependencies += "com.sun.net.httpserver" % "http" % "20070405",
        libraryDependencies += "io.circe" %% "circe-core" % "0.14.5",
        libraryDependencies += "io.circe" %% "circe-generic" % "0.14.5",
        libraryDependencies += "io.circe" %% "circe-parser" % "0.14.5",
)
