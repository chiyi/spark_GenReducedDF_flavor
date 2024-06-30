name := "DataRichness"
version := "0.0"
scalaVersion := "2.12.19"
Compile / unmanagedClasspath += file("../common/scala/target/scala-2.12/common_2.12-0.0.jar")
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"
