organization := "edu.uta"
version := "0.1-SNAPSHOT"
scalaVersion := "2.11.7"
licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
sourceDirectory in Compile := baseDirectory.value / "src" / "main"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6"

lazy val `diql-spark` = (project in file("."))
  .settings(
     unmanagedSourceDirectories in Compile += baseDirectory.value / "src" / "spark",
     libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0",
     initialCommands in console := """import edu.uta.diql._
          import org.apache.spark._
          import org.apache.spark.rdd._
          val sc = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[2]"))
     """,
     cleanupCommands in console := "sc.stop()"
  )

lazy val `diql-flink` = (project in file("."))
  .settings(
     unmanagedSourceDirectories in Compile += baseDirectory.value / "src" / "flink",
     libraryDependencies ++= Seq("org.apache.flink" %% "flink-scala" % "1.2.0",
          "org.apache.flink" %% "flink-clients" % "1.2.0"),
     initialCommands in console := """import edu.uta.diql._
          import org.apache.flink.api.scala._
          import java.io.File
          val env = ExecutionEnvironment.getExecutionEnvironment
     """
)

lazy val `diql-scalding` = (project in file("."))
  .settings(
     unmanagedSourceDirectories in Compile += baseDirectory.value / "src" / "scalding",
     resolvers += "conjars.org" at "http://conjars.org/repo",
     libraryDependencies ++= Seq("com.twitter" %% "scalding-core" % "0.17.0",
          "cascading" % "cascading-core" % "2.6.1",
          "cascading" % "cascading-hadoop2-mr1" % "2.6.1",
          "cascading" % "cascading-local" % "2.6.1"),
     initialCommands in console := """import edu.uta.diql._
          import com.twitter.scalding._
          import com.twitter.scalding.typed.{LiteralValue,ComputedValue}
     """
)

lazy val root = `diql-spark`
