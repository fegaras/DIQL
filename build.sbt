organization := "edu.uta"
version := "0.1-SNAPSHOT"
scalaVersion := "2.11.7"
licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")
sourceDirectory in Compile := baseDirectory.value / "src" / "main"
unmanagedSourceDirectories in Compile += baseDirectory.value / "src" / "diablo"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6"

lazy val diql_spark = (project in file("."))
  .settings(
     name := "DIQL on Spark",
     unmanagedSourceDirectories in Compile += baseDirectory.value / "src" / "spark",
     libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3",
     libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.3",
     initialCommands in console := """import edu.uta.diql._
          import org.apache.spark._
          import org.apache.spark.rdd._
          val sc = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[2]"))
     """,
     cleanupCommands in console := "sc.stop()"
  )

lazy val diql_flink = (project in file("."))
  .settings(
     name := "DIQL on Flink",
     unmanagedSourceDirectories in Compile += baseDirectory.value / "src" / "flink",
     libraryDependencies ++= Seq("org.apache.flink" %% "flink-scala" % "1.6.2",
          "org.apache.flink" %% "flink-clients" % "1.6.2"),
     initialCommands in console := """import edu.uta.diql._
          import org.apache.flink.api.scala._
          import java.io.File
          val env = ExecutionEnvironment.getExecutionEnvironment
     """
)

lazy val diql_scalding = (project in file("."))
  .settings(
     name := "DIQL on Scalding",
     unmanagedSourceDirectories in Compile += baseDirectory.value / "src" / "scalding",
     resolvers += "conjars.org" at "http://conjars.org/repo",
     libraryDependencies ++= Seq("com.twitter" %% "scalding-core" % "0.17.2",
          "cascading" % "cascading-core" % "3.3.0",
          "cascading" % "cascading-hadoop2-mr1" % "3.3.0",
          "cascading" % "cascading-local" % "3.3.0"),
     initialCommands in console := """import edu.uta.diql._
          import com.twitter.scalding._
          import com.twitter.scalding.typed.{LiteralValue,ComputedValue}
     """
)

lazy val diql_parallel = (project in file("."))
  .settings(
     name := "DIQL on Parallel Scala",
     unmanagedSourceDirectories in Compile += baseDirectory.value / "src" / "parallel",
     libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value,
     initialCommands in console := """import edu.uta.diql._
          import scala.io.Source
          import scala.collection.parallel.ParIterable
     """
)

lazy val root = diql_spark
