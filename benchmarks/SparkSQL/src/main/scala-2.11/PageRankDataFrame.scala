
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object PageRankDataFrame {

  case class Edge(src: String, dest: String)

  val alpha = 0.85
  val iterations = 10

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("page-rank")
      .set("spark.ui.showConsoleProgress", "false")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)


    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._
    val edgesRDD = spark.sparkContext.textFile("test.txt")
    val schemaString = "src dest"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val rowRDD = edgesRDD.map(_.split(","))
      .map(attributes => Row(attributes(0),attributes(1).trim))
   // val edges = Seq(edgesRDD)

    val edgesDF = sqlContext.createDataFrame(rowRDD,schema)

    val edgeAndDegreesDF = computeDegree(edgesDF)

    var edgeDegreePageRankDF = edgeAndDegreesDF.withColumn("pagerank", lit(1 - alpha))

    for (i <- Range(0, iterations)) {
      edgeDegreePageRankDF.cache()

      println("Iteration " + i)
      // compute the delta
      val reducedDeltasDF = computeDeltas(edgeDegreePageRankDF)

      // compute the new PR
      edgeDegreePageRankDF = updatePageRanks(edgeDegreePageRankDF, reducedDeltasDF)
    }

    val res = edgeDegreePageRankDF
      .select("src", "pagerank")
      .distinct()

    res.show()
  }

  def computeDegree(edgesDF: DataFrame): DataFrame = {
    val edgesDegreeDF = edgesDF
      .groupBy("src")
      .agg(count("src").as("degree"))

    val edgeAndDegreesDF = edgesDF.join(edgesDegreeDF, "src")

    edgeAndDegreesDF
  }

  def computeDeltas(edgeDegreePageRankRDD: DataFrame): DataFrame = {
    edgeDegreePageRankRDD
      .withColumn("delta", udf(computeDelta _).apply(col("pagerank"), col("degree")))
      .groupBy("dest")
      .agg(sum("delta").as("delta"))
      .withColumnRenamed("dest", "vertex")
  }

  def computeDelta(pagerank: Double, degree: Long): Double = pagerank * alpha / degree

  def updatePageRanks(edgeDegreePageRankDF: DataFrame, reducedDeltasDF: DataFrame): DataFrame = {
    val joinDF = edgeDegreePageRankDF
      .drop("pagerank")
      .join(reducedDeltasDF, edgeDegreePageRankDF("src") === reducedDeltasDF("vertex"), "left_outer")
      .drop("vertex")

    joinDF.withColumn("pagerank", udf(calcPageRank _).apply(col("src"), col("dest"), col("degree"), col("delta")))
      .drop("delta")
  }

  def calcPageRank(src: String, dest: String, degree: Long, delta: Any): Double = {
    delta match {
      case delta: Double => (1 - alpha) + delta
      case null => 1 - alpha
    }
  }

}