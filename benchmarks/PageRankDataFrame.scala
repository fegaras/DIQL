import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j._

object Test {

  case class Edge ( src: Int, dest: Int )

  val alpha = 0.85

  def rank ( sums: Long, counts: Long ): Double = (1-alpha)+alpha*sums/counts

  def main ( args: Array[String] ) {
    val iterations = args(0).toInt
    val input_file = args(1)
    val output_file = args(2)
    val sparkConf = new SparkConf().setAppName("Spark SQL PageRank")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    sparkConf.set("spark.logConf","false")
    sparkConf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    sqlContext.setConf("spark.sql.shuffle.partitions",
                       sparkConf.get("spark.executor.instances","2"))

    val t: Long = System.currentTimeMillis()

    val edges = spark.sparkContext.textFile(input_file)
                     .map(_.split(",")).map(n => Edge(n(0).toInt,n(1).toInt))
                     .toDF()

    var nodes = edges.groupBy("src").agg(count("dest").as("degree"))
                     .withColumn("rank",lit(1-alpha))
                     .withColumnRenamed("src","id")
    println("Count "+nodes.count())

    for ( i <- 1 to iterations ) {
      println(s"Iteration $i")
      val newranks = nodes.join(edges,nodes("id")===edges("src"))
                          .groupBy("dest")
                          .agg(udf(rank _).apply(sum("rank"),sum("degree")).as("newrank"))
      nodes = newranks.join(nodes,nodes("id")===newranks("dest"))
                      .drop("rank").drop("dest")
                      .withColumnRenamed("newrank","rank").cache()
      println("Count "+nodes.count())
    }

    nodes.write.csv(output_file)

    println("Spark SQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }
}
