import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j._

object Test {

  case class Edge ( src: Int, dest: Int )

  case class Rank ( id: Int, degree: Long, rank: Double )

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

    sqlContext.setConf("spark.sql.shuffle.partitions",args(3))

    val t: Long = System.currentTimeMillis()

    val edges = spark.sparkContext.textFile(input_file)
                     .map(_.split(",")).map(n => Edge(n(0).toInt,n(1).toInt))
                     .cache().toDF()

    var nodes = edges.groupBy("src").agg(count("dest").as("degree"))
                     .withColumn("rank",lit(1-alpha))
                     .withColumnRenamed("src","id").cache()

    for ( i <- 1 to iterations ) {
      println(s"Iteration $i")
      val newranks = nodes.join(edges,nodes("id")===edges("src"))
                          .groupBy("dest")
                          .agg(udf(rank _).apply(sum("rank"),sum("degree")).as("newrank"))
      nodes = newranks.join(nodes,nodes("id")===newranks("dest"))
                      .drop("rank","dest")
                      .withColumnRenamed("newrank","rank").cache()
      // Bug: DataFrames cannot do iteration; if you don't force the evaluation with an action, DataFrames will not completely evaluate the pipeline
      nodes = nodes.rdd.cache().map(x => Rank(x.getInt(1),x.getLong(2),x.getDouble(0))).toDF()
    }

    nodes.write.csv(output_file)

    println("**** Spark DataFrame run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }
}
