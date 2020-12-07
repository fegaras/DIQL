import edu.uta.diql._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.hadoop.fs._
import scala.util.Random

object PageRank {

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val vertices = args(1).toInt
    val edges = args(2).toLong

    val conf = new SparkConf().setAppName("LinearRegression")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    conf.set("spark.logConf","false")
    conf.set("spark.eventLog.enabled","false")
    LogManager.getRootLogger().setLevel(Level.WARN)

    val rand = new Random()
    val RMATa = 0.30
    val RMATb = 0.25
    val RMATd = 0.25
    val RMATc = 0.20
    val vn = math.round(Math.pow(2.0,Math.ceil(Math.log(vertices)/Math.log(2.0)))).toInt
    def pickQuadrant ( a: Double, b: Double, c: Double, d: Double ): Int
      = rand.nextDouble() match {
          case x if x < a => 0
          case x if (x >= a && x < a + b) => 1
          case x if (x >= a + b && x < a + b + c) => 2
          case _ => 3
        }

    def chooseCell ( x: Int, y: Int, t: Int ): (Int,Int) = {
        if (t <= 1)
           (x,y)
        else {
           val newT = math.round(t.toFloat/2.0).toInt
           pickQuadrant(RMATa, RMATb, RMATc, RMATd) match {
             case 0 => chooseCell(x, y, newT)
             case 1 => chooseCell(x + newT, y, newT)
             case 2 => chooseCell(x, y + newT, newT)
             case 3 => chooseCell(x + newT, y + newT, newT)
           }
        }
    }

    def addEdge ( vn: Int ): (Int,Int) = {
       val v = math.round(vn.toFloat/2.0).toInt
       chooseCell(v,v,v)
    }

    val E = sc.parallelize(1L to edges/100)
              .flatMap{ i => (1 to 100).map{ j => addEdge(vn) } }
              .map{ case (i,j) => ((i.toLong,j.toLong),true) }
              .cache()
    var b: Double = 0.8;
    var c: Double = b/vertices;
    val R = sc.parallelize(1L to vertices).map(v => (v,(1.0-b)/vertices))
    val Rds = R.toDS()
    Rds.createOrReplaceTempView("Rds")

    val size = sizeof(((1L,1L),true))
    println("*** %d %d  %.2f GB".format(vertices,edges,edges.toDouble*size/(1024.0*1024.0*1024.0)))
 
    val Eds = E.toDS()
    Eds.createOrReplaceTempView("Eds")
    val n = vertices
    
    def test () {
      var t: Long = System.currentTimeMillis()

      try {
       val links = E.map(_._1).groupByKey().cache()
       var ranks = links.mapValues(v => 1.0/vertices)

       val contribs = links.join(ranks).values.flatMap {
                          case (urls,rank)
                              => val size = urls.size
                                 urls.map(url => (url, rank/size))
                       }
       ranks = contribs.reduceByKey(_+_).mapValues((1-b)/vertices+b*_).cache();
       println(ranks.count);

       println("**** SparkRDD run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()

      try {
      v(sc,"""
       var C: vector[Int] = vector();
      var S: vector[Double] = vector();
      var I: vector[Double] = vector();

      for i = 1, n do
         for j = 1, n do
            if (E[i,j])
                C[i] += 1;
      
      for i = 1, n do{
         for j = 1, n do{
            if (E[j,i])
               S[i] += c/C[j];
                };
           };

      for i = 1, n do
         I[i] += S[i] + R[i];
     
       println(I.count);       
      """)

      println("**** Diablo run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }
   
      t = System.currentTimeMillis()

      try {
      s(sc,"""
        var C: vector[Int] = vector();
      var S: vector[Double] = vector();
      var I: vector[Double] = vector();

      for i = 1, n do
         for j = 1, n do
            if (Eds[i,j])
                C[i] += 1;
      
      for i = 1, n do{
         for j = 1, n do{
            if (Eds[j,i])
               S[i] += c/C[j];
                };
           };

      for i = 1, n do
         I[i] += S[i] + Rds[i];
      println(I.count);
       """)

      println("**** SQLGen run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      } catch { case x: Throwable => println(x) }

      t = System.currentTimeMillis()
      try{
      var C = spark.sql("SELECT Eds._1._1 AS _1, COUNT(Eds._2) AS _2 FROM Eds WHERE Eds._2 = true  GROUP BY Eds._1._1");
      C.createOrReplaceTempView("C");
      var S = spark.sql(StringContext("SELECT Eds._1._2 AS _1, SUM(", " / C._2) AS _2 FROM C JOIN Eds ON C._1 == Eds._1._1 GROUP BY Eds._1._2").s(c));
      S.createOrReplaceTempView("S");
      var I = spark.sql("SELECT S._1 AS _1, SUM(S._2 + Rds._2) AS _2 FROM Rds JOIN S ON Rds._1 == S._1 GROUP BY S._1");
      I.createOrReplaceTempView("I");
      println(I.count);
      println("**** SparkSQL run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
      }catch { case x: Throwable => println(x) }
 }

    for ( i <- 1 to repeats )
        test()

    sc.stop()
  }
}

