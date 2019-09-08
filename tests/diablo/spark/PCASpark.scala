import org.apache.spark.{SparkConf, SparkContext}

object PCASpark {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("PCA").setMaster("local[2]")
    val sc = new SparkContext(conf)


    val P = sc.textFile("pca.txt").map(line => {
      val a = line.split(",")
      (a(0).toLong, a(1).toLong, a(2).toDouble)})

    val d =3
    val n = P.count()
    val r = n/d

    val mean = P.map(x => (x._2, x._3)).reduceByKey(_+_).map(x => (x._1, x._2/r)).sortByKey().collect()
    val x_bar = mean(0)._2
    val y_bar = mean(1)._2
    val z_bar = mean(2)._2

    val mul = P.map{ case (i,j,v) => (j,(i,v)) }
     .cogroup( P.map{ case (i,j,v) => (i,(j,v)) } )

    def v(i: Double, value:Double) =
      if (i == 0) {value - x_bar}
      else if (i == 1) {value - y_bar}
      else {value - z_bar}

    val cov = P.map{ case (i,j,v) => (i,(j,v)) }
      .cogroup( P.map{ case (i,j,v) => (i,(j,v)) } )
        .flatMap{case (k,(ms,ns)) => ms.flatMap{ case (i,m) => ns.map{
          case (j,n) => ((i,j), v(i,m)*v(j,n))
        }}}
      .reduceByKey(_+_)
      .map{ case ((i,j),v) => (i,j,v/(r-1)) }

    cov.foreach(println)
    sc.stop
  }
}
