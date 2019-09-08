import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KmeansSpark {

  type Point = (Double, Double)
  var centroids: Array[Point] = Array[Point]()

  //Receives each point and clusters, returns cluster(closest to that point and the point itself)
  def assignment(x: Point, centers: Array[Point]) = {
    var minDistance: Double = 0
    var distance: Double = 0
    var count = 0
    var point: Point = (0.0, 0.0)
    for (c: Point <- centers) {
      distance = math.sqrt(math.pow((c._1 - x._1), 2) + math.pow((c._2 - x._2), 2))
      if (count == 0) {
        minDistance = distance
        point = c
        count = count + 1
      }
      if (distance < minDistance) {
        minDistance = distance
        point = c
      }
    }
    point
  }

  //takes points assigned to one cluster and get the new cluster coordinates
  // x,y coordinates of cluster is the mean x,y of all points
  def update(points: Iterable[Point]) = {
    var x: Double = 0.0
    var y: Double = 0.0
    val total = points.size
    for (p: Point <- points) {
      x += p._1
      y += p._2
    }
    (x / total, y / total)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Kmeans Clustering").setMaster("local[2]")
    val sc = new SparkContext(conf)

    centroids = sc.textFile("c").map(line => {
      val a = line.split(",")
      (a(0).toDouble, a(1).toDouble)})
      .collect()

    var points = sc.textFile("p").map(line => {
      val a = line.split(",")
      (a(0).toDouble, a(1).toDouble)
    })

    for( a <- 1 to 10) {
      points.map(point => (assignment(point, centroids), point))
            .groupByKey()
            .map(points => update(points._2))
            .foreach(println)
    }

    sc.stop()
  }
}
