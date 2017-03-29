/**
  * Created by ashiqimran on 3/6/17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.annotation.tailrec
import scala.reflect.ClassTag
import scala.util._
//import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
object GraphGenerator {
  val RMATa = 0.45
  val RMATb = 0.15
  val RMATd = 0.25
  val RMATc = 0.15
  def main(args: Array[ String ]): Unit = {


    val conf = new SparkConf().setAppName("GraphGenerator")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val V = Integer.parseInt(args(0))
    val E = Integer.parseInt(args(1))
    val result:Graph[Int, Int] = rmatGraph(sc,V,E)
    //result.vertices.saveAsTextFile("vertices")
    result.edges.map{case Edge(src, dst, prop) => (src , dst)}.map(x=>s"${x._1},${x._2}").repartition(1).saveAsTextFile("edges")


    //println(result.vertices.collect().mkString("\n"))
  }

  def rmatGraph(sc: SparkContext, requestedNumVertices: Int, numEdges: Int): Graph[Int, Int] = {
    // let N = requestedNumVertices
    // the number of vertices is 2^n where n=ceil(log2[N])
    // This ensures that the 4 quadrants are the same size at all recursion levels
    val numVertices = math.round(
      math.pow(2.0, math.ceil(math.log(requestedNumVertices) / math.log(2.0)))).toInt
    val numEdgesUpperBound =
      math.pow(2.0, 2 * ((math.log(numVertices) / math.log(2.0)) - 1)).toInt
    if (numEdgesUpperBound < numEdges) {
      throw new IllegalArgumentException(
        s"numEdges must be <= $numEdgesUpperBound but was $numEdges")
    }
    var edges: Set[Edge[Int]] = Set()
    while (edges.size < numEdges) {
      if (edges.size % 100 == 0) {
        //logDebug(edges.size + " edges")
      }
      edges += addEdge(numVertices)
    }
    outDegreeFromEdges(sc.parallelize(edges.toList))
  }

  private def outDegreeFromEdges[ED: ClassTag](edges: RDD[Edge[ED]]): Graph[Int, ED] = {
    val vertices = edges.flatMap { edge => List((edge.srcId, 1)) }
      .reduceByKey(_ + _)
      .map{ case (vid, degree) => (vid, degree) }
    Graph(vertices, edges, 0)
  }

  private def addEdge(numVertices: Int): Edge[Int] = {
    // val (src, dst) = chooseCell(numVertices/2.0, numVertices/2.0, numVertices/2.0)
    val v = math.round(numVertices.toFloat/2.0).toInt

    val (src, dst) = chooseCell(v, v, v)
    Edge[Int](src, dst, 1)
  }
  @tailrec
  private def chooseCell(x: Int, y: Int, t: Int): (Int, Int) = {
    if (t <= 1) {
      (x, y)
    } else {
      val newT = math.round(t.toFloat/2.0).toInt
      pickQuadrant(RMATa, RMATb, RMATc, RMATd) match {
        case 0 => chooseCell(x, y, newT)
        case 1 => chooseCell(x + newT, y, newT)
        case 2 => chooseCell(x, y + newT, newT)
        case 3 => chooseCell(x + newT, y + newT, newT)
      }
    }
  }

  private def pickQuadrant(a: Double, b: Double, c: Double, d: Double): Int = {
    if (a + b + c + d != 1.0) {
      throw new IllegalArgumentException("R-MAT probability parameters sum to " + (a + b + c + d)
        + ", should sum to 1.0")
    }
    val rand = new Random()
    val result = rand.nextDouble()
    result match {
      case x if x < a => 0 // 0 corresponds to quadrant a
      case x if (x >= a && x < a + b) => 1 // 1 corresponds to b
      case x if (x >= a + b && x < a + b + c) => 2 // 2 corresponds to c
      case _ => 3 // 3 corresponds to d
    }
  }
}
