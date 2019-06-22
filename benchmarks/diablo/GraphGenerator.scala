import scala.annotation.tailrec
import scala.util._

object GraphGenerator {
  val RMATa = 0.30
  val RMATb = 0.25
  val RMATd = 0.25
  val RMATc = 0.20

  def main ( args: Array[ String ] ) {
    val V = Integer.parseInt(args(0))
    val E = Integer.parseInt(args(1))
    val output_file = args(2)
    val outf = new java.io.PrintWriter(output_file)
    rmatGraph(V,E)
    outf.close()

  def rmatGraph ( requestedNumVertices: Int, numEdges: Int ) {
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
    (1 to numEdges).map( _ => addEdge(numVertices))
  }

  def addEdge(numVertices: Int) {
    val v = math.round(numVertices.toFloat/2.0).toInt
    chooseCell(v, v, v)
  }

  def chooseCell(x: Int, y: Int, t: Int) {
    if (t <= 1) {
      outf.println(x+","+y)
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

  def pickQuadrant(a: Double, b: Double, c: Double, d: Double): Int = {
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
}
