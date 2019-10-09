import edu.uta.diql._
import scala.io.Source
import scala.collection.parallel.mutable.ParArray

object Test {

  def main ( args: Array[String] ) {
    println("Number of cores: "+Runtime.getRuntime().availableProcessors())

    var t: Long = System.currentTimeMillis()  
    val a = (1 to args(0).toInt).toList

    println("**** construct sequential: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    a.map(_+1)

    println("**** sequential: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    a.groupBy(x => x).map(x => (x,+1))

    println("**** sequential group by: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    val c = (1 to args(0).toInt).toArray.par

    println("**** construct parallel: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    explain(true)

    t = System.currentTimeMillis()

   q("""
      select v+1 from v <- c
     """)

    println("**** parallel: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

   q("""
      select (k, +/v) from v <- c group by k: v+1
     """)

    println("**** parallel group by: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }
}
