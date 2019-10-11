import edu.uta.diql._
import edu.uta.diql.core._
import scala.io.Source
import scala.collection.parallel.ParIterable

object Test {

  def main ( args: Array[String] ) {
    println("Number of cores: "+Runtime.getRuntime().availableProcessors())

    var t: Long = System.currentTimeMillis()  
    val a = (1 to args(0).toInt).map{ x => (x,x+1) }.toList
    val b = (1 to args(0).toInt).map{ x => (x,x*2) }.toList

    println("**** construct sequential: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    ( a.map{ case (k,v) => (k,Left(v).asInstanceOf[Either[Int,Int]]) }
        ++ b.map{ case (k,v) => (k,Right(v).asInstanceOf[Either[Int,Int]]) } )
      .groupBy(_._1)
      .map{ case (k,s) => ( k, ( s.flatMap{ case (_,Left(v)) => List(v); case _ => Nil }.toList,
                                 s.flatMap{ case (_,Right(v)) => List(v); case _ => Nil }.toList ) ) }
      .flatMap{ case (k,(as,bs)) => as.flatMap(x => bs.map(y => x+y)) }

    println("**** sequential: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

    val c = a.par
    val d = b.par

    println("**** construct parallel: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    explain(true)

    t = System.currentTimeMillis()

    ( c.map{ case (k,v) => (k,Left(v).asInstanceOf[Either[Int,Int]]) }
        ++ d.map{ case (k,v) => (k,Right(v).asInstanceOf[Either[Int,Int]]) } )
      .groupBy(_._1)
      .map{ case (k,s) => ( k, ( s.flatMap{ case (_,Left(v)) => List(v); case _ => Nil }.toList,
                                 s.flatMap{ case (_,Right(v)) => List(v); case _ => Nil }.toList ) ) }

    println("**** parallel1: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    t = System.currentTimeMillis()

   q("""
      select v+w from (i,v) <- c, (j,w) <- d where i==j
     """)

    println("**** parallel: "+(System.currentTimeMillis()-t)/1000.0+" secs")

  }
}
