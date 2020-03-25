import edu.uta.diql._
import scala.io.Source
import scala.util.Random
import scala.collection.parallel.ParIterable


object Parallel {
  val rand = new Random()
  val num_steps = 1
  var t: Long = System.currentTimeMillis()

  println("Number of cores: "+Runtime.getRuntime().availableProcessors())

    def conditionalSum ( length: Long ) {

      val V = (1L to length).map{ n => rand.nextDouble()*200 }.toIterable.par

      println("*** %d  %.2f MB".format(length,length*64/(1024.0*1024.0)))

      t = System.currentTimeMillis()

      v("""
         var sum: Double = 0.0;

         for v in V do
             if (v < 100)
                sum += v;

         println(sum);
        """)

      println("**** ConditionalSumParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

    }

    def textProcessing ( length: Long ) {

      val max: Long = length/10   // 10 duplicates on the average

      val V = (1L to length).map{ j => "x%010d".format(Math.abs(rand.nextLong())%max) }.toIterable.par

      val size = 11+32
      println("*** %d  %.2f MB".format(length,length*size/(1024.0*1024.0)))

      val x = V.head

      t = System.currentTimeMillis()

      v("""
         var eq: Boolean = true;

         for v in V do
             eq := eq && v == x;

         println(eq);

        """)

      println("**** EqualParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")

      val words = V

      val key1 = "key1"
      val key2 = "key2"
      val key3 = "key3"

      t = System.currentTimeMillis()

      v("""

         var c: Boolean = false;

         for w in words do
             c := c || (w == key1 || w == key2 || w == key3);

         println(c);

        """)

      println("**** StringMatchParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    }

    def wordCount ( length: Long ) {

      val max: Long = length/10   // 10 duplicates on the average

      val V = (1L to length).map{ j => "x%010d".format(Math.abs(rand.nextLong())%max) }.toIterable.par

      val size = 11+32
      println("*** %d  %.2f MB".format(length,length*size/(1024.0*1024.0)))

      val words = V

      t = System.currentTimeMillis()

      v("""

         var C: map[String,Int] = map();

         for w in words do
             C[w] += 1;

         println(C.size);

        """)

      println("**** WordCountParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }

  def histogram ( length: Long ) {
    case class Color ( red: Int, green: Int, blue: Int )

    def byte () = Math.abs(rand.nextInt()) % 256

    val P = (1L to length).map{ j => Color(byte(),byte(),byte()) }.toIterable.par

    val size = sizeof(Color(1,1,1))
    println("*** %d  %.2f MB".format(length,length*size/(1024.0*1024.0)))

      t = System.currentTimeMillis()

      v("""

         var R: map[Int,Int] = map();
         var G: map[Int,Int] = map();
         var B: map[Int,Int] = map();

         for p in P do {
             R[p.red] += 1;
             G[p.green] += 1;
             B[p.blue] += 1;
         };

         println(R.size);
         println(G.size);
         println(B.size);

        """)

      println("**** HistogramParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    }

  def linearRegression ( length: Long ) {
    val P = (1L to length).map{ j => val x = rand.nextDouble()*1000
                                     val dx = rand.nextDouble()*10
                                     (x+dx,x-dx) }.toIterable.par

    val size = sizeof((1.0D,1.0D))
    println("*** %d  %.2f MB".format(length,length*size/(1024.0*1024.0)))

    val n = P.size

      t = System.currentTimeMillis()

      v("""

         var sum_x: Double = 0.0;
         var sum_y: Double = 0.0;
         var x_bar: Double = 0.0;
         var y_bar: Double = 0.0;
         var xx_bar: Double = 0.0;
         var yy_bar: Double = 0.0;
         var xy_bar: Double = 0.0;
         var slope: Double = 0.0;
         var intercept: Double = 0.0;

         for p in P do {
             sum_x += p._1;
             sum_y += p._2;
         };

         x_bar := sum_x/n;
         y_bar := sum_y/n;

         for p in P do {
             xx_bar += (p._1 - x_bar)*(p._1 - x_bar);
             yy_bar += (p._2 - y_bar)*(p._2 - y_bar);
             xy_bar += (p._1 - x_bar)*(p._2 - y_bar);
        };

        slope := xy_bar/xx_bar;
        intercept := y_bar - slope*x_bar;

        println(slope+" "+intercept);

        """)

      println("**** LinearRegressionParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }

  def groupBy ( length: Long ) {
    case class GB ( K: Long, A: Double )

    val max: Long = length/10   // 10 duplicates on the average
 
    val GBsize = sizeof(GB(1L,1.0D))
    println("*** %d  %.2f MB".format(length,length*GBsize/(1024.0*1024.0)))

    val V = (1L to length).map{ j => GB( Math.abs(rand.nextDouble()*max).toLong,
                                         rand.nextDouble() ) }.toIterable.par

      t = System.currentTimeMillis()

      v("""

         var C: vector[Double] = vector();

         for v in V do
            C[v.K] += v.A;

         println(C.size);
     
        """)

      println("**** GroupByParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }

  def randomMatrix ( n: Int, m: Int ) = {
    val max = 10
    val l = Random.shuffle((0 until n).toList)
    val r = Random.shuffle((0 until m).toList)
    l.flatMap{ i => r.map{ j => ((i.toLong,j.toLong),rand.nextDouble()*max) } }.toIterable.par
  }

  def add ( n: Int, m: Int ) {
    val mm = m

    val M = randomMatrix(n,m)
    val N = randomMatrix(n,m)

    val size = sizeof(((1L,1L),1.0D))
    println("*** %d %d  %.2f MB".format(n,m,(n*m)*size/(1024.0*1024.0)))

      t = System.currentTimeMillis()

      v("""

         var R: matrix[Double] = matrix();

         for i = 0, n-1 do
           for j = 0, mm-1 do
               R[i,j] := M[i,j]+N[i,j];

         println(R.size);

        """)

      println("**** AddParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }

  def multiply ( n: Int, m: Int ) {
    val mm = m

    val M = randomMatrix(n,m)
    val N = randomMatrix(n,m)

    val size = sizeof(((1L,1L),1.0D))
    println("*** %d %d  %.2f MB".format(n,m,(n*m)*size/(1024.0*1024.0)))

      t = System.currentTimeMillis()

      v("""

         var R: matrix[Double] = matrix();

         for i = 0, n-1 do
             for j = 0, n-1 do {
                  R[i,j] := 0.0;
                  for k = 0, mm-1 do
                      R[i,j] += M[i,k]*N[k,j];
             };

         println(R.size);

        """)

      println("**** MultiplyParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }

  import scala.annotation.tailrec


  def pageRank ( vertices: Int, edges: Int ) {
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

    val E = (1 to edges).map(x => addEdge(vn))
                        .map{ case (i,j) => ((i.toLong,j.toLong),true) }
                        .toIterable.par

    val size = sizeof(((1L,1L),true))
    println("*** %d %d  %.2f MB".format(vertices,edges,edges*size/(1024.0*1024.0)))

      t = System.currentTimeMillis()

      v("""

         var P: vector[Double] = vector();
         var C: vector[Int] = vector();
         var N: Int = vertices;
         var b: Double = 0.85;

         for i = 1, N do {
             C[i] := 0;
             P[i] := 1.0/N;
         };

         for i = 1, N do
             for j = 1, N do
                if (E[i,j])
                   C[i] += 1;

         var k: Int = 0;

         while (k < num_steps) {
           var Q: matrix[Double] = matrix();
           k += 1;
           for i = 1, N do
             for j = 1, N do
                 if (E[i,j])
                    Q[i,j] := P[i];
           for i = 1, N do
               P[i] := (1-b)/N;
           for i = 1, N do
               for j = 1, N do
                   P[i] += b*Q[j,i]/C[j];
         };

         println(P.size);

        """)

      println("**** PagerankParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
   }

  def kmeans ( length: Long ) {
    def getd (): Double = {
      val v = rand.nextDouble()*20.0D
      if (v.toInt % 2 == 0) getd() else v
    }

    val P = (1L to length).map{ i => (i.toLong,(getd(),getd())) }.toIterable.par

    val size = sizeof((1.0D,1.0D))
    println("*** %d  %.2f MB".format(length,length*size/(1024.0*1024.0)))

    var initial_centroids
          = (for { i <- 0 to 9; j <- 0 to 9 }
             yield ((i*2+1.2).toDouble,(j*2+1.2).toDouble)).toArray

    var C = initial_centroids.zipWithIndex.map{ case (p,i) => (i.toLong,p) }

      def distance ( x: (Double,Double), y: (Double,Double) ): Double
        = Math.sqrt((x._1-y._1)*(x._1-y._1)+(x._2-y._2)*(x._2-y._2))

      case class ArgMin ( index: Long, distance: Double ) {
        def ^ ( x: ArgMin ): ArgMin
          = if (distance <= x.distance) this else x
      }

      case class Avg ( sum: (Double,Double), count: Long ) {
        def ^^ ( x: Avg ): Avg
          = Avg((sum._1+x.sum._1,sum._2+x.sum._2),count+x.count)
        def value(): (Double,Double)
          = (sum._1/count,sum._2/count)
      }

      val K = C.length
      val N = P.size

      var avg = (1 to K).map{ i => (i.toLong-1,Avg((0.0,0.0),0)) }.toArray

      t = System.currentTimeMillis()

      v("""
        var closest: vector[ArgMin] = vector();
        var steps: Int = 0;
        while (steps < num_steps) {
           steps += 1;
           for i = 0, N-1 do {
               closest[i] := ArgMin(0,10000.0);
               for j = 0, K-1 do
                   closest[i] := closest[i] ^ ArgMin(j,distance(P[i],C[j]));
               avg[closest[i].index] := avg[closest[i].index] ^^ Avg(P[i],1);
           };
           for i = 0, K-1 do
               C[i] := avg[i].value();
        };
        """)
      println(C.length)

      println("**** KMeansParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
  }

  val a = 0.002
  val b = 0.02

  implicit class Mult ( private val value: Double ) extends AnyVal {
    def ^ ( that: Double ): Double
      = value+(1-a*b)*that
  }

  def factorization ( n: Int, m: Int ) {
    import Math._
    val d = 2
    val mm = m

    val l = Random.shuffle((0 until n).toList)
    val r = Random.shuffle((0 until m).toList)
    val R = l.flatMap{ i => r.map{ j => ((i.toLong,j.toLong),Math.floor(rand.nextDouble()*5+1).toInt) } }.toIterable.par

    val size = sizeof(((1L,1L),1))
    println("*** %d %d  %.2f MB".format(n,m,(n*m)*size/(1024.0*1024.0)))

      t = System.currentTimeMillis()

      v("""
         var P: matrix[Double] = matrix();
         var Q: matrix[Double] = matrix();
         var pq: matrix[Double] = matrix();
         var E: matrix[Double] = matrix();

         for i = 0, n-1 do
             for k = 0, d-1 do
                 P[i,k] := random();

         for k = 0, d-1 do
             for j = 0, mm-1 do
                 Q[k,j] := random();

         var steps: Int = 0;
         while ( steps < num_steps ) {
           steps += 1;
           for i = 0, n-1 do
               for j = 0, mm-1 do {
                   pq[i,j] := 0.0;
                   for k = 0, d-1 do
                       pq[i,j] += P[i,k]*Q[k,j];
                   E[i,j] := R[i,j]-pq[i,j];
                   for k = 0, d-1 do {
                       P[i,k] := P[i,k] ^ (2*a*E[i,j]*Q[k,j]);
                       Q[k,j] := Q[k,j] ^ (2*a*E[i,j]*P[i,k]);
                   };
               };
         };

         println(P.size);
         println(Q.size);
        """)

      println("**** FactorizationParallel run time: "+(System.currentTimeMillis()-t)/1000.0+" secs")
    }

  def main ( args: Array[String] ) {
    val repeats = args(0).toInt
    val scale = args(1).toInt

    for ( i <- 1 to repeats ) {
      conditionalSum(20000000*scale)
      textProcessing(10000000*scale)
      wordCount(1000000*scale)
      histogram(1000000*scale)
      linearRegression(2000000*scale)
      groupBy(1000000*scale)
      add(30*Math.sqrt(scale).toInt,30*Math.sqrt(scale).toInt)
      multiply(60*Math.sqrt(scale).toInt,60*Math.sqrt(scale).toInt)
      pageRank(30000*scale,30000*scale)
      kmeans(10000*scale)
      factorization(140*Math.sqrt(scale).toInt,140*Math.sqrt(scale).toInt)
    }
  }
}
