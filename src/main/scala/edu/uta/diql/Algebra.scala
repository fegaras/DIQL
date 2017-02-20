package edu.uta.diql

object algebra {

  def cMap[A,B] ( f: A => Iterable[B], S: Iterable[A] ): Iterable[B]
    = S.flatMap(f)

  def groupBy[K,A] ( S: Iterable[(K,A)] ): Iterable[(K,Iterable[A])]
    = S.groupBy{ case (k,a) => k }.mapValues( _.map{ case (k,a) => a })

  def orderBy[K,A] ( S: Iterable[(K,A)] ) ( implicit cmp: Ordering[K] ): Iterable[A]
    = S.toSeq.sortWith{ case ((k1,_),(k2,_)) => cmp.lt(k1,k2) }.map(_._2)

  def reduce[A] ( acc: (A,A) => A, S: Iterable[A] ): A
    = S.reduce(acc)

  def coGroup[K,A,B] ( X: Iterable[(K,A)], Y: Iterable[(K,B)] ): Iterable[(K,(Iterable[A],Iterable[B]))]
    = { val xi = X.map{ case (k,x) => (k,Left(x)) }
        val yi = Y.map{ case (k,y) => (k,Right(y)) }
        val g = groupBy(xi++yi)
        g.map{ case (k,xy)
                => (k,(xy.foldLeft(Nil:List[A]){ case (r,Left(x)) => x::r; case (r,_) => r },
                       xy.foldLeft(Nil:List[B]){ case (r,Right(y)) => y::r; case (r,_) => r })) }
     }

  def cross[A,B] ( X: Iterable[A], Y: Iterable[B] ): Iterable[(A,B)]
    = X.flatMap(x => Y.map(y => (x,y)))

  def merge[A] ( X: Iterable[A], Y: Iterable[A] ): Iterable[A]
    = X++Y
}
