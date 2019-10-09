/*
 * Copyright © 2017 University of Texas at Arlington
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uta.diql

import scala.reflect.macros.whitebox.Context
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer


@SerialVersionUID(100L)
sealed abstract class Lineage ( val tree: Int, val value: Any ) extends Serializable
case class UnaryLineage ( override val tree: Int, override val value: Any,
                          lineage: Iterable[Lineage] )
     extends Lineage(tree,value)
case class BinaryLineage ( override val tree: Int, override val value: Any,
                           left: Iterable[Lineage], right: Iterable[Lineage] )
     extends Lineage(tree,value)

@SerialVersionUID(100L)
sealed abstract class LiftedResult[T] ( val lineage: Lineage ) extends Serializable
case class ResultValue[T] ( value: T, override val lineage: Lineage )
     extends LiftedResult[T](lineage)
case class ErasedValue[T] ( override val lineage: Lineage )
     extends LiftedResult[T](lineage)
case class ErrorValue[T] ( message: String, override val lineage: Lineage )
     extends LiftedResult[T](lineage)


/** Used for inverse ordering (Flink requires POJOs for custom key ordering) */
@SerialVersionUID(100L)
class Inv[K] ( val value: K ) ( implicit ord: K => Ordered[K] )
      extends Comparable[Inv[K]] with Serializable {
    override def compareTo ( y: Inv[K] ): Int = -value.compare(y.value)
    override def toString: String = "Inv("+value+")"
}

/** Used by the avg/e aggregation */
@SerialVersionUID(100L)
class Avg[T] ( val sum: T, val count: Long ) ( implicit num: Numeric[T] ) extends Serializable {
    def avg_combine ( other: Avg[T] ): Avg[T]
       = new Avg[T](num.plus(sum,other.sum),count+other.count)
    def value: Double = num.toDouble(sum)/count
    override def toString: String = sum+"/"+count
}


package object core {

  var distributed = core.DistributedEvaluator.distributed

  /** list of defined monoids; other infix operations are just semigroups */
  var monoids
      = Map( "+" -> "0", "*" -> "1", "&&" -> "true", "||" -> "false",
             "count" -> "0", "avg_combine" -> "new Avg(0,0L)", "min" -> null,
             "max" -> null, "avg" -> null
           )

  type macroDefType = (List[(String,Type)],Expr)

  /** macro definitions */
  val macro_defs = new HashMap[String,macroDefType]()

  def findMacros ( name: String, args: Int ): Iterable[macroDefType]
    = macro_defs.filter{ case (n,(ps,_)) => n == name && ps.length == args }.values

  /** return the zero element of the monoid, if any */
  def monoid ( c: Context, m: Monoid ): Option[c.Tree] =
    m match {
      case BaseMonoid(n)
        => if (monoids.contains(n) && monoids(n) != null)
              Some(c.parse(monoids(n)))
           else None
      case ProductMonoid(ms)
        => val cs = ms.flatMap(monoid(c,_))
           import c.universe._
           if (cs.length == ms.length)
              Some(q"(..$cs)")
           else None
      case ParametricMonoid(_,p)
        => monoid(c,p)
      case _ => throw new Error("Unexpected monoid: "+m)
  }

  // sort by K and remove duplicates (return last duplicate)
  def sortIterator[K,V] ( x: Iterator[(K,V)] ) ( implicit ord: Ordering[K] ): Iterator[(K,V)]
    = new Iterator[(K,V)] {
        val i = x.toArray.sortBy(_._1).toIterator
        var prev: (K,V) = _
        var data: (K,V) = _
        var last = false
        override def next (): (K,V) = {
          if (last) {
             last = false
             return data
          }
          do {
            prev = data
            data = i.next()
          } while (i.hasNext && (prev == null || data._1 == prev._1))
          if (!i.hasNext && (prev == null || data._1 == prev._1))
             data
          else {
            last = !i.hasNext
            prev
          }
        }
        override def hasNext: Boolean
          = i.hasNext || last
    }

  // merge sorted streams on K and combine values on the same key using op
  def mergeIterators[K,V] ( op: (V,V)=>V ) ( xi: Iterator[(K,V)], yi: Iterator[(K,V)] ) ( implicit ord: Ordering[K] ): Iterator[(K,V)] = {
    def next ( i: Iterator[(K,V)] ): (K,V)
      = if (i.hasNext) i.next() else null
    val a = new ArrayBuffer[(K,V)]()
    var x = next(xi)
    var y = next(yi)
    while (x != null || y != null) {
      if ( y == null ) {
         a += x
         x = next(xi)
      } else if ( x != null && x._1 == y._1 ) {
         a += ( (x._1,op(x._2,y._2)) )
         x = next(xi)
         y = next(yi)
      } else if ( x != null && ord.lt(x._1,y._1) ) {
         a += x
         x = next(xi)
      } else {
         a += y
         y = next(yi)
      }
    }
    a.toIterator
  }

  var diql_explain = false
  var diql_streaming = false
}
