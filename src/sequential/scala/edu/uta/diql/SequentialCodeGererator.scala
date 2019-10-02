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
package edu.uta.diql.core

import scala.reflect.ClassTag
import scala.reflect.macros.whitebox.Context


abstract class SequentialCodeGenerator extends DistributedCodeGenerator {
  import c.universe.{Expr=>_,_}
  import AST._

  override def typeof ( c: Context ) = c.typeOf[Iterable[_]]

  override def mkType ( c: Context ) ( tp: c.Tree ): c.Tree = {
    import c.universe._
    tq"Iterable[$tp]"
  }

  /** Is tp a data stream? */
  override def isStream ( c: Context ) ( tp: c.Type ): Boolean = false

  override val datasetClassPath = "Iterable"

  def initialize[K,V] ( ignore: String, v: Traversable[(K,V)] ): Iterable[(K,V)]
    = v.toIterable

  def merge[K,V] ( v: Traversable[(K,V)], op: (V,V)=>V, s: Iterable[(K,V)] ): Array[(K,V)]
    = coGroup(v,s.toList).map{ case (k,(xs,ys)) => (k,(xs++ys).reduce(op)) }.toArray

  def merge[K,V] ( v: Traversable[(K,V)], op: (V,V)=>V, s: Traversable[(K,V)] ): Array[(K,V)]
    = inMemory.coGroup(v,s).map{ case (k,(xs,ys)) => (k,(xs++ys).reduce(op)) }.toArray

  def merge[K,V] ( v: Iterable[(K,V)], op: (V,V)=>V, s: Traversable[(K,V)] ): Iterable[(K,V)]
    = coGroup(v,s).map{ case (k,(xs,ys)) => (k,(xs++ys).reduce(op)) }

  def merge[K,V] ( v: Iterable[(K,V)], op: (V,V)=>V, s: Iterable[(K,V)] ): Iterable[(K,V)]
    = coGroup(v,s).map{ case (k,(xs,ys)) => (k,(xs++ys).reduce(op)) }

  /** Implementation of the algebraic operations in Scala's Parallel library
   */
  def flatMap[A,B] ( f: (A) => TraversableOnce[B], S: Iterable[A] ): Iterable[B]
    = S.flatMap(f)

  def flatMap2[A,B] ( f: (A) => Iterable[B], S: Iterable[A] ): Iterable[B]
    = S.flatMap(f(_).toSeq)

  def groupBy[K,A] ( S: Iterable[(K,A)] ): Iterable[(K,Iterable[A])]
    = S.groupBy(_._1).map{ case (k,s) => (k,s.map(_._2).toList) }.toIterable

  def orderBy[K,A] ( S: Iterable[(K,A)] ) ( implicit ord: Ordering[K] ): Iterable[A]
    = S.toList.sortBy(_._1).map(_._2)

  def reduce[A] ( acc: (A,A) => A, S: Iterable[A] ): A
    = S.reduce(acc)

  private def partitionMap[A1,A2] ( s: List[Either[A1,A2]] ): (List[A1], List[A2]) = {
      val l = scala.collection.mutable.ListBuffer.empty[A1]
      val r = scala.collection.mutable.ListBuffer.empty[A2]
      s.foreach {
          case Left(x1) => l += x1
          case Right(x2) => r += x2
      }
      (l.toList, r.toList)
    }

  def coGroup[K,A,B] ( X: Iterable[(K,A)], Y: Iterable[(K,B)] ): Iterable[(K,(Iterable[A],Iterable[B]))]
    = ( X.map{ case (k,v) => (k,Left(v).asInstanceOf[Either[A,B]]) }
        ++ Y.map{ case (k,v) => (k,Right(v).asInstanceOf[Either[A,B]]) } )
      .groupBy(_._1)
      .map{ case (k,s) => ( k, partitionMap(s.map(_._2).toList) ) }.toIterable

  def coGroup[K,A,B] ( X: Traversable[(K,A)], Y: Iterable[(K,B)] ): Iterable[(K,(Iterable[A],Iterable[B]))]
    = coGroup(X.toIterable,Y)

  def coGroup[K,A,B] ( X: Iterable[(K,A)], Y: Traversable[(K,B)] ): Iterable[(K,(Iterable[A],Iterable[B]))]
    = coGroup(X,Y.toIterable)

  def cross[A,B] ( X: Iterable[A], Y: Iterable[B] ): Iterable[(A,B)]
    = { val ys = Y.toSeq
        X.flatMap( x => ys.map(y => (x,y)) )
      }

  def cross[A,B] ( X: Traversable[A], Y: Iterable[B] ): Iterable[(A,B)]
    = Y.flatMap( y => X.map(x => (x,y)) )

  def cross[A,B] ( X: Iterable[A], Y: Traversable[B] ): Iterable[(A,B)]
    = X.flatMap( x => Y.map(y => (x,y)) )

  def collect[A: ClassTag] ( X: Iterable[A] ): Array[A]
    = X.toArray

  def cache[A] ( X: Iterable[A] ): Iterable[A]
    = X

  def head[A] ( X: Iterable[A] ): A = X.head

  override def codeGen ( e: Expr, env: Environment ): c.Tree
    = if (!isDistributed(e))
        super.codeGen(e,env,codeGen)
      else e match {
        case flatMap(Lambda(p,Elem(b)),x)
          if irrefutable(p)
          => val pc = code(p)
             val (_,tp,xc) = typedCode(x,env,codeGen)
             val bc = codeGen(b,add(p,tp,env))
             if (toExpr(p) == b)
                xc
             else q"$xc.map{ case $pc => $bc }"
        case flatMap(Lambda(p,IfE(d,Elem(b),Empty())),x)
          if irrefutable(p)
          => val pc = code(p)
             val xc = codeGen(x,env)
             val dc = codeGen(d,env)
             val bc = codeGen(b,env)
             if (toExpr(p) == b)
                q"$xc.filter{ case $pc => $dc }"
             else q"$xc.filter{ case $pc => $dc }.map{ case $pc => $bc }"
        case flatMap(Lambda(p,b),x)
          => val pc = code(p)
             val (_,tp,xc) = typedCode(x,env,codeGen)
             val bc = codeGen(b,add(p,tp,env))
             if (irrefutable(p))
                q"$xc.flatMap{ case $pc => $bc }"
             else q"$xc.flatMap{ case $pc => $bc; case _ => Nil }"
        case groupBy(x)
          => val xc = codeGen(x,env)
             q"core.distributed.groupBy($xc)"
        case orderBy(x)
          => val xc = codeGen(x,env)
             q"core.distributed.orderBy($xc)"
        case coGroup(x,y)
          => val xc = codeGen(x,env)
             val yc = codeGen(y,env)
             q"core.distributed.coGroup($xc,$yc)"
        case cross(x,y)
          => val xc = codeGen(x,env)
             val yc = codeGen(y,env)
             q"core.distributed.cross($xc,$yc)"
        case reduce(BaseMonoid("+"),
                    flatMap(Lambda(p,Elem(LongConst(1))),x))
          => val xc = codeGen(x,env)
             q"$xc.size"
        case reduce(m,x)
          => val (_,tp,xc) = typedCode(x,env,codeGen)
             val fm = accumulator(m,tp,e)
             monoid(c,m) match {
               case Some(mc)
                 => if (isDistributed(x))
                      q"$xc.fold[$tp]($mc)($fm)"
                    else q"$xc.foldLeft[$tp]($mc)($fm)"
               case _ => q"$xc.reduce($fm)"
             }
        case _ => super.codeGen(e,env,codeGen)
    }

  /** Convert method calls to algebraic terms so that they can be optimized */
  override def algebraGen ( e: Expr ): Expr = e
}
