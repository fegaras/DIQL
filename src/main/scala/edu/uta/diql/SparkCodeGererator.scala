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

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox.Context
import scala.language.experimental.macros
import scala.language.higherKinds


abstract class SparkCodeGenerator extends DistributedCodeGenerator[RDD] {
  import c.universe.{Expr=>_,_}
  import AST._

  override def typeof ( c: Context ) = c.typeOf[RDD[_]]

  override def mkType ( c: Context ) ( tp: c.Tree ): c.Tree = {
    import c.universe._
    tq"RDD[$tp]"
  }

  /** Default Spark implementation of the algebraic operations
   *  used for type-checking in CodeGenerator.code
   */
  override def flatMap[A,B] ( f: (A) => TraversableOnce[B], S: RDD[A] )
          (implicit tag: ClassTag[B]): RDD[B]
    = S.flatMap[B](f)

  // bogus; used for type-checking only
  override def flatMap2[A,B] ( f: (A) => RDD[B], S: RDD[A] )
          (implicit tag: ClassTag[B]): RDD[B]
    = S.flatMap[B](f(_).collect)

  // bogus; used for type-checking only
  override def flatMap[A,B] ( f: (A) => RDD[B], S: Traversable[A] ): RDD[B]
    = f(S.head)

  override def groupBy[K,A] ( S: RDD[(K,A)] )
          (implicit kt: ClassTag[K], vt: ClassTag[A]): RDD[(K,Iterable[A])]
    = new PairRDDFunctions(S).groupByKey()

  override def orderBy[K,A] ( S: RDD[(K,A)] )
          ( implicit ord: Ordering[K], kt: ClassTag[K], at: ClassTag[A] ): RDD[A]
    = S.sortBy(_._1).values

  override def reduce[A] ( acc: (A,A) => A, S: RDD[A] ): A
    = S.reduce(acc)

  override def coGroup[K,A,B] ( X: RDD[(K,A)], Y: RDD[(K,B)] )
          (implicit kt: ClassTag[K], vt: ClassTag[A]): RDD[(K,(Iterable[A],Iterable[B]))]
    = X.cogroup(Y)

  override def coGroup[K,A,B] ( X: Traversable[(K,A)], Y: RDD[(K,B)] )
          (implicit kt: ClassTag[K], vt: ClassTag[B]): RDD[(K,(Iterable[A],Iterable[B]))]
    = broadcastCogroupLeft(X,Y)

  override def coGroup[K,A,B] ( X: RDD[(K,A)], Y: Traversable[(K,B)] )
          (implicit kt: ClassTag[K], vt: ClassTag[A]): RDD[(K,(Iterable[A],Iterable[B]))]
    = broadcastCogroupRight(X,Y)

  override def cross[A,B] ( X: RDD[A], Y: RDD[B] )
          (implicit bt: ClassTag[B]): RDD[(A,B)]
    = X.cartesian(Y)

  override def cross[A,B] ( X: Traversable[A], Y: RDD[B] )
          (implicit bt: ClassTag[B]): RDD[(A,B)]
    = broadcastCrossLeft(X,Y)

  override def cross[A,B] ( X: RDD[A], Y: Traversable[B] )
          (implicit bt: ClassTag[A]): RDD[(A,B)]
    = broadcastCrossRight(X,Y)

  override def merge[A] ( X: RDD[A], Y: RDD[A] ): RDD[A]
    = X++Y

  override def collect[A] ( X: RDD[A] ): Array[A]
    = X.collect()

  override def cache[A] ( X: RDD[A] ): RDD[A]
    = X.cache()

  // bogus; used for type-checking only
  override def head[A] ( X: RDD[A] ): A = X.first()

  def broadcastCogroupLeft[K,A,B] ( X: Traversable[(K,A)], Y: RDD[(K,B)] )
         (implicit kt: ClassTag[K], vt: ClassTag[B]): RDD[(K,(Iterable[A],Iterable[B]))] = {
    val bc = Y.sparkContext.broadcast(X.groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    Y.groupByKey().flatMap( y => bc.value.get(y._1) match {
                                    case Some(xs) => List((y._1,(xs.toIterable,y._2)))
                                    case _ => Nil
                                 } )
  }

  def broadcastCogroupLeft[K,A,B] ( X: RDD[(K,A)], Y: RDD[(K,B)] )
         (implicit kt: ClassTag[K], vt: ClassTag[B]): RDD[(K,(Iterable[A],Iterable[B]))]
    = broadcastCogroupLeft(X.collect(),Y)

  def broadcastCogroupRight[K,A,B] ( X: RDD[(K,A)], Y: Traversable[(K,B)] )
           (implicit kt: ClassTag[K], vt: ClassTag[A]): RDD[(K,(Iterable[A],Iterable[B]))] = {
    val bc = X.sparkContext.broadcast(Y.groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    X.groupByKey().flatMap( x => bc.value.get(x._1) match {
                                    case Some(ys) => List((x._1,(x._2,ys.toIterable)))
                                    case _ => Nil
                                 } )
  }

  def broadcastCogroupRight[K,A,B] ( X: RDD[(K,A)], Y: RDD[(K,B)] )
           (implicit kt: ClassTag[K], vt: ClassTag[A]): RDD[(K,(Iterable[A],Iterable[B]))]
    = broadcastCogroupRight(X,Y.collect())

  def broadcastJoinLeft[K,A,B] ( X: Traversable[(K,A)], Y: RDD[(K,B)] )
         (implicit kt: ClassTag[K], vt: ClassTag[B]): RDD[(K,(A,B))] = {
    val bc = Y.sparkContext.broadcast(X.groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    Y.flatMap( y => bc.value.get(y._1) match {
                            case Some(xs) => xs.map( x => (y._1,(x,y._2)) )
                            case _ => Nil
                      } )
  }

  def broadcastJoinLeft[K,A,B] ( X: RDD[(K,A)], Y: RDD[(K,B)] )
         (implicit kt: ClassTag[K], vt: ClassTag[B]): RDD[(K,(A,B))]
    = broadcastJoinLeft(X.collect(),Y)

  def broadcastJoinRight[K,A,B] ( X: RDD[(K,A)], Y: Traversable[(K,B)] )
         (implicit kt: ClassTag[K], vt: ClassTag[A]): RDD[(K,(A,B))] = {
    val bc = X.sparkContext.broadcast(Y.groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    X.flatMap( x => bc.value.get(x._1) match {
                            case Some(ys) => ys.map( y => (x._1,(x._2,y)) )
                            case _ => Nil
                      } )
  }

  def broadcastJoinRight[K,A,B] ( X: RDD[(K,A)], Y: RDD[(K,B)] )
         (implicit kt: ClassTag[K], vt: ClassTag[A]): RDD[(K,(A,B))]
    = broadcastJoinRight(X,Y.collect())

  def broadcastCrossLeft[A,B] ( X: Traversable[A], Y: RDD[B] )
         (implicit at: ClassTag[B]): RDD[(A,B)] = {
    val bc = Y.sparkContext.broadcast(X)
    Y.flatMap( y => bc.value.map( x => (x,y) ) )
  }

  def broadcastCrossLeft[A,B] ( X: RDD[A], Y: RDD[B] )
         (implicit at: ClassTag[B]): RDD[(A,B)]
    = broadcastCrossLeft(X.collect(),Y)

  def broadcastCrossRight[A,B] ( X: RDD[A], Y: Traversable[B] )
         (implicit bt: ClassTag[A]): RDD[(A,B)] = {
    val bc = X.sparkContext.broadcast(Y)
    X.flatMap( x => bc.value.map( y => (x,y) ) )
  }

  def broadcastCrossRight[A,B] ( X: RDD[A], Y: RDD[B] )
         (implicit bt: ClassTag[A]): RDD[(A,B)]
    = broadcastCrossRight(X,Y.collect())

  def pullReduces ( e: Expr, keyvars: List[String], svar: String ): Option[Set[Expr]] =
    e match {
      case reduce(m,vs)
        => Some(Set(e))
      case Var(v) if keyvars.contains(v)
        => Some(Set())
      case _ => accumulate[Option[Set[Expr]]](e,pullReduces(_,keyvars,svar),
                  { case (Some(a),Some(r)) => Some(a++r)
                    case _ => None
                  },
                None)
    }

  /** The Spark code generator for algebraic terms */
  override def codeGen( e: Expr, env: Map[c.Tree,c.Tree] ): c.Tree = {
    e match {
      case MatchE(x,List(Case(p@VarPat(v),BoolConst(true),b)))
        if occurrences(v,b) > 1 && isDistributed(x)
        => val xc = codeGen(x,env)
           val tp = getType(xc,env)
           val vc = TermName(v)
           val bc = codeGen(b,add(p,tp,env))
           return q"{ val $vc = $xc.cache(); $bc }"
      case _ =>
    if (!isDistributed(e))   // if e is not an RDD operation, use the code generation for Traversable
       return super.codeGen(e,env,codeGen(_,_))
    else e match {
      case flatMap(Lambda(TuplePat(List(VarPat(v),_)),Elem(Var(_v))),
                   groupBy(x))
        if _v == v
        => val xc = codeGen(x,env)
           q"$xc.distinct()"
      case flatMap(Lambda(TuplePat(List(k,vs)),
                          Elem(Tuple(List(k_,reduce(m,vs_))))),
                   groupBy(x))
        if k_ == k && vs_ == vs
        => val xc = codeGen(x,env)
           val fm = TermName(method_name(m))
           monoid(c,m) match {
             case Some(mc) => q"$xc.foldByKey($mc)(_ $fm _)"
             case _ => q"$xc.reduceByKey(_ $fm _)"
           }
      case flatMap(Lambda(p@TuplePat(List(k,TuplePat(List(xs,ys)))),
                          flatMap(Lambda(px,flatMap(Lambda(py,Elem(b)),ys_)),xs_)),
                   coGroup(x,y))
        if xs_ == toExpr(xs) && ys_ == toExpr(ys)
           && occurrences(patvars(xs)++patvars(ys),b) == 0
           && irrefutable(p)
        => val xc = codeGen(x,env)
           val yc = codeGen(y,env)
           val kc = code(k)
           val pxc = code(px)
           val pyc = code(py)
           val bc = codeGen(b,env)
           val join = if (smallDataset(x))
                         q"core.distributed.broadcastJoinLeft($xc,$yc)"
                      else if (smallDataset(y))
                         q"core.distributed.broadcastJoinRight($xc,$yc)"
                      else q"$xc.join($yc)"
           q"$join.map{ case ($kc,($pxc,$pyc)) => $bc }"
      case flatMap(Lambda(p,Elem(b)),x)
        if irrefutable(p)
        => val pc = code(p)
           val (_,tp,xc) = typedCode(x,env,codeGen(_,_))
           val bc = codeGen(b,add(p,tp,env))
           q"$xc.map{ case $pc => $bc }"
      case flatMap(Lambda(p,IfE(d,Elem(b),Empty())),x)
        if irrefutable(p)
        => val pc = code(p)
           val (_,tp,xc) = typedCode(x,env,codeGen(_,_))
           val dc = codeGen(d,add(p,tp,env))
           val bc = codeGen(b,add(p,tp,env))
           if (toExpr(p) == b)
              q"$xc.filter{ case $pc => $dc }"
           else q"$xc.filter{ case $pc => $dc }.map{ case $pc => $bc }"
      case flatMap(Lambda(p,b),x)
        => val pc = code(p)
           val (_,tp,xc) = typedCode(x,env,codeGen(_,_))
           val bc = codeGen(b,add(p,tp,env))
           if (irrefutable(p))
              q"$xc.flatMap{ case $pc => $bc }"
           else q"$xc.flatMap{ case $pc => $bc; case _ => Nil }"
      case groupBy(x)
        => val xc = codeGen(x,env)
           q"$xc.groupByKey()"
      case orderBy(x)
        => val xc = codeGen(x,env)
           // Spark bug: q"$xc.sortBy(_._1).values doesn't work correctly in local mode
           q"$xc.sortBy(_._1,true,1).values"
      case coGroup(x,y)
        => val xc = codeGen(x,env)
           val yc = codeGen(y,env)
           if (smallDataset(x))
              q"core.distributed.broadcastCogroupLeft($xc,$yc)"
           else if (smallDataset(y))
              q"core.distributed.broadcastCogroupRight($xc,$yc)"
           else q"$xc.cogroup($yc)"
      case cross(x,y)
        => val xc = codeGen(x,env)
           val yc = codeGen(y,env)
           if (smallDataset(x))
              q"core.distributed.broadcastCrossLeft($xc,$yc)"
           else if (smallDataset(y))
              q"core.distributed.broadcastCrossRight($xc,$yc)"
           else q"$xc.cartesian($yc)"
      case reduce("+",flatMap(Lambda(p,Elem(LongConst(1))),x))
        => val xc = codeGen(x,env)
           q"$xc.count()"
      case reduce(m,x)
        => val (_,tp,xc) = typedCode(x,env,codeGen(_,_))
           val fm = accumulator(m,tp)
           monoid(c,m) match {
             case Some(mc) => q"$xc.fold($mc)($fm)"
             case _ => q"$xc.reduce($fm)"
           }
      case Merge(x,y)
        => val xc = codeGen(x,env)
           val yc = codeGen(y,env)
           q"$xc++$yc"
      case _ => super.codeGen(e,env,codeGen(_,_))
    } }
  }

  /** Convert RDD method calls to algebraic terms so that they can be optimized */
  override def algebraGen ( e: Expr ): Expr = {
    import edu.uta.diql.core.{flatMap=>FlatMap,coGroup=>CoGroup,cross=>Cross}
    e match {
      case MethodCall(x,"map",List(Lambda(p,b)))
        => FlatMap(Lambda(p,Elem(algebraGen(b))),algebraGen(x))
      case MethodCall(x,"flatMap",List(Lambda(p,b)))
        => FlatMap(Lambda(p,algebraGen(b)),algebraGen(x))
      case MethodCall(x,"cogroup",List(y))
        => CoGroup(algebraGen(x),algebraGen(y))
      case MethodCall(x,"cartesian",List(y))
        => Cross(algebraGen(x),algebraGen(y))
      case MethodCall(x,"join",List(y))
        => val vx = newvar
           val vy = newvar
           val xs = newvar
           val ys = newvar
           val b = FlatMap(Lambda(VarPat(vx),
                                  FlatMap(Lambda(VarPat(vy),
                                                 Elem(Tuple(List(Var(vx),Var(vy))))),
                                          Var(ys))),
                           Var(xs))
           FlatMap(Lambda(TuplePat(List(StarPat(),TuplePat(List(VarPat(xs),VarPat(ys))))),b),
                   CoGroup(algebraGen(x),algebraGen(y)))
      case _ => apply(e,algebraGen(_))
    }
  }
}
