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
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream._
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox.Context


abstract class SparkCodeGenerator extends DistributedCodeGenerator {
  import c.universe.{Expr=>_,_}
  import AST._
  import edu.uta.diql.{LiftedResult,ResultValue}

  override def typeof ( c: Context ) = c.typeOf[RDD[_]]

  override def mkType ( c: Context ) ( tp: c.Tree ): c.Tree = {
    import c.universe._
    tq"RDD[$tp]"
  }

  /** Is tp a data stream? */
  override def isStream ( c: Context ) ( tp: c.Type ): Boolean
    = tp <:< c.typeOf[DStream[_]]

  def debug[T: ClassTag] ( value: RDD[LiftedResult[T]], exprs: List[String] ): RDD[T] = {
    val debugger = new Debugger(value.collect,exprs)
    debugger.debug()
    value.flatMap{ case ResultValue(v,_) => List(v) case _ => Nil }
  }

  /** Default Spark implementation of the algebraic operations
   *  used for type-checking in CodeGenerator.code
   */
  def flatMap[A, B: ClassTag]
        ( f: (A) => TraversableOnce[B], S: RDD[A] ): RDD[B]
    = S.flatMap[B](f)

  // bogus; used for type-checking only
  def flatMap2[A, B: ClassTag]
        ( f: (A) => RDD[B], S: RDD[A] ): RDD[B]
    = S.flatMap[B](f(_).collect)

  // bogus; used for type-checking only
  def flatMap[A,B] ( f: (A) => RDD[B], S: Traversable[A] ): RDD[B]
    = f(S.head)

  def groupBy[K: ClassTag, A: ClassTag]
        ( S: RDD[(K,A)] ): RDD[(K,Iterable[A])]
    = new PairRDDFunctions(S).groupByKey()

  def orderBy[K: ClassTag, A: ClassTag]
        ( S: RDD[(K,A)] ) ( implicit ord: Ordering[K] ): RDD[A]
    = S.sortByKey(true,1).values

  def reduce[A] ( acc: (A,A) => A, S: RDD[A] ): A
    = S.reduce(acc)

  def coGroup[K: ClassTag, A: ClassTag, B]
        ( X: RDD[(K,A)], Y: RDD[(K,B)] ): RDD[(K,(Iterable[A],Iterable[B]))]
    = X.cogroup(Y)

  def coGroup[K: ClassTag, A, B: ClassTag]
        ( X: Traversable[(K,A)], Y: RDD[(K,B)] ): RDD[(K,(Iterable[A],Iterable[B]))]
    = broadcastCogroupLeft(X,Y)

  def coGroup[K: ClassTag, A: ClassTag, B]
        ( X: RDD[(K,A)], Y: Traversable[(K,B)] ): RDD[(K,(Iterable[A],Iterable[B]))]
    = broadcastCogroupRight(X,Y)

  def cross[A, B: ClassTag]
        ( X: RDD[A], Y: RDD[B] ): RDD[(A,B)]
    = X.cartesian(Y)

  def cross[A, B: ClassTag]
        ( X: Traversable[A], Y: RDD[B] ): RDD[(A,B)]
    = broadcastCrossLeft(X,Y)

  def cross[A: ClassTag, B]
        ( X: RDD[A], Y: Traversable[B] ): RDD[(A,B)]
    = broadcastCrossRight(X,Y)

  def merge[A] ( X: RDD[A], Y: RDD[A] ): RDD[A]
    = X++Y

  def collect[A] ( X: RDD[A] ): Array[A]
    = X.collect()

  def cache[A] ( X: RDD[A] ): RDD[A]
    = X.cache()

  // bogus; used for type-checking only
  def head[A] ( X: RDD[A] ): A = X.first()

  def broadcastCogroupLeft[K: ClassTag, A, B: ClassTag]
        ( X: Traversable[(K,A)], Y: RDD[(K,B)] ): RDD[(K,(Iterable[A],Iterable[B]))] = {
    val bc = Y.sparkContext.broadcast(X.groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    Y.groupByKey().map( y => bc.value.get(y._1) match {
                                    case Some(xs) => (y._1,(xs.toIterable,y._2))
                                    case _ => (y._1,(Nil,y._2))
                                 } )
  }

  def broadcastCogroupLeft[K: ClassTag, A, B: ClassTag]
        ( X: RDD[(K,A)], Y: RDD[(K,B)] ): RDD[(K,(Iterable[A],Iterable[B]))]
    = broadcastCogroupLeft(X.collect(),Y)

  def broadcastCogroupRight[K: ClassTag, A: ClassTag, B]
        ( X: RDD[(K,A)], Y: Traversable[(K,B)] ): RDD[(K,(Iterable[A],Iterable[B]))] = {
    val bc = X.sparkContext.broadcast(Y.groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    X.groupByKey().map( x => bc.value.get(x._1) match {
                                    case Some(ys) => (x._1,(x._2,ys.toIterable))
                                    case _ => (x._1,(x._2,Nil))
                                 } )
  }

  def broadcastCogroupRight[K: ClassTag, A: ClassTag, B]
        ( X: RDD[(K,A)], Y: RDD[(K,B)] ): RDD[(K,(Iterable[A],Iterable[B]))]
    = broadcastCogroupRight(X,Y.collect())

  def broadcastJoinLeft[K: ClassTag, A, B: ClassTag]
        ( X: Traversable[(K,A)], Y: RDD[(K,B)] ): RDD[(K,(A,B))] = {
    val bc = Y.sparkContext.broadcast(X.groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    Y.flatMap( y => bc.value.get(y._1) match {
                            case Some(xs) => xs.map( x => (y._1,(x,y._2)) )
                            case _ => Nil
                      } )
  }

  def broadcastJoinLeft[K: ClassTag, A, B: ClassTag]
        ( X: RDD[(K,A)], Y: RDD[(K,B)] ): RDD[(K,(A,B))]
    = broadcastJoinLeft(X.collect(),Y)

  def broadcastJoinRight[K: ClassTag, A: ClassTag, B]
        ( X: RDD[(K,A)], Y: Traversable[(K,B)] ): RDD[(K,(A,B))] = {
    val bc = X.sparkContext.broadcast(Y.groupBy(_._1).mapValues(_.map(_._2)).map(identity))
    X.flatMap( x => bc.value.get(x._1) match {
                            case Some(ys) => ys.map( y => (x._1,(x._2,y)) )
                            case _ => Nil
                      } )
  }

  def broadcastJoinRight[K: ClassTag, A: ClassTag, B]
        ( X: RDD[(K,A)], Y: RDD[(K,B)] ): RDD[(K,(A,B))]
    = broadcastJoinRight(X,Y.collect())

  def broadcastCrossLeft[A, B: ClassTag]
        ( X: Traversable[A], Y: RDD[B] ): RDD[(A,B)] = {
    val bc = Y.sparkContext.broadcast(X)
    Y.flatMap( y => bc.value.map( x => (x,y) ) )
  }

  def broadcastCrossLeft[A, B: ClassTag]
        ( X: RDD[A], Y: RDD[B] ): RDD[(A,B)]
    = broadcastCrossLeft(X.collect(),Y)

  def broadcastCrossRight[A: ClassTag, B]
        ( X: RDD[A], Y: Traversable[B] ): RDD[(A,B)] = {
    val bc = X.sparkContext.broadcast(Y)
    X.flatMap( x => bc.value.map( y => (x,y) ) )
  }

  def broadcastCrossRight[A: ClassTag, B]
        ( X: RDD[A], Y: RDD[B] ): RDD[(A,B)]
    = broadcastCrossRight(X,Y.collect())

  private def occursInFunctional ( v: String, e: Expr ): Boolean
    = e match {
        case flatMap(f,_)
          if occurrences(v,f) > 0
          => true
        case Elem(b)
          if occurrences(v,b) > 0
          => true
        case Tuple(s)
          if s.map(occurrences(v,_)).sum > 0
          => true
        case _ => AST.accumulate[Boolean](e,occursInFunctional(v,_),_||_,false)
    }

  private def isReduce ( e: Expr ): Boolean = e match { case reduce(_,_) => true; case _ => false }

  /** The Spark code generator for algebraic terms */
  override def codeGen( e: Expr, env: Environment ): c.Tree = {
    e match {
      case MatchE(x,List(Case(p@VarPat(v),BoolConst(true),b)))
        if isDistributed(x) && occursInFunctional(v,b) && !isReduce(x)
        // if x is an RDD that occurs in a functional, it must be broadcast
        => val xc = codeGen(x,env)
           val xtp = getType(xc,env)
           val vn = TermName(v)
           val bv = c.freshName("bv")
           val bn = TermName(bv)
           val tp = getType(q"$vn.sparkContext.broadcast($vn.collect.toList)",
                            add(VarPat(v),xtp,env))
           val bc = codeGen(subst(Var(v),MethodCall(Var(bv),"value",null),b),
                            add(VarPat(bv),tp,env))
           q"""{ val $vn:$xtp = $xc
                 val $bn = $vn.sparkContext.broadcast($vn.collect.toList)
                 $bc
               }"""
      case MatchE(x,List(Case(p@VarPat(v),BoolConst(true),b)))
        // if x is an RDD that occurs more than once, cache it
        if isDistributed(x)  && isReduce(x)
      => val xc = codeGen(x,env)
         val tp = getType(xc,env)
         val vc = TermName(v)
         typedCodeOpt(xc,tp,env,codeGen) match {
            case Some(t)
              => val nv = Var(v)
                 nv.tpe = t
                 x.tpe = t
                 val bc = codeGen(subst(Var(v),nv,b),add(p,tp,env))
                 return q"{ val $vc:$tp = $xc; $bc }"
            case None =>
          }
         val bc = codeGen(b,add(p,tp,env))
         q"{ val $vc:$tp = $xc; $bc }"
      case MatchE(x,List(Case(p@VarPat(v),BoolConst(true),b)))
        // if x is an RDD that occurs more than once, cache it
        if occurrences(v,b) > 1 && isDistributed(x) && !isReduce(x)
      => val xc = codeGen(x,env)
         val tp = getType(xc,env)
         val vc = TermName(v)
         typedCodeOpt(xc,tp,env,codeGen) match {
            case Some(t)
              => val nv = Var(v)
                 nv.tpe = t
                 x.tpe = t
                 val bc = codeGen(subst(Var(v),nv,b),add(p,tp,env))
                 return q"{ val $vc:$tp = $xc.cache(); $bc }"
            case None =>
         }
         val bc = codeGen(b,add(p,tp,env))
         q"{ val $vc:$tp = $xc; $bc }"
      case _ =>
    if (!isDistributed(e))
       // if e is not an RDD operation, use the code generation for Traversable
       super.codeGen(e,env,codeGen)
    else e match {
      case flatMap(Lambda(TuplePat(List(k,TuplePat(List(xs,ys)))),
                          flatMap(Lambda(px,flatMap(Lambda(py,Elem(b)),ys_)),xs_)),
                   coGroup(x,y))
        if xs_ == toExpr(xs) && ys_ == toExpr(ys)
           && occurrences(patvars(xs)++patvars(ys),b) == 0
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
      case flatMap(Lambda(TuplePat(List(k,TuplePat(List(xs,ys)))),
                          flatMap(Lambda(py,flatMap(Lambda(px,Elem(b)),xs_)),ys_)),
                   coGroup(x,y))
        if xs_ == toExpr(xs) && ys_ == toExpr(ys)
           && occurrences(patvars(xs)++patvars(ys),b) == 0
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
      case flatMap(Lambda(p@TuplePat(List(k,TuplePat(List(xs,ys)))),
                          c@flatMap(Lambda(_,b),xs_)),
                   coGroup(x,y))
        if xs_ == toExpr(xs)
           && occurrences(patvars(xs),b) == 0
           && smallDataset(x)
        => val xc = codeGen(x,env)
           val yc = codeGen(y,env)
           val cc = codeGen(c,env)
           val pc = code(p)
           q"core.distributed.broadcastCogroupLeft($xc,$yc).flatMap{ case $pc => $cc }"
      case flatMap(Lambda(p@TuplePat(List(k,TuplePat(List(xs,ys)))),
                          c@flatMap(Lambda(_,b),ys_)),
                   coGroup(x,y))
        if ys_ == toExpr(ys)
           && occurrences(patvars(ys),b) == 0
           && smallDataset(y)
        => val xc = codeGen(x,env)
           val yc = codeGen(y,env)
           val cc = codeGen(c,env)
           val pc = code(p)
           q"core.distributed.broadcastCogroupRight($xc,$yc).flatMap{ case $pc => $cc }"
      case flatMap(Lambda(TuplePat(List(VarPat(v),_)),Elem(Var(_v))),
                   groupBy(x))
        if _v == v
        => val xc = codeGen(x,env)
           q"$xc.map(_._1).distinct()"
      case flatMap(Lambda(TuplePat(List(VarPat(k),VarPat(vs))),
                          Elem(Tuple(List(Var(k_),reduce(m@BaseMonoid(n),Var(vs_)))))),
                   groupBy(x))
        if k_ == k && vs_ == vs
        => val xc = codeGen(x,env)
           val fm = TermName(method_name(n))
           monoid(c,m) match {
             case Some(mc) => q"$xc.foldByKey($mc)(_ $fm _)"
             case _ => q"$xc.reduceByKey(_ $fm _)"
           }
      case flatMap(Lambda(p@TuplePat(List(VarPat(kp),_)),
                   MatchE(reduce(m,z@flatMap(Lambda(vars,Elem(y)),Var(_))),
                          List(Case(q,BoolConst(true),b)))),
                   gb@groupBy(x))
        => import edu.uta.diql.core.{flatMap => fm}
           val xc = codeGen(x,env)
           val (_,tp,_) = typedCode(fm(Lambda(p,z),gb),env,codeGen)
           val fz = codeGen(fm(Lambda(TuplePat(List(VarPat(kp),vars)),
                                      Elem(Tuple(List(Var(kp),y)))),x),env)
           val f = accumulator(m,tp,e)
           val nb = codeGen(Lambda(TuplePat(List(VarPat(kp),q)),b),env)
           q"$fz.reduceByKey{ case (x,y) => $f(x,y) }.flatMap($nb)"
      case flatMap(Lambda(p,Elem(b)),x)
        if irrefutable(p)
        => val pc = code(p)
           val (_,tp,xc) = typedCode(x,env,codeGen)
           val bc = codeGen(b,add(p,tp,env))
           q"$xc.map{ case $pc => $bc }"
      case flatMap(Lambda(p,IfE(d,Elem(b),Empty())),x)
        if irrefutable(p)
        => val pc = code(p)
           val (_,tp,xc) = typedCode(x,env,codeGen)
           val dc = codeGen(d,add(p,tp,env))
           val bc = codeGen(b,add(p,tp,env))
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
           q"$xc.groupByKey()"
      case orderBy(x)
        => val xc = codeGen(x,env)
           q"$xc.sortByKey(true,1).values"
      case coGroup(x,y)
        => val xc = codeGen(x,env)
           val yc = codeGen(y,env)
           q"$xc.cogroup($yc)"
      case cross(x,y)
        => val xc = codeGen(x,env)
           val yc = codeGen(y,env)
           if (smallDataset(x))
              q"core.distributed.broadcastCrossLeft($xc,$yc)"
           else if (smallDataset(y))
              q"core.distributed.broadcastCrossRight($xc,$yc)"
           else q"$xc.cartesian($yc)"
      case reduce(BaseMonoid("+"),
                  flatMap(Lambda(p,Elem(LongConst(1))),x))
        => val xc = codeGen(x,env)
           q"$xc.count()"
      case reduce(m,x)
        => val (_,tp,xc) = typedCode(x,env,codeGen)
           val fm = accumulator(m,tp,e)
           monoid(c,m) match {
             case Some(mc)
               => if (isDistributed(x))
                     q"$xc.fold($mc)($fm)"
                  else q"$xc.foldLeft[$tp]($mc)($fm)"
             case _ => q"$xc.reduce($fm)"
           }
      case _ => super.codeGen(e,env,codeGen)
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
      case _ => apply(e,algebraGen)
    }
  }
}
