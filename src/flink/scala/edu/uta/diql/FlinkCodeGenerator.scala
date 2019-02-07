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

import org.apache.flink.api.scala._
import org.apache.flink.api.common.typeinfo._
import org.apache.flink.api.common.operators.Order._
import org.apache.flink.api.common.functions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.util._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.reflect.macros.whitebox.Context


abstract class FlinkCodeGenerator extends DistributedCodeGenerator {
  import c.universe.{Expr=>_,_}
  import AST._
  import edu.uta.diql.{LiftedResult,ResultValue}

  override def typeof ( c: Context ) = c.typeOf[DataSet[_]]

  override def mkType ( c: Context ) ( tp: c.Tree ): c.Tree = {
    import c.universe._
    tq"DataSet[$tp]"
  }

  /** Is tp a data stream? */
  override def isStream ( c: Context ) ( tp: c.Type ): Boolean
    = false // tp <:< c.typeOf[DStream[_]]

  def debug[T] ( value: DataSet[LiftedResult[T]], exprs: List[String] )
               (implicit bt: ClassTag[T], eb: TypeInformation[T]): DataSet[T] = {
    val debugger = new Debugger(value.collect().toArray,exprs)
    debugger.debug()
    value.flatMap(x => x match { case ResultValue(v,_) => List(v); case _ => Nil })
  }

/** Default Flink implementation of the algebraic operations
   *  used for type-checking in CodeGenerator.code
   */
  def flatMap[A,B] ( f: (A) => TraversableOnce[B], S: DataSet[A] )
        (implicit bt: ClassTag[B], eb: TypeInformation[B]): DataSet[B]
    = S.flatMap(f)

  // bogus; used for type-checking only
  def flatMap2[A,B] ( f: (A) => DataSet[B], S: DataSet[A] )
          (implicit bt: ClassTag[B], eb: TypeInformation[B]): DataSet[B]
    = S.flatMap(f(_).collect)

  // bogus; used for type-checking only
  def flatMap[A,B] ( f: (A) => DataSet[B], S: Traversable[A] ): DataSet[B]
    = f(S.head)

  def keyedIterable[K,A] ( i: Iterator[(K,A)] ): (K,Iterable[A])
    = { val (k,v) = i.next
        (k,List(v)++i.map(_._2))
      }

  // inneficient
  def keyedIterable2[K,A] ( i: Iterator[(K,A)] ): (K,Iterable[A])
    = { var nk: K = null.asInstanceOf[K]
        val ni = i.map{ case (k,v) => nk = k; v }.toIterable
        (nk,ni)
      }

  def reduceIterable[K,A] ( i: Iterator[(K,A)], merge: (A,A) => A ): (K,A)
    = { val (k,v) = i.next
        var acc = v
        while (i.hasNext)
           acc = merge(acc,i.next._2)
        (k,acc)
      }

  def groupBy[K,A] ( S: DataSet[(K,A)] )
          (implicit kt: ClassTag[K], at: ClassTag[A],
              ea: TypeInformation[(K,Iterable[A])]): DataSet[(K,Iterable[A])]
    = S.groupBy(0).reduceGroup(keyedIterable(_))

  def orderBy[K,A] ( S: DataSet[(K,A)] )
          ( implicit kt: ClassTag[K], at: ClassTag[A],
                     ea: TypeInformation[A] ): DataSet[A]
        = S.partitionByRange(0).sortPartition(0,ASCENDING).map(_._2)

  def reduce[A] ( acc: (A,A) => A, S: DataSet[A] ): A
    = S.reduce(acc).collect()(0)

  def coGroup[K,A,B] ( X: DataSet[(K,A)], Y: DataSet[(K,B)] )
          (implicit kt: ClassTag[K], at: ClassTag[A],
           ea: TypeInformation[(K,(Iterable[A],Iterable[B]))]): DataSet[(K,(Iterable[A],Iterable[B]))]
    = X.coGroup(Y).where(0).equalTo(0) {
            (xs,ys) => if (!xs.hasNext) {
                          val (k,i) = keyedIterable(ys)
                          (k,(Nil,i))
                       } else {
                          val (k,i) = keyedIterable(xs)
                          (k,(i,ys.map(_._2).toIterable))
                       }
      }

  def coGroup[K,A,B] ( X: Traversable[(K,A)], Y: DataSet[(K,B)] )
         (implicit kt: ClassTag[K], bt: ClassTag[B], a: TypeInformation[(K,A)],
             ea: TypeInformation[(K,Iterable[A])], eb: TypeInformation[(K,Iterable[B])],
             ek: TypeInformation[(K,(Iterable[A],Iterable[B]))]): DataSet[(K,(Iterable[A],Iterable[B]))]
    = cogroupWithTinyLeft(X,Y)

  def coGroup[K,A,B] ( X: DataSet[(K,A)], Y: Traversable[(K,B)] )
         (implicit kt: ClassTag[K], bt: ClassTag[B], b: TypeInformation[(K,B)],
             ea: TypeInformation[(K,Iterable[A])], eb: TypeInformation[(K,Iterable[B])],
             ek: TypeInformation[(K,(Iterable[A],Iterable[B]))]): DataSet[(K,(Iterable[A],Iterable[B]))]            
    = cogroupWithTinyRight(X,Y)

  def cross[A,B] ( X: DataSet[A], Y: DataSet[B] )
          (implicit bt: ClassTag[B]): DataSet[(A,B)]
    = X.cross(Y)

  def cross[A,B] ( X: Traversable[A], Y: DataSet[B] )
          (implicit at: ClassTag[A], bt: ClassTag[B],
              ea: TypeInformation[A], eb: TypeInformation[B]): DataSet[(A,B)]
    = Y.crossWithTiny(Y.getExecutionEnvironment.fromCollection(X.toSeq)).map( v => (v._2,v._1) )

  def cross[A,B] ( X: DataSet[A], Y: Traversable[B] )
          (implicit at: ClassTag[A], bt: ClassTag[B],
              eb: TypeInformation[B]): DataSet[(A,B)]
    = X.crossWithTiny(X.getExecutionEnvironment.fromCollection(Y.toSeq))

  def merge[A] ( X: DataSet[A], Y: DataSet[A] ): DataSet[A]
    = X.getExecutionEnvironment.union(List(X,Y))

  def collect[A] ( X: DataSet[A] ): Seq[A]
    = X.collect

  def cache[A] ( X: DataSet[A] ): DataSet[A]
    = X

  // bogus; used for type-checking only
  def head[A] ( X: DataSet[A] ): A
    = X.first(1).collect.head

  def cogroupWithTinyLeft[K,A,B] ( X: Traversable[(K,A)], Y: DataSet[(K,B)] )
         (implicit kt: ClassTag[K], bt: ClassTag[B], a: TypeInformation[(K,A)],
             ea: TypeInformation[(K,Iterable[A])], eb: TypeInformation[(K,Iterable[B])],
             ek: TypeInformation[(K,(Iterable[A],Iterable[B]))]): DataSet[(K,(Iterable[A],Iterable[B]))]
    = cogroupWithTinyLeft(Y.getExecutionEnvironment.fromCollection(X.toSeq),Y)

  def cogroupWithTinyLeft[K,A,B] ( X: DataSet[(K,A)], Y: DataSet[(K,B)] )
         (implicit kt: ClassTag[K], bt: ClassTag[B],
             ea: TypeInformation[(K,Iterable[A])], eb: TypeInformation[(K,Iterable[B])],
             ek: TypeInformation[(K,(Iterable[A],Iterable[B]))]): DataSet[(K,(Iterable[A],Iterable[B]))]
    = Y.groupBy(0).reduceGroup(keyedIterable(_)).map(
        new RichMapFunction[(K,Iterable[B]),(K,(Iterable[A],Iterable[B]))]() {
           var probe: Map[K,Iterable[A]] = null
           override def open ( config: Configuration ) {
             probe = getRuntimeContext().getBroadcastVariable[(K,Iterable[A])]("left").asScala.toMap
           }
           def map ( y: (K,Iterable[B]) ): (K,(Iterable[A],Iterable[B])) = {
               probe.get(y._1) match {
                 case Some(xs) => (y._1,(xs,y._2))
                 case _ => (y._1,(Nil,y._2))
               }
           }
        }).withBroadcastSet(X.groupBy(0).reduceGroup(keyedIterable(_)),"left")

  def cogroupWithTinyRight[K,A,B] ( X: DataSet[(K,A)], Y: Traversable[(K,B)] )
         (implicit kt: ClassTag[K], bt: ClassTag[B], b: TypeInformation[(K,B)],
             ea: TypeInformation[(K,Iterable[A])], eb: TypeInformation[(K,Iterable[B])],
             ek: TypeInformation[(K,(Iterable[A],Iterable[B]))]): DataSet[(K,(Iterable[A],Iterable[B]))]            
    = cogroupWithTinyRight(X,X.getExecutionEnvironment.fromCollection(Y.toSeq))

  def cogroupWithTinyRight[K,A,B] ( X: DataSet[(K,A)], Y: DataSet[(K,B)] )
         (implicit kt: ClassTag[K], bt: ClassTag[B],
             ea: TypeInformation[(K,Iterable[A])], eb: TypeInformation[(K,Iterable[B])],
             ek: TypeInformation[(K,(Iterable[A],Iterable[B]))]): DataSet[(K,(Iterable[A],Iterable[B]))]
    = X.groupBy(0).reduceGroup(keyedIterable(_)).map(
        new RichMapFunction[(K,Iterable[A]),(K,(Iterable[A],Iterable[B]))]() {
           var probe: Map[K,Iterable[B]] = null
           override def open ( config: Configuration ) {
             probe = getRuntimeContext().getBroadcastVariable[(K,Iterable[B])]("right").asScala.toMap
           }
           def map ( x: (K,Iterable[A]) ): (K,(Iterable[A],Iterable[B])) = {
               probe.get(x._1) match {
                 case Some(ys) => (x._1,(x._2,ys))
                 case _ => (x._1,(x._2,Nil))
               }
           }
        }).withBroadcastSet(Y.groupBy(0).reduceGroup(keyedIterable(_)),"right")

  def joinWithTinyLeft[K,A,B] ( X: Traversable[(K,A)], Y: DataSet[(K,B)] )
         (implicit kt: ClassTag[K], bt: ClassTag[B],
             ea: TypeInformation[(K,A)],
             ek: TypeInformation[(K,(A,B))]): DataSet[(K,(A,B))]
    = joinWithTinyLeft(Y.getExecutionEnvironment.fromCollection(X.toSeq),Y)

  def joinWithTinyLeft[K,A,B] ( X: DataSet[(K,A)], Y: DataSet[(K,B)] )
         (implicit kt: ClassTag[K], bt: ClassTag[B],
             ek: TypeInformation[(K,(A,B))]): DataSet[(K,(A,B))]
    = Y.joinWithTiny(X).where(0).equalTo(0){ (y,x) => (y._1,(x._2,y._2)) }

  def joinWithTinyRight[K,A,B] ( X: DataSet[(K,A)], Y: Traversable[(K,B)] )
         (implicit kt: ClassTag[K], bt: ClassTag[B],
             eb: TypeInformation[(K,B)],
             ek: TypeInformation[(K,(A,B))]): DataSet[(K,(A,B))]
    = joinWithTinyRight(X,X.getExecutionEnvironment.fromCollection(Y.toSeq))

  def joinWithTinyRight[K,A,B] ( X: DataSet[(K,A)], Y: DataSet[(K,B)] )
         (implicit kt: ClassTag[K], bt: ClassTag[B],
             ek: TypeInformation[(K,(A,B))]): DataSet[(K,(A,B))]
    = X.joinWithTiny(Y).where(0).equalTo(0){ (x,y) => (y._1,(x._2,y._2)) }

  def crossWithTinyLeft[A,B] ( X: Traversable[A], Y: DataSet[B] )
         (implicit at: ClassTag[A], bt: ClassTag[B],
             ea: TypeInformation[A],
             ek: TypeInformation[(A,B)]): DataSet[(A,B)]
    = crossWithTinyLeft(Y.getExecutionEnvironment.fromCollection(X.toSeq),Y)

  def crossWithTinyLeft[A,B] ( X: DataSet[A], Y: DataSet[B] )
         (implicit at: ClassTag[A], bt: ClassTag[B],
             ek: TypeInformation[(A,B)]): DataSet[(A,B)]
    = Y.crossWithTiny(X){ (y,x) => (x,y) }

  def crossWithTinyRight[A,B] ( X: DataSet[A], Y: Traversable[B] )
         (implicit at: ClassTag[A], bt: ClassTag[B],
             eb: TypeInformation[B],
             ek: TypeInformation[(A,B)]): DataSet[(A,B)]
    = crossWithTinyRight(X,X.getExecutionEnvironment.fromCollection(Y.toSeq))

  def crossWithTinyRight[A,B] ( X: DataSet[A], Y: DataSet[B] )
         (implicit at: ClassTag[A], bt: ClassTag[B],
             ek: TypeInformation[(A,B)]): DataSet[(A,B)]
    = X.crossWithTiny(Y)

  private def occursInFunctional ( v: String, e: Expr ): Option[Expr]
    = e match {
        case flatMap(f,_)
          if occurrences(v,f) > 0
          => Some(e)
        case Var(w)
          if v == w
          => Some(e)
        case _ => AST.accumulate[Option[Expr]](e,occursInFunctional(v,_),_ orElse _,None)
      }

  def broadcastFlatMap[A,B,C] ( name: String, f: ((A,Iterable[C])) => TraversableOnce[B], S: DataSet[A], R: DataSet[C] )
        (implicit bt: ClassTag[B], eb: TypeInformation[B]): DataSet[B]
    = S.flatMap(new RichFlatMapFunction[A,B]() {
           var bR: Iterable[C] = null
           override def open ( config: Configuration ) {
             bR = getRuntimeContext().getBroadcastVariable[C](name).asScala
           }
           override def flatMap ( x: A, out: Collector[B] ) {
             for ( y <- f((x,bR)) ) out.collect(y)
           }
        }).withBroadcastSet(R,name)

  override def isDistributed ( e: Expr ): Boolean
    = e match {
        case Call("broadcastFlatMap",_) => true
        case Call("broadcastVar",List(_)) => false
        case _ => super.isDistributed(e)
      }

  /** The Flink code generator for algebraic terms */
  override def codeGen( e: Expr, env: Environment ): c.Tree = {
    e match {
      case MatchE(x,List(Case(p@VarPat(v),BoolConst(true),b)))
        if isDistributed(x) && occursInFunctional(v,b).nonEmpty
        // if x is a DataSet that occurs in a functional, it must be broadcast
        => occursInFunctional(v,b) match {
             case Some(fm@flatMap(Lambda(py,y),z))
               => val ny = Lambda(TuplePat(List(py,p)),subst(Var(v),Call("broadcastVar",List(Var(v))),y))
                  val nb = subst(fm,Call("broadcastFlatMap",List(StringConst(v),ny,z,x)),b)
                  AST.clean(ny)
                  codeGen(nb,env)
             case Some(z)
               => val xc = codeGen(x,env)
                  val tp = getType(xc,env)
                  val pc = code(p)
                  val bc = codeGen(subst(z,MethodCall(z,"collect",List()),b),add(p,tp,env))
                  q"{ val $pc = $xc; $bc }"
             case _ => null
             }
      case _ =>
    if (!isDistributed(e))
       // if e is not a DataSet operation, use the code generation for Traversable
       super.codeGen(e,env,codeGen)
    else e match {
      case repeat(Lambda(p@VarPat(v),step),init,Lambda(_,BoolConst(false)),n)
        if false        // iterations on Flink have bugs
        => val initc = codeGen(init,env)
           val tp = getType(initc,env)
           val nenv = add(p,tp,env)
           val stepc = codeGen(step,nenv)
           val pc = code(p)
           val nc = codeGen(n,env)
           val xv = TermName(c.freshName("x"))
           q"$initc.iterate($nc)( ($xv:$tp) => $xv match { case $pc => $stepc } )"
      case repeat(Lambda(p@VarPat(v),step),init,Lambda(_,reduce(m,b)),n)
        if false        // iterations on Flink have bugs
        => val initc = codeGen(init,env)
           val tp = getType(initc,env)
           val nenv = add(p,tp,env)
           val stepc = codeGen(step,nenv)
           val bc = codeGen(b,nenv)
           val fm = accumulator(m,tq"Boolean",e)  // m must be && or ||
           val pc = code(p)
           val nc = codeGen(n,env)
           val xv = TermName(c.freshName("x"))
           q"""$initc.iterateWithTermination($nc)( ($xv:$tp) => $xv match {
                    case $pc => ( $stepc, $bc.reduce($fm).filter(x => x) )
                  } )"""
      case flatMap(Lambda(TuplePat(List(k,TuplePat(List(xs,ys)))),
                          flatMap(Lambda(px,flatMap(Lambda(py,Elem(b)),ys_)),xs_)),
                   coGroup(x,y))
        if xs_ == toExpr(xs) && ys_ == toExpr(ys)
           && occurrences(patvars(xs)++patvars(ys),b) == 0
        => val (_,xtp,xc) = typedCode(x,env,codeGen)
           val (_,ytp,yc) = typedCode(y,env,codeGen)
           val kc = code(k)
           val pxc = code(px)
           val pyc = code(py)
           val bc = codeGen(b,env)
           val nv = TermName(c.freshName("x"))
           if (smallDataset(x)) {
              val j = q"core.distributed.joinWithTinyLeft($xc,$yc)"
              val tp = getType(q"core.distributed.head($j)",env)
              q"$j.map( ($nv:$tp) => $nv match { case ($kc,($pxc,$pyc)) => $bc } )"
           } else if (smallDataset(y)) {
              val j = q"core.distributed.joinWithTinyRight($xc,$yc)"
              val tp = getType(q"core.distributed.head($j)",env)
              q"$j.map( ($nv:$tp) => $nv match { case ($kc,($pxc,$pyc)) => $bc } )"
           } else {
              val tp = tq"($xtp,$ytp)"
              q"""$xc.join($yc).where(0).equalTo(0)
                     .map( ($nv:$tp) => $nv match { case (($kc,$pxc),(_,$pyc)) => $bc } )"""
           }
      case flatMap(Lambda(TuplePat(List(k,TuplePat(List(xs,ys)))),
                          flatMap(Lambda(py,flatMap(Lambda(px,Elem(b)),xs_)),ys_)),
                   coGroup(x,y))
        if xs_ == toExpr(xs) && ys_ == toExpr(ys)
           && occurrences(patvars(xs)++patvars(ys),b) == 0
        => val (_,xtp,xc) = typedCode(x,env,codeGen)
           val (_,ytp,yc) = typedCode(y,env,codeGen)
           val tp = tq"($xtp,$ytp)"
           val kc = code(k)
           val pxc = code(px)
           val pyc = code(py)
           val bc = codeGen(b,env)
           val nv = TermName(c.freshName("x"))
           if (smallDataset(x)) {
              val j = q"core.distributed.joinWithTinyLeft($xc,$yc)"
              val tp = getType(q"core.distributed.head($j)",env)
              q"$j.map( ($nv:$tp) => $nv match { case ($kc,($pxc,$pyc)) => $bc } )"
           } else if (smallDataset(y)) {
              val j = q"core.distributed.joinWithTinyRight($xc,$yc)"
              val tp = getType(q"core.distributed.head($j)",env)
              q"$j.map( ($nv:$tp) => $nv match { case ($kc,($pxc,$pyc)) => $bc } )"
           } else {
              val tp = tq"($xtp,$ytp)"
              q"""$xc.join($yc).where(0).equalTo(0)
                     .map( ($nv:$tp) => $nv match { case (($kc,$pxc),(_,$pyc)) => $bc } )"""
           }
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
           q"core.distributed.cogroupWithTinyLeft($xc,$yc).flatMap{ case $pc => $cc }"
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
           q"core.distributed.cogroupWithTinyRight($xc,$yc).flatMap{ case $pc => $cc }"
      case flatMap(Lambda(TuplePat(List(VarPat(v),_)),Elem(Var(_v))),
                   groupBy(x))
        if _v == v
        => val xc = codeGen(x,env)
           q"$xc.map(_._1).distinct()"
      case flatMap(Lambda(TuplePat(List(k,vs)),
                          Elem(Tuple(List(k_,reduce(BaseMonoid(m),vs_))))),
                   groupBy(x))
        if k_ == k && vs_ == vs
        => val xc = codeGen(x,env)
           val fm = TermName(method_name(m))
           q"$xc.groupBy(0).reduceGroup( i => core.distributed.reduceIterable(i,_ $fm _))"
      case flatMap(Lambda(p@TuplePat(List(VarPat(kp),_)),
                   MatchE(reduce(m,z@flatMap(Lambda(vars,Elem(y)),Var(_))),
                          List(Case(q,BoolConst(true),b)))),
                   gb@groupBy(x))
        => import edu.uta.diql.core.{flatMap => fm}
           // flink needs the type of groupBy key
           val (_,ktp,_) = typedCode(fm(Lambda(VarPat("v"),Elem(MethodCall(Var("v"),"_1",null))),x),env,codeGen)
           val (_,tp,_) = typedCode(fm(Lambda(p,z),gb),env,codeGen)
           val (_,ftp,fz) = typedCode(fm(Lambda(TuplePat(List(VarPat(kp),vars)),
                                                Elem(Tuple(List(Var(kp),y)))),x),env,codeGen)
           val f = accumulator(m,tp,e)
           val pc = code(TuplePat(List(VarPat(kp),q)))
           val nb = codeGen(b,env)
           val nv = TermName(c.freshName("x"))
           q"""$fz.groupBy(0)
                  .reduceGroup( i => core.distributed.reduceIterable[$ktp,$tp](i,{ case (x,y) => $f(x,y) }) )
                  .flatMap( ($nv:$ftp) => $nv match { case $pc => $nb } )"""
      case flatMap(Lambda(p,Elem(b)),x)
        if irrefutable(p)
        => val pc = code(p)
           val (_,tp,xc) = typedCode(x,env,codeGen)
           val bc = codeGen(b,add(p,tp,env))
           val nv = TermName(c.freshName("x"))
           q"$xc.map( ($nv:$tp) => $nv match { case $pc => $bc } )"
      case flatMap(Lambda(p,IfE(d,Elem(b),Empty())),x)
        if irrefutable(p)
        => val pc = code(p)
           val (_,tp,xc) = typedCode(x,env,codeGen)
           val dc = codeGen(d,add(p,tp,env))
           val bc = codeGen(b,add(p,tp,env))
           val nv = TermName(c.freshName("x"))
           if (toExpr(p) == b)
              q"$xc.filter( ($nv:$tp) => $nv match { case $pc => $dc })"
           else q"""$xc.filter( ($nv:$tp) => $nv match { case $pc => $dc } )
                       .map( ($nv:$tp) => $nv match { case $pc => $bc } )"""
      case flatMap(Lambda(p,b),x)
        => val pc = code(p)
           val (_,tp,xc) = typedCode(x,env,codeGen)
           val bc = codeGen(b,add(p,tp,env))
           val nv = TermName(c.freshName("x"))
           if (irrefutable(p))
              q"$xc.flatMap( ($nv:$tp) => $nv match { case $pc => $bc } )"
           else q"$xc.flatMap( ($nv:$tp) => $nv match { case $pc => $bc; case _ => Nil } )"
      case Call("broadcastFlatMap",List(StringConst(name),Lambda(TuplePat(List(p,py)),b),x,y))
        => val pc = code(p)
           val pyc = code(py)
           val (_,xtp,xc) = typedCode(x,env,codeGen)
           val (_,ytp,yc) = typedCode(y,env,codeGen)
           val bc = codeGen(b,add(p,xtp,add(py,tq"Iterable[$ytp]",env)))
           val nv = TermName(c.freshName("x"))
           val m = if (irrefutable(p))
                      q"($nv:($xtp,Iterable[$ytp])) => $nv match { case ($pc,$pyc) => $bc }"
                   else q"($nv:($xtp,Iterable[$ytp])) => $nv match { case ($pc,$pyc) => $bc; case _ => Nil }"
           q"core.distributed.broadcastFlatMap($name,$m,$xc,$yc)"
      case groupBy(x)
        => val xc = codeGen(x,env)
           q"$xc.groupBy(0).reduceGroup(core.distributed.keyedIterable(_))"
      case orderBy(x)
        => val xc = codeGen(x,env)
           q"core.distributed.orderBy($xc)"
      case coGroup(x,y)
        => val xc = codeGen(x,env)
           val yc = codeGen(y,env)
           //q"core.distributed.coGroup($xc,$yc)"
           q"""$xc.coGroup($yc).where(0).equalTo(0) {
                     (xs,ys) => if (!xs.hasNext) {
                          				 val (k,i) = core.distributed.keyedIterable(ys)
                          				 (k,(Nil,i))
                       				  } else {
                       				  	 val (k,i) = core.distributed.keyedIterable(xs)
                                	 (k,(i,ys.map(_._2).toIterable))
                                }
                   }"""
      case cross(x,y)
        => val xc = codeGen(x,env)
           val yc = codeGen(y,env)
           if (smallDataset(x))
              q"core.distributed.crossWithTinyLeft($xc,$yc)"
           else if (smallDataset(y))
              q"core.distributed.crossWithTinyRight($xc,$yc)"
           else q"$xc.cross($yc)"
      case reduce(BaseMonoid("+"),flatMap(Lambda(p,Elem(LongConst(1))),x))
        => val xc = codeGen(x,env)
           q"$xc.count()"
      case reduce(m,x)
        => val (_,tp,xc) = typedCode(x,env,codeGen)
           val fm = accumulator(m,tp,e)
           monoid(c,m) match {
             case Some(mc)
               => q"{ val c = $xc.reduce($fm).collect(); if (c.isEmpty) $mc else c(0) }"
             case _ => q"$xc.reduce($fm).collect()(0)"
           }
      case _ => super.codeGen(e,env,codeGen)
    } }
  }

  /** Convert DataSet method calls to algebraic terms so that they can be optimized */
  override def algebraGen ( e: Expr ): Expr = {
    import edu.uta.diql.core.{flatMap=>FlatMap,coGroup=>CoGroup,cross=>Cross}
    e match {
      case _ => apply(e,algebraGen)
    }
  }
}
