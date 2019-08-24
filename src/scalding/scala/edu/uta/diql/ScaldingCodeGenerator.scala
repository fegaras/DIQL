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

import com.twitter.scalding.typed._
import scala.reflect.macros.whitebox.Context


abstract class ScaldingCodeGenerator extends DistributedCodeGenerator {
  import c.universe.{Expr=>_,_}
  import AST._
  import edu.uta.diql.{LiftedResult,ResultValue}

  override def typeof ( c: Context ) = c.typeOf[TypedPipe[_]]

  override def mkType ( c: Context ) ( tp: c.Tree ): c.Tree = {
    import c.universe._
    tq"TypedPipe[$tp]"
  }

  /** Is tp a data stream? */
  override def isStream ( c: Context ) ( tp: c.Type ): Boolean
    = false // tp <:< c.typeOf[DStream[_]]

  override val datasetClassPath = "com.twitter.scalding.typed.TypedPipe"

  def debug[T] ( value: TypedPipe[LiftedResult[T]], exprs: List[String] ): TypedPipe[T]
    = value.map(List(_)).sum
           .flatMap[T](s => { new Debugger(s.toArray,exprs).debug(); Nil }) ++
      value.flatMap{ case ResultValue(v,_) => List(v); case _ => Nil }

  /** Default Scalding implementation of the algebraic operations
   *  used for type-checking in CodeGenerator.code
   */
  def flatMap[A,B] ( f: (A) => TraversableOnce[B], S: TypedPipe[A] ): TypedPipe[B]
    = S.flatMap[B](f)

  // bogus; used for type-checking only
  def flatMap2[A,B] ( f: (A) => TypedPipe[B], S: TypedPipe[A] ): TypedPipe[B]
    = S.flatMap[B](x => null)

  // bogus; used for type-checking only
  def flatMap[A,B] ( f: (A) => TypedPipe[B], S: Traversable[A] ): TypedPipe[B]
    = f(S.head)

  def groupBy[K,A] ( S: TypedPipe[(K,A)] )
          (implicit ord: Ordering[K]): TypedPipe[(K,Iterable[A])]
    = S.group.toList.toTypedPipe

  def orderBy[K,A] ( S: TypedPipe[(K,A)] )
          ( implicit ord: Ordering[K] ): TypedPipe[A]
    = S.groupBy(_._1).sortBy(_._1).values.map(_._2)

  def reduce[A] ( acc: (A,A) => A, S: TypedPipe[A] ): A
//    = collect(S.groupAll.reduce(acc).values).map(_.head)
    = null.asInstanceOf[A]

  def coGroup[K,A,B] ( X: TypedPipe[(K,A)], Y: TypedPipe[(K,B)] )
          ( implicit ord: Ordering[K] ): TypedPipe[(K,(Iterable[A],Iterable[B]))]
    = X.group.cogroup(Y.group){ case (k,ix,iy) => Iterator((ix.toIterable,iy.toIterable)) }.toTypedPipe

  def coGroup[K,A,B] ( X: Traversable[(K,A)], Y: TypedPipe[(K,B)] )
          ( implicit ord: Ordering[K] ): TypedPipe[(K,(Iterable[A],Iterable[B]))]
    = coGroup(Y,TypedPipe.from(X.toIterable)).map{ case (k,(y,x)) => (k,(x,y)) }

  def coGroup[K,A,B] ( X: TypedPipe[(K,A)], Y: Traversable[(K,B)] )
          ( implicit ord: Ordering[K] ): TypedPipe[(K,(Iterable[A],Iterable[B]))]
    = coGroup(X,TypedPipe.from(Y.toIterable))

  def cross[A,B] ( X: TypedPipe[A], Y: TypedPipe[B] ): TypedPipe[(A,B)]
    = X.cross(Y)

  def cross[A,B] ( X: Traversable[A], Y: TypedPipe[B] ): TypedPipe[(A,B)]
    = Y.cross(TypedPipe.from(X.toIterable)).map{ case (y,x) => (x,y) }

  def cross[A,B] ( X: TypedPipe[A], Y: Traversable[B] ): TypedPipe[(A,B)]
    = X.cross(TypedPipe.from(Y.toIterable))

  def merge[A] ( X: TypedPipe[A], Y: TypedPipe[A] ): TypedPipe[A]
    = X++Y

  def collect[A] ( X: TypedPipe[A] ): ValuePipe[List[A]]
    = X.map(List(_)).sum

  def cache[A] ( X: TypedPipe[A] ): TypedPipe[A]
    = X.fork

  // bogus; used for type-checking only
  def head[A] ( X: TypedPipe[A] ): A
    = null.asInstanceOf[A]

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

  private def valuePipeInFunctional ( e: Expr ): List[Expr]
    = e match {
        case reduce(m,x)
          if isDistributed(x)
          => e::valuePipeInFunctional(x)
        case Call("broadcast",_)
          => List(e)
        case _ => AST.accumulate[List[Expr]](e,valuePipeInFunctional(_),_++_,Nil)
      }

  override def isDistributed ( e: Expr ): Boolean
    = e match {
        case Call("broadcast",_) => true
        case _ => super.isDistributed(e)
      }

  override def repeatStepCoercion ( itp: c.Tree, stp: c.Tree, e: c.Tree ): c.Tree = {
    (itp,stp) match {
      case (tq"(..$its)",tq"(..$sts)") if its.length > 1
        => val s = (its zip sts zip (1 to its.length)).map{ case ((it,st),i)
                        => val n = TermName("_"+i)
                           repeatStepCoercion(it,st,q"$e.$n") }
           q"(..$s)"
      case _
        => val it = c.Expr[Any](c.typecheck(itp,c.TYPEmode)).actualType
           val st = c.Expr[Any](c.typecheck(stp,c.TYPEmode)).actualType
           if (it <:< distributed.typeof(c))
              q"$e.fork"
           else if (it <:< typeOf[Traversable[_]] || it <:< typeOf[Array[_]])
             if (st <:< typeOf[Traversable[_]] || st <:< typeOf[Array[_]])
                q"$e.toArray"
             else q"$e.map(List(_)).sum.map(_.toArray)"
           else e
    }
  }

  def iCodeGen ( e: Expr, env: Environment ): c.Tree = {
    val ec = codeGen(e,env)
    if (isDistributed(e)) ec
    else q"TypedPipe.from($ec.toIterable)"
  }

  /** The Scalding code generator for algebraic terms */
  override def codeGen ( e: Expr, env: Environment ): c.Tree = {
    e match {
     case MatchE(x,List(Case(p@VarPat(v),BoolConst(true),b)))
        if isDistributed(x) && occursInFunctional(v,b).nonEmpty
        // if x is a TypedPipe that occurs in a functional, it must be broadcast
        => occursInFunctional(v,b) match {
             case Some(fm)
               => codeGen(subst(Var(v),Call("broadcast",List(x)),b),env)
             case _ => null
             }
      case Call("avg_value",List(x))
        => val xc = codeGen(x,env)
           val tp = getType(xc,env)
           tp match {
                case tq"edu.uta.diql.Avg[$t]"
                  => q"$xc.value"
                case _   // Scalding ValuePipe
                  => q"$xc.map(_.value)"
           }
      case _ =>
    if (!isDistributed(e))
       // if e is not an TypedPipe operation, use the code generation for Traversable
       super.codeGen(e,env,codeGen(_,_))
    else e match {
      case flatMap(Lambda(p,b),x)
        if valuePipeInFunctional(b).nonEmpty
        => val reds = valuePipeInFunctional(b)
           val vars = reds.map(_ => newvar)
           val nb = reds.zip(vars).foldRight[Expr](b) {
                        case ((red,v),r) => subst(red,MethodCall(Var(v),"head",null),r)
                    }
           val pc = code(p)
           val (_,tp,xc) = typedCode(x,env,codeGen)
           val nenv = reds.zip(vars).foldRight[Environment](add(p,tp,env)) {
                         case ((red,v),r)
                           => val tq"$t[$etp]" = getType(codeGen(red,env),env)
                              add(VarPat(v),tq"Option[$etp]",r)
                      }
           val rc = if (reds.length == 1)
                       codeGen(reds(0),env)
                    else q"(..${reds.map(codeGen(_,env))})"
           val prc = if (vars.length == 1)
                        code(VarPat(vars(0)))
                     else code(TuplePat(vars.map(VarPat(_))))
           nb match {
              case Elem(be)
                => val bc = codeGen(be,nenv)
                   q"$xc.mapWithValue($rc){ case ($pc,$prc) => $bc }"
              case _
                => val bc = codeGen(nb,nenv)
                   q"$xc.flatMapWithValue($rc){ case ($pc,$prc) => $bc }"
           }
      case flatMap(Lambda(TuplePat(List(k,TuplePat(List(xs,ys)))),
                          flatMap(Lambda(px,flatMap(Lambda(py,Elem(b)),ys_)),xs_)),
                   coGroup(x,y))
        if xs_ == toExpr(xs) && ys_ == toExpr(ys)
           && occurrences(patvars(xs)++patvars(ys),b) == 0
        => val xc = iCodeGen(x,env)
           val yc = iCodeGen(y,env)
           val kc = code(k)
           val pxc = code(px)
           val pyc = code(py)
           val bc = codeGen(b,env)
           if (smallDataset(x))
              q"$yc.join($xc).toTypedPipe.map{ case ($kc,($pyc,$pxc)) => $bc}"
           else q"$xc.join($yc).toTypedPipe.map{ case ($kc,($pxc,$pyc)) => $bc}"
      case flatMap(Lambda(TuplePat(List(k,TuplePat(List(xs,ys)))),
                          flatMap(Lambda(py,flatMap(Lambda(px,Elem(b)),xs_)),ys_)),
                   coGroup(x,y))
        if xs_ == toExpr(xs) && ys_ == toExpr(ys)
           && occurrences(patvars(xs)++patvars(ys),b) == 0
        => val xc = iCodeGen(x,env)
           val yc = iCodeGen(y,env)
           val kc = code(k)
           val pxc = code(px)
           val pyc = code(py)
           val bc = codeGen(b,env)
           if (smallDataset(x))
              q"$yc.join($xc).toTypedPipe.map{ case ($kc,($pyc,$pxc)) => $bc}"
           else q"$xc.join($yc).toTypedPipe.map{ case ($kc,($pxc,$pyc)) => $bc}"
      case flatMap(Lambda(TuplePat(List(VarPat(v),_)),Elem(Var(_v))),
                   groupBy(x))
        if _v == v
        => val xc = codeGen(x,env)
           q"$xc.group.sum.keys"
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
           q"$xc.group.toList.toTypedPipe"
      case orderBy(x)
        => val xc = codeGen(x,env)
           q"$xc.groupBy(_._1).sortBy(_._1).values.map(_._2)"
      case coGroup(x,y)
        => val xc = iCodeGen(x,env)
           val yc = iCodeGen(y,env)
           q"$xc.group.cogroup($yc.group){ case (k,ix,iy) => Iterator((ix.toIterable,iy.toIterable)) }.toTypedPipe"
      case cross(x,y)
        => val xc = iCodeGen(x,env)
           val yc = iCodeGen(y,env)
           if (smallDataset(y))
              q"$yc.cross($xc).map{ case (y,x) => (x,y) }"
           else q"$xc.cross($yc)"
      case reduce(BaseMonoid("+"),x)
        => val xc = codeGen(x,env)
           q"$xc.sum"
      case reduce(m,x)
        => val (_,tp,xc) = typedCode(x,env,codeGen(_,_))
           val fm = accumulator(m,tp,e)
           q"$xc.groupAll.reduce($fm).values.map(List(_)).sum.map(_.head)"
      case MethodCall(x,"++",List(y))
        => val xc = iCodeGen(x,env)
           val yc = iCodeGen(y,env)
           q"$xc ++ $yc"
      case Merge(x,y)
        => val xc = iCodeGen(x,env)
           val yc = iCodeGen(y,env)
           q"$xc ++ $yc"
      case Call("broadcast",List(x@reduce(_,_)))
        => codeGen(x,env)
      case Call("broadcast",List(x))
        => val xc = codeGen(x,env)
           q"$xc.map(List(_)).sum"
      case _ => super.codeGen(e,env,codeGen(_,_))
    } }
  }

  /** Convert TypedPipe method calls to algebraic terms so that they can be optimized */
  override def algebraGen ( e: Expr ): Expr = {
    import edu.uta.diql.core.{flatMap=>FlatMap,coGroup=>CoGroup,cross=>Cross}
    e match {
      case _ => apply(e,algebraGen(_))
    }
  }
}
