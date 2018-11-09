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

import edu.uta.diql.core.Parser.parse

import scala.collection.immutable.HashMap
import scala.collection.mutable
// @formatter:off

abstract class Streaming extends CodeGeneration {

  import AST._

  type Env = Map[String,Monoid]

  val repeatEnv = new mutable.HashMap[String,Monoid]

  def apply ( f: Lambda, e: Expr ): Expr
    = (f, e) match {
      case (Lambda(TuplePat(List(p1,p2)),b),Tuple(List(e1,e2)))
        => apply(Lambda(p2,apply(Lambda(p1,b),e1)),e2)
      case (Lambda(VarPat(v),b),_)
        => subst(v,e,b)
      case _ => e
    }

  def bindPat ( p: Pattern, m: Monoid ): List[(String,Monoid)]
    = (p,m) match {
        case (VarPat(v),n)
          => List((v,n))
        case (TuplePat(pl),ProductMonoid(ml))
          => (pl zip ml).flatMap{ case (pp,mm) => bindPat(pp,mm) }
        case _
          => accumulatePat[List[(String,Monoid)]](p,bindPat(_,m),_++_,Nil)
  }

  def inference ( e: Expr, env: Env ): Option[Monoid]
    = e match {
        case groupBy(x)
          => inference(x,env) match {
                case m@Some(FixedMonoid())
                  => m
                case Some(m)
                  => Some(ParametricMonoid("groupBy",m))
                case _ => None
             }
        case orderBy(x)
          => inference(x,env) match {
                case m@Some(FixedMonoid())
                  => m
                case Some(m)
                  => Some(ParametricMonoid("orderBy",m))
                case _ => None
             }
        case coGroup(x,y)
          => for {
                mx <- inference(x,env)
                my <- inference(y,env)
             } yield if (mx.isInstanceOf[FixedMonoid] && my.isInstanceOf[FixedMonoid])
                        mx
                      else ParametricMonoid("groupBy",ProductMonoid(List(mx,my)))
        case flatMap(Lambda(VarPat(v),Elem(Var(w))),x)
          if v == w
          => inference(x,env)
        case flatMap(Lambda(p,Elem(Tuple(List(k,u)))),x)
          => inference(x,env) match {
                case Some(ParametricMonoid("groupBy",m))
                  => for {
                        nm <- inference(k,env++bindPat(p,ProductMonoid(List(FixedMonoid(),m))))
                     } yield ParametricMonoid("groupBy",nm)
                case Some(ParametricMonoid("orderBy",m))
                  => for {
                        nm <- inference(k,env++bindPat(p,ProductMonoid(List(FixedMonoid(),m))))
                     } yield ParametricMonoid("orderBy",nm)
                case _ => None
             }
        case flatMap(Lambda(p,b),x)
          => inference(x,env) match {
                case nm@Some(BaseMonoid("bag"))
                  => inference(b,env++bindPat(p,FixedMonoid())) match {
                        case gm@Some(ParametricMonoid("groupBy",_))
                          => gm
                        case gm@Some(ParametricMonoid("orderBy",_))
                          => gm
                        case _ => nm
                     }
                case Some(ParametricMonoid("groupBy",m))
                  => inference(b,env++bindPat(p,ProductMonoid(List(FixedMonoid(),m)))) match {
                        case nm@Some(ParametricMonoid("groupBy",_))
                          => nm
                        case nm@Some(BaseMonoid("bag"))
                          => nm
                        case nm@Some(FixedMonoid())
                          => nm
                        case _ => None
                     }
                case Some(ParametricMonoid("orderBy",m))
                  => inference(b,env++bindPat(p,ProductMonoid(List(FixedMonoid(),m)))) match {
                        case nm@Some(ParametricMonoid("orderBy",_))
                          => nm
                        case nm@Some(BaseMonoid("bag"))
                          => nm
                        case nm@Some(FixedMonoid())
                          => nm
                        case _ => None
                     }
                case Some(FixedMonoid())
                  => inference(b,env++bindPat(p,FixedMonoid())) match {
                        case nm@Some(ParametricMonoid("groupBy",_))
                          => nm
                        case nm@Some(ParametricMonoid("orderBy",_))
                          => nm
                        case nm@Some(BaseMonoid("bag"))
                          => nm
                        case nm@Some(FixedMonoid())
                          => nm
                        case _ => None
                     }
                case _ => None
             }
        case reduce(m@ParametricMonoid("groupBy",BaseMonoid("count")),u)
          => inference(u,env) match {
                case Some(_)
                  => Some(m)
                case _ => None
             }
        case reduce(m@ParametricMonoid("groupBy",_),u)
          => inference(u,env) match {
                case Some(ParametricMonoid("groupBy",BaseMonoid("bag")))
                  => Some(m)
                case _ => None
             }
        case reduce(m@BaseMonoid("count"),u)
          => inference(u,env) match {
                case Some(_)
                  => Some(m)
                case _ => None
             }
        case reduce(m,u)
          => inference(u,env) match {
                case Some(BaseMonoid("bag"))
                  => Some(m)
                case n@Some(FixedMonoid())
                  => n
                case _ => None
             }
        case Tuple(l)
          => val s = l.map{ x => inference(x,env) }.foldRight[Option[List[Monoid]]](Some(Nil)){
                  case (Some(m),Some(s))
                    => Some(m::s)
                  case _ => return None
             }
             if (s.exists(_.forall(_.isInstanceOf[FixedMonoid])))
                Some(FixedMonoid())
             else  s.map(ProductMonoid)
        case Var(v)
          => Some(if (env.contains(v)) env(v) else FixedMonoid())
        case IntConst(_)
          => Some(FixedMonoid())
        case LongConst(_)
          => Some(FixedMonoid())
        case DoubleConst(_)
          => Some(FixedMonoid())
        case StringConst(_)
          => Some(FixedMonoid())
        case CharConst(_)
          => Some(FixedMonoid())
        case BoolConst(_)
          => Some(FixedMonoid())
        case _ if streamSource(e)
          => Some(BaseMonoid("bag"))
        case _ if isDistributed(e)
          => Some(FixedMonoid())
        case _ => accumulate[Option[Monoid]]( e, inference(_,env),
                        { case ( r@Some(FixedMonoid()), Some(FixedMonoid()) )
                            => r
                          case _ => None
                        },
                      Some(FixedMonoid()) )
      }

  def inference ( e: Expr ): Option[Monoid] = inference(e,new HashMap[String,Monoid]())

  def sMap1 ( f: Lambda, X: Expr ): Expr = {
    val th = newvar
    val k = newvar
    val a = newvar
    val b = newvar
    flatMap(Lambda(TuplePat(List(TuplePat(List(VarPat(th),VarPat(k))),VarPat(a))),
                   flatMap(Lambda(VarPat(b),
                                  Elem(Tuple(List(Tuple(List(Var(th),Var(k))),Var(b))))),
                           apply(f,Tuple(List(Var(k),Var(a)))))),
            X)
  }

  def sMap2 ( f: Lambda, X: Expr ): Expr = {
    val th = newvar
    val k = newvar
    val nk = newvar
    val a = newvar
    val b = newvar
    flatMap(Lambda(TuplePat(List(TuplePat(List(VarPat(k),VarPat(th))),VarPat(a))),
                   flatMap(Lambda(TuplePat(List(VarPat(nk),VarPat(b))),
                                  Elem(Tuple(List(Var(nk),Tuple(List(Tuple(List(Var(k),Var(th))),Var(b))))))),
                           apply(f,Tuple(List(Var(k),Var(a)))))),
            X)
  }

  def sMap3 ( f: Lambda, X: Expr ): Expr = {
      val k = newvar
      val a = newvar
      val b = newvar
      flatMap(Lambda(VarPat(a),
                     flatMap(Lambda(TuplePat(List(VarPat(k),VarPat(b))),
                                    Elem(Tuple(List(Var(k),Tuple(List(Tuple(Nil),Var(b))))))),
                             apply(f,Var(a)))),
              X)
  }

  def swap ( X: Expr ): Expr = {
    val th = newvar
    val k = newvar
    val v = newvar
    flatMap(Lambda(TuplePat(List(VarPat(k),TuplePat(List(VarPat(th),VarPat(v))))),
                   Elem(Tuple(List(Tuple(List(Var(k),Var(th))),Var(v))))),
            X)
  }

  def mix ( X: Expr ): Expr = {
    val thx = newvar
    val thy = newvar
    val k = newvar
    val s1 = newvar
    val s2 = newvar
    val xs = newvar
    val ys = newvar
    flatMap(Lambda(TuplePat(List(VarPat(k),TuplePat(List(VarPat(s1),VarPat(s2))))),
                   flatMap(Lambda(TuplePat(List(VarPat(thx),VarPat(xs))),
                                  flatMap(Lambda(TuplePat(List(VarPat(thy),VarPat(ys))),
                                                 Tuple(List(Tuple(List(Var(k),Tuple(List(Var(thx),Var(thy))))),
                                                            Tuple(List(Var(s1),Var(s2)))))),
                                          groupBy(Var(s2)))),
                           groupBy(Var(s1)))),
            X)
  }

  def streamSource ( e: Expr ): Boolean
    = distributed.isStream(c)(c.Expr[Any](typecheck(e)).actualType)

  def repeatVar ( e: Expr ): Boolean = true

  def injectQ ( e: Expr ): Expr
    = e match {
        case reduce(m,flatMap(f,s))
          if streamSource(s) || repeatVar(s)
          => Elem(Tuple(List(Tuple(Nil),e)))
        case reduce(m,flatMap(f,s))
          => reduce(ParametricMonoid("groupBy",m),sMap1(f,injectE(s)))
        case reduce(m,u)
          if streamSource(u) || repeatVar(u)
          => Elem(Tuple(List(Tuple(Nil),e)))
        case reduce(_,_)
          => Elem(Tuple(List(Tuple(Nil),e)))
        case flatMap(f,u)
          if streamSource(u) || repeatVar(u)
          => val a = newvar
             val b = newvar
             flatMap(Lambda(VarPat(a),flatMap(Lambda(VarPat(b),
                                                     Elem(Tuple(List(Tuple(Nil),Var(b))))),
                                              apply(f,Var(a)))),
                     u)
        case flatMap(f,u)
          => sMap1(f,injectE(u))
        case _ => Elem(Tuple(List(Tuple(Nil),e)))
      }

  def injectE ( e: Expr ): Expr
    = e match {
        case groupBy(u)
          => groupBy(swap(injectC(u)))
        case orderBy(u)
          => orderBy(swap(injectC(u)))
        case coGroup(c1@flatMap(_,u),c2@flatMap(_,s))
          if streamSource(u) && streamSource(s)
          => val k = newvar
             val v = newvar
             flatMap(Lambda(TuplePat(List(VarPat(k),VarPat(v))),
                            Elem(Tuple(List(Tuple(List(Var(k),Tuple(List(Tuple(Nil),Tuple(Nil))))),Var(v))))),
                     coGroup(c1,c2))
         case coGroup(c1@flatMap(_,u),c2)
          if streamSource(u)
          => val k = newvar
             val kk = newvar
             val xs = newvar
             val ys = newvar
             val v = newvar
             flatMap(Lambda(TuplePat(List(VarPat(k),TuplePat(List(VarPat(xs),VarPat(ys))))),
                            flatMap(Lambda(TuplePat(List(VarPat(kk),VarPat(v))),
                                           Elem(Tuple(List(Tuple(List(Var(k),Tuple(List(Tuple(Nil),Var(kk))))),
                                                           Tuple(List(Var(xs),Var(v))))))),
                                    groupBy(Var(ys)))),
                     coGroup(c1,injectC(c2)))
         case coGroup(c1,c2@flatMap(_,u))
          if streamSource(u)
          => val k = newvar
             val kk = newvar
             val xs = newvar
             val ys = newvar
             val v = newvar
             flatMap(Lambda(TuplePat(List(VarPat(k),TuplePat(List(VarPat(xs),VarPat(ys))))),
                            flatMap(Lambda(TuplePat(List(VarPat(kk),VarPat(v))),
                                           Elem(Tuple(List(Tuple(List(Var(k),Tuple(List(Var(kk),Tuple(Nil))))),
                                                           Tuple(List(Var(v),Var(ys))))))),
                                    groupBy(Var(xs)))),
                     coGroup(injectC(c1),c2))
        case coGroup(c1,c2)
          => mix(coGroup(injectC(c1),injectC(c2)))
        case _ => injectC(e)
  }

  def injectC ( e: Expr ): Expr
    = e match {
        case flatMap(f,s)
          if streamSource(s) || repeatVar(s)
          => sMap3(f,s)
        case flatMap(f,u)
          => sMap2(f,injectE(u))
        case _
          if streamSource(e) || repeatVar(e)
          => sMap3(Lambda(VarPat("x"),Elem(Var("x"))),e)
        case _ => e
  }

  def findHomomorphisms ( e: Expr, env: Env ): List[Expr]
    = inference(e,env) match {
        case None
          => List(e)
        case Some(FixedMonoid())
          => List(e)
        case _
          => e match {
              case flatMap(Lambda(p,Elem(b)),x)
                => inference(x,env) match {
                      case Some(BaseMonoid("bag"))
                        => List(e)
                      case Some(ParametricMonoid("groupBy",m))
                        => inference(b,env++bindPat(p,m)) match {
                              case Some(ProductMonoid(_))
                                => List(e)
                              case _
                                => accumulate[List[Expr]](e,findHomomorphisms(_,env),_++_,Nil)
                           }
                      case Some(ParametricMonoid("orderBy",m))
                        => inference(b,env++bindPat(p,m)) match {
                              case Some(ProductMonoid(_))
                                => List(e)
                              case _
                                => accumulate[List[Expr]](e,findHomomorphisms(_,env),_++_,Nil)
                           }
                      case Some(fm@FixedMonoid())
                        => inference(b,env++bindPat(p,fm)) match {
                              case Some(FixedMonoid()) => List(e)
                              case Some(BaseMonoid("bag")) => List(e)
                              case _
                                => accumulate[List[Expr]](e,findHomomorphisms(_,env),_++_,Nil)
                            }
                      case _ => accumulate[List[Expr]](e,findHomomorphisms(_,env),_++_,Nil)
                   }
              case _ => accumulate[List[Expr]](e,findHomomorphisms(_,env),_++_,Nil)
             }
     }

  def split ( e: Expr, pat: Pattern, env: Env ): (Expr,Expr)
    = e match {
        case flatMap(Lambda(p,Elem(Tuple(List(Var(k),b)))),u)
          => var nenv = env
             val kv = newvar
             val bv = newvar
             inference(u,env) match {
                case Some(bm@BaseMonoid("bag"))
                  => nenv = nenv++bindPat(p,bm)
                case Some(ParametricMonoid("groupBy",m))
                  => nenv = nenv++bindPat(p,ProductMonoid(List(FixedMonoid(),m)))
                case Some(ParametricMonoid("orderBy",m))
                  => nenv = nenv++bindPat(p,ProductMonoid(List(FixedMonoid(),m)))
                case _ =>;
             }
             val hs = findHomomorphisms(b,nenv)
             hs match {
               case List(bb) if b == bb
                 => (toExpr(pat),e)
               case _
                 => val ne = b
                    (flatMap(Lambda(TuplePat(List(VarPat(kv),VarPat(bv))),
                                    Elem(Tuple(List(Var(kv),ne)))),toExpr(pat)),
                     flatMap(Lambda(p,Elem(Tuple(List(Var(k),Tuple(hs))))),u))
             }
        case flatMap(Lambda(p,b),u)
          => inference(u,env) match {
                case Some(ParametricMonoid(gb,m))
                  if List("groupBy","orderBy").contains(gb)
                  => val nenv = env++bindPat(p,ProductMonoid(List(FixedMonoid(),m)))
                     inference(b,nenv) match {
                        case Some(ParametricMonoid("groupBy",_))
                          => (toExpr(p),e)
                        case Some(ParametricMonoid("orderBy",_))
                          => (toExpr(p),e)
                        case _
                          => val nv = newvar
                             val (a,h) = split(b,p,nenv)
                             if (occurrences(patvars(p),a) == 0)
                                ( a, flatMap(Lambda(p,h),u) )
                             else {
                               val nv = newvar
                               ( flatMap(Lambda(TuplePat(List(p,pat)),a),toExpr(pat)),
                                 flatMap(Lambda(p,flatMap(Lambda(VarPat(nv),
                                                                 Elem(Tuple(List(toExpr(p),Var(nv))))),
                                                          h)),
                                         u) )
                             }
                     }
                case Some(m@FixedMonoid())
                  => val (a,h) = split(b,pat,env++bindPat(p,m))
                     if (occurrences(patvars(p),a) == 0)
                        ( a, flatMap(Lambda(p,h),u) )
                     else {
                        val nv = newvar
                        ( flatMap(Lambda(TuplePat(List(p,pat)),a),toExpr(pat)),
                           flatMap(Lambda(p,flatMap(Lambda(VarPat(nv),Elem(Tuple(List(toExpr(p),Var(nv))))),
                                                           h)),
                                          u) )
                     }
                case Some(m@BaseMonoid("union"))
                  => val (a,h) = split(b,pat,env++bindPat(p,m))
                     if (occurrences(patvars(p),a) == 0)
                        ( a, flatMap(Lambda(p,h),u) )
                     else {
                        val nv = newvar
                        ( flatMap(Lambda(TuplePat(List(p,pat)),a),toExpr(pat)),
                           flatMap(Lambda(p,flatMap(Lambda(VarPat(nv),Elem(Tuple(List(toExpr(p),Var(nv))))),
                                                           h)),
                                          u) )
                     }
                case m => throw new Error("Failed split: "+m)
          }
        case reduce(m,u)
          => inference(u,env) match {
                case Some(BaseMonoid("union"))
                  => (toExpr(pat),e)
                case _
                  => val (b,h) = split(u,pat,env)
                     ( reduce(m,b), h )
             }
        case Tuple(ts)
          => pat match {
              case TuplePat(ps)
                => val s = for {
                              (t,p) <- ts zip ps
                           } yield split(t,p,env)
                ( Tuple(s.map(_._1)), Tuple(s.map(_._2)) )
              case _ => throw new Error("Need a tuple pattern: "+pat)
             }
        case Call(f,as)
          => val ps = as.map(a => newvar)
             val s = for {
                       (a,p) <- as zip ps
                     } yield split(a,VarPat(p),env)
             ( apply(Lambda(TuplePat(ps.map(VarPat)),Call(f,s.map(_._1))),toExpr(pat)),
               Tuple(s.map(_._2)) )
        case _
          if streamSource(e)
          => (toExpr(pat),e)
        case _
          => (e,e)
  }
}
