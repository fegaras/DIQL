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
 // @formatter:off

abstract class Streaming extends CodeGeneration {

  import AST._

  def apply ( f: Lambda, e: Expr ): Expr
    = (f, e) match {
      case (Lambda(TuplePat(List(p1,p2)),b),Tuple(List(e1,e2)))
        => apply(Lambda(p2,apply(Lambda(p1,b),e1)),e2)
      case (Lambda(VarPat(v),b),_)
        => subst(v,e,b)
      case _ => e
  }

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

  def streamSource ( e: Expr ): Boolean = true

  def injectQ ( e: Expr ): Expr
    = e match {
        case reduce(m,flatMap(f,s))
          if streamSource(s)
          => Elem(Tuple(List(Tuple(Nil),e)))
        case reduce(m,flatMap(f,s))
          => reduce("groupBy("+m+")",sMap1(f,injectE(s)))
        case reduce(_,_)
          => Elem(Tuple(List(Tuple(Nil),e)))
  }

  def injectE ( e: Expr ): Expr
    = e match {
        case groupBy(c)
          => groupBy(swap(injectC(c)))
        case orderBy(c)
          => orderBy(swap(injectC(c)))
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
    = e
}
