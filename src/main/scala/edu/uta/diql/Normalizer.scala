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

object Normalizer {
  import AST._

  def renameVars ( f: Lambda ): Lambda =
    f match {
      case Lambda(p,b)
        => val m = patvars(p).map((_,newvar))
           Lambda(m.foldLeft(p){ case (r,(from,to)) => subst(from,to,r) },
                  m.foldLeft(b){ case (r,(from,to)) => subst(from,Var(to),r) })
    }

  def normalize ( e: Expr ): Expr =
    e match {
      case flatMap(f,flatMap(Lambda(p,b),x))
        => normalize(flatMap(Lambda(p,flatMap(renameVars(f),b)),x))
      case flatMap(Lambda(p,b),Empty())
        => Empty()
      case flatMap(Lambda(p,b),Elem(x))
        => normalize(MatchE(x,List(Case(p,BoolConst(true),b))))
      case flatMap(f,IfE(c,e1,e2))
        => normalize(IfE(c,flatMap(f,e1),flatMap(f,e2)))
      case groupBy(Empty())
        => Empty()
      case groupBy(groupBy(x))
        => val nv = newvar
           val kv = newvar
           normalize(flatMap(Lambda(TuplePat(List(VarPat(kv),VarPat(nv))),
                                 Elem(Tuple(List(Var(kv),Elem(Var(nv)))))),
                          groupBy(x)))
      case coGroup(x,Empty())
        => val nv = newvar
           val kv = newvar
           normalize(flatMap(Lambda(TuplePat(List(VarPat(kv),VarPat(nv))),
                                 Elem(Tuple(List(Var(kv),Tuple(List(Var(nv),Empty())))))),
                          groupBy(x)))
      case coGroup(Empty(),x)
        => val nv = newvar
           val kv = newvar
           normalize(flatMap(Lambda(TuplePat(List(VarPat(kv),VarPat(nv))),
                                 Elem(Tuple(List(Var(kv),Tuple(List(Empty(),Var(nv))))))),
                          groupBy(x)))
      case IfE(BoolConst(true),e1,e2)
        => normalize(e1)
      case IfE(BoolConst(false),e1,e2)
        => normalize(e2)
      case MatchE(x,List(Case(StarPat(),BoolConst(true),y)))
        => normalize(y)
      case MethodCall(Tuple(s),a,null)
        => val pat = """_(\d+)""".r
           a match {
             case pat(x) => normalize(s(x.toInt))
             case _ => MethodCall(Tuple(s.map(normalize(_))),a,null)
           }
      case MethodCall(BoolConst(b),"&&",List(x))
        => if (b) normalize(x) else BoolConst(false)
      case MethodCall(x,"&&",List(BoolConst(b)))
        => if (b) normalize(x) else BoolConst(false)
      case MethodCall(BoolConst(b),"||",List(x))
        => if (b) BoolConst(true) else normalize(x)
      case MethodCall(x,"||",List(BoolConst(b)))
        => if (b) BoolConst(true) else normalize(x)

      case _ => apply(e,normalize(_))
    }

  def normalizeAll ( e: Expr ): Expr = {
    var olde = e
    var ne = olde
    do { olde = ne
         ne = normalize(ne)
       } while (olde != ne)
    ne
  }
}
