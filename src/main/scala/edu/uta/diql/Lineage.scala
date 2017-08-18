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


object Provenance {
  import AST._

  /** The nodes of the query AST */
  var exprs: List[String] = Nil

  // set it to true for fine-grained provenance
  var fine_grain = false

  /** map {((k,v),p)} to {(k,(v,p))} */
  private def flip ( e: Expr ): Expr =
    flatMap(Lambda(TuplePat(List(TuplePat(List(VarPat("k"),VarPat("v"))),VarPat("p"))),
                   Elem(Tuple(List(Var("k"),Tuple(List(Var("v"),Var("p"))))))),
            e)

  /** map {(v,p)} to {v} */
  private def first ( e: Expr ): Expr =
    flatMap(Lambda(TuplePat(List(VarPat("v"),VarPat("p"))),
                   Elem(Var("v"))),
            e)

  /** map {(v,p)} to {p} */
  private def second ( e: Expr ): Expr =
    flatMap(Lambda(TuplePat(List(VarPat("v"),VarPat("p"))),
                   Elem(Var("p"))),
            e)

  private def exprNode ( e: Expr ): String
    = e match {
        case flatMap(_,_) => "flatMap"
        case groupBy(_) => "groupBy"
        case coGroup(_,_) => "coGroup"
        case cross(_,_) => "cross"
        case reduce(_,_) => "reduce"
        case repeat(_,_,_,_) => "repeat"
        case _ => "input"
      }

  /** Construct a provenance tuple
   * @param expr the AST that corresponds to this value
   * @param value the value
   * @param provenance the input provenance of this value
   * @return a provenance tuple
   */
  private def prov ( expr: Expr, value: Expr, provenance: Expr ): Expr = {
    val loc = exprs.length
    exprs = exprs:+exprNode(expr)
    value match {
      case Var(_)
        => Tuple(List(value,
                      Constructor("UnaryLineage",
                                  List(IntConst(loc),value,provenance))))
      case _
        => MatchE(value,
                  List(Case(VarPat("v"),BoolConst(true),
                            Tuple(List(Var("v"),
                                       Constructor("UnaryLineage",
                                                   List(IntConst(loc),Var("v"),provenance)))))))
    }
  }

  private def prov1 ( expr: Expr, value: Expr, provenance: Expr ): Expr
    = prov(expr,value,Call("List",List(provenance)))

  private def prov ( expr: Expr, value: Expr, left: Expr, right: Expr ): Expr = {
    exprs = exprs:+exprNode(expr)
    val loc = exprs.length-1
    value match {
      case Var(_)
        => Tuple(List(value,
                      Constructor("BinaryLineage",
                                  List(IntConst(loc),value,left,right))))
      case _
        => MatchE(value,
                  List(Case(VarPat("v"),BoolConst(true),
                            Tuple(List(Var("v"),
                                       Constructor("BinaryLineage",
                                                   List(IntConst(loc),Var("v"),left,right)))))))
    }
  }

  private def lift_var ( v: Expr, nv: Expr, fv: Expr, e: Expr ): Expr
    = e match {
      case flatMap(Lambda(w,b),x)
        if !fine_grain && !contains_trace(b)
        => // don't lift the flatMap function in coarse-grained provenance
           flatMap(Lambda(w,subst(v,fv,b)),lift_var(v,nv,fv,x))
      case Var(w) if w==v => nv
      case _ => apply(e,lift_var(v,nv,fv,_))
    }

  private def contains_trace ( e: Expr ): Boolean
    = e match {
      case Call("trace",List(msg,x)) => true
      case _ => accumulate[Boolean](e,contains_trace(_),_||_,false)
    }

  /** Lift the expression e of type {t} to {(t,provenance)} */
  def embed ( e: Expr ): Expr
    = e match {
      case repeat(Lambda(p,u),x,Lambda(_,c),n)
        => repeat(Lambda(p,embed(u)),x,Lambda(p,embed(c)),n)
      case flatMap(Lambda(p,b),x)
        if !fine_grain && !contains_trace(b)
        // coarse grain
        => val q = newvar
           flatMap(Lambda(TuplePat(List(p,VarPat(q))),
                          flatMap(Lambda(VarPat("v"),
                                         Elem(prov1(e,Var("v"),Var(q)))),
                                  b)),
                   embed(x))
      case flatMap(Lambda(p,b),x)
        // fine grain
        => val q = newvar
           flatMap(Lambda(p,
                          flatMap(Lambda(TuplePat(List(VarPat("v"),VarPat(q))),
                                         Elem(prov1(e,Var("v"),Var(q)))),
                                  b)),
                   embed(x))
      case groupBy(x)
        => flatMap(Lambda(TuplePat(List(VarPat("k"),VarPat("s"))),
                          Elem(prov(e,Tuple(List(Var("k"),first(Var("s")))),second(Var("s"))))),
                   groupBy(flip(embed(x))))
      case orderBy(x)
        => flatMap(Lambda(TuplePat(List(VarPat("k"),VarPat("s"))),
                          Elem(prov(e,Tuple(List(Var("k"),first(Var("s")))),second(Var("s"))))),
                   orderBy(flip(embed(x))))
      case coGroup(x,y)
        => flatMap(Lambda(TuplePat(List(VarPat("k"),TuplePat(List(VarPat("xs"),VarPat("ys"))))),
                          Elem(prov(e,Tuple(List(Var("k"),Tuple(List(first(Var("xs")),first(Var("ys")))))),
                                    second(Var("xs")),second(Var("ys"))))),
                   coGroup(flip(embed(x)),flip(embed(y))))
      case cross(x,y)
        => flatMap(Lambda(TuplePat(List(TuplePat(List(VarPat("x"),VarPat("px"))),
                                        TuplePat(List(VarPat("y"),VarPat("py"))))),
                          Elem(Tuple(List(Tuple(List(Var("x"),Var("y"))),
                                          prov(e,Tuple(List(Var("x"),Var("y"))),
                                               Var("px"),Var("py")))))),
                   cross(embed(x),embed(y)))
      case reduce(m,x)
        => val nv = newvar
           MatchE(embed(x),
                  List(Case(VarPat(nv),BoolConst(true),
                            prov(e,reduce(m,first(Var(nv))),second(Var(nv))))))
      case Tuple(s)
        => val ns = s.map(embed(_))
           val vs = s.map(_ => (newvar,newvar))
           MatchE(Tuple(ns),
                  List(Case(TuplePat(vs.map{ case (v,p) => TuplePat(List(VarPat(v),VarPat(p))) }),
                            BoolConst(true),
                            prov(e,Tuple(vs.map{ case (v,p) => Var(v) }),
                                 Call("List",vs.map{ case (v,p) => Var(p) })))))
      case Call(f,s)
        => val ns = s.map(embed(_))
           val vs = s.map(_ => (newvar,newvar))
           MatchE(Tuple(ns),
                  List(Case(TuplePat(vs.map{ case (v,p) => TuplePat(List(VarPat(v),VarPat(p))) }),
                            BoolConst(true),
                            prov(e,Call(f,vs.map{ case (v,p) => Var(v) }),
                                 Call("List",vs.map{ case (v,p) => Var(p) })))))
      case _ => flatMap(Lambda(VarPat("v"),Elem(prov(e,Var("v"),Call("List",List())))),e)
    }
}
