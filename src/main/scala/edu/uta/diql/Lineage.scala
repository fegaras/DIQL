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

  /** variables in the repeat pattern */
  private var repeatVars: List[String] = Nil

  private def label ( e: Expr ): Int = {
    exprs = exprs:+nodeLabel(e)
    exprs.length-1
  }

  private def nodeLabel ( e: Expr ): String
    = e match {
        case Var(v) => v
        case reduce(m,_) => s"reduce($m)"
        case Call(f,_) => s"call($f)"
        case MethodCall(_,f,_) => s"method($f)"
        case Constructor(f,_) => s"constructor($f)"
        case StringConst(_) => ""
        case IntConst(_) => ""
        case LongConst(_) => ""
        case DoubleConst(_) => ""
        case BoolConst(_) => ""
        case _
          => import Pretty._
             val s = e.toString
             parseAll(tree,s) match {
               case Success(t:Node,_) => t.name
               case _ => s
             }
      }

  /** return the value from the result */
  private def value ( e: Expr ): Expr
    = flatMap(Lambda(CallPat("ResultValue",List(VarPat("v"),VarPat("p"))),
                     Elem(Var("v"))),
              e)

  /** return the lineage from the result */
  private def lineage ( e: Expr ): Expr
    = flatMap(Lambda(CallPat("ResultValue",List(VarPat("v"),VarPat("p"))),
                     Elem(Var("p"))),
              e)

  /** map {((k,v),p)} to {(k,(v,p))} */
  private def flip ( e: Expr ): Expr
    = flatMap(Lambda(CallPat("ResultValue",List(TuplePat(List(VarPat("k"),VarPat("v"))),VarPat("p"))),
                     Elem(Tuple(List(Var("k"),Tuple(List(Var("v"),Var("p"))))))),
              e)

  /** map {(v,p)} to {v} */
  private def first ( e: Expr ): Expr
    = flatMap(Lambda(TuplePat(List(TuplePat(List(VarPat("v"),VarPat("p"))))),
                     Elem(Var("v"))),
              e)

  /** map {(v,p)} to {p} */
  private def second ( e: Expr ): Expr
    = flatMap(Lambda(TuplePat(List(TuplePat(List(VarPat("v"),VarPat("p"))))),
                     Elem(Var("p"))),
              e)

  /** Construct a provenance tuple
   * @param expr the AST that corresponds to this value
   * @param value the value
   * @param provenance the input provenance of this value
   * @return a provenance tuple
   */
  private def prov ( expr: Expr, value: Expr, provenance: Expr ): Expr = {
    val loc = label(expr)
    value match {
      case Var(_)
        => Call("ResultValue",
                List(value,
                     Constructor("UnaryLineage",
                                 List(IntConst(loc),value,provenance))))
      case _
        => MatchE(value,
                  List(Case(VarPat("v"),BoolConst(true),
                            Call("ResultValue",
                                 List(Var("v"),
                                      Constructor("UnaryLineage",
                                                  List(IntConst(loc),Var("v"),provenance)))))))
    }
  }

  private def prov ( expr: Expr, value: Expr, left: Expr, right: Expr ): Expr = {
    val loc = label(expr)
    value match {
      case Var(_)
        => Call("ResultValue",
                List(value,
                     Constructor("BinaryLineage",
                                 List(IntConst(loc),value,left,right))))
      case _
        => MatchE(value,
                  List(Case(VarPat("v"),BoolConst(true),
                            Call("ResultValue",
                                 List(Var("v"),
                                      Constructor("BinaryLineage",
                                                  List(IntConst(loc),Var("v"),left,right)))))))
    }
  }

  /** Lift the expression e of type {t} to {(t,provenance)} */
  private def embed ( e: Expr ): Expr
    = e match {
      case repeat(Lambda(p,u),x,Lambda(_,c),n)
        => repeatVars = patvars(p)++repeatVars
           val nc = repeatVars.foldRight(c){ case (z,r) => subst(z,value(Var(z)),r) }
           repeat(Lambda(p,embed(u)),embed(x),Lambda(p,nc),n)
      case flatMap(Lambda(p,b),x)
        => val v = newvar
           val w = newvar
           val tp = distributed.ExprElemType(x)
           val loc = label(e)
           flatMap(Lambda(VarPat(w),
                          Call("liftFlatMap",
                               List(TypedLambda(List((v,tp)),
                                                MatchE(Var(v),List(Case(p,BoolConst(true),b)))),
                                    Var(w),
                                    IntConst(loc)))),
                   embed(x))
      case groupBy(x)
        => val w = newvar
           val loc = label(e)
           MatchE(embed(x),
                  List(Case(VarPat(w),BoolConst(true),
                            Merge(flatMap(Lambda(TuplePat(List(VarPat("k"),VarPat("s"))),
                                                 Elem(Call("provenance",
                                                           List(Tuple(List(Var("k"),first(Var("s")))),
                                                                IntConst(loc),second(Var("s")))))),
                                          groupBy(flip(Var(w)))),
                                  flatMap(Lambda(VarPat("v"),Call("propagateLineage",List(Var("v")))),
                                          Var(w))))))
      case orderBy(x)
        => val w = newvar
           val loc = label(e)
           MatchE(embed(x),
                  List(Case(VarPat(w),BoolConst(true),
                            Merge(flatMap(Lambda(TuplePat(List(VarPat("v"),VarPat("p"))),
                                                 Elem(Call("provenance",
                                                           List(Var("v"),IntConst(loc),
                                                                Elem(Var("p")))))),
                                          orderBy(flip(Var(w)))),
                                  flatMap(Lambda(VarPat("v"),Call("propagateLineage",List(Var("v")))),
                                          Var(w))))))
      case coGroup(x,y)
        => val xv = newvar
           val yv = newvar
           val loc = label(e)
           val ne = flatMap(Lambda(TuplePat(List(VarPat("k"),TuplePat(List(VarPat("xs"),VarPat("ys"))))),
                                   Elem(Call("provenance",
                                             List(Tuple(List(Var("k"),Tuple(List(first(Var("xs")),first(Var("ys")))))),
                                                  IntConst(loc),second(Var("xs")),second(Var("ys")))))),
                            coGroup(flip(Var(xv)),flip(Var(yv))))
           MatchE(embed(x),
                  List(Case(VarPat(xv),BoolConst(true),
                       MatchE(embed(y),
                              List(Case(VarPat(yv),BoolConst(true),
                                        Merge(Merge(ne,
                                                    flatMap(Lambda(VarPat("v"),Call("propagateLineage",List(Var("v")))),
                                                                   Var(xv))),
                                              flatMap(Lambda(VarPat("v"),Call("propagateLineage",List(Var("v")))),
                                                      Var(yv)))))))))
      case cross(x,y)
        => val xv = newvar
           val yv = newvar
           val loc = label(e)
           val ne = flatMap(Lambda(TuplePat(List(CallPat("ResultValue",List(VarPat("x"),VarPat("px"))),
                                                 CallPat("ResultValue",List(VarPat("y"),VarPat("py"))))),
                                   Elem(Call("provenance",
                                             List(Tuple(List(Var("x"),Var("y"))),IntConst(loc),
                                                  Elem(Var("px")),Elem(Var("py")))))),
                            cross(Var(xv),Var(yv)))
           MatchE(embed(x),
                  List(Case(VarPat(xv),BoolConst(true),
                       MatchE(embed(y),
                              List(Case(VarPat(yv),BoolConst(true),
                                        Merge(Merge(ne,
                                                    flatMap(Lambda(VarPat("v"),Call("propagateLineage",List(Var("v")))),
                                                                   Var(xv))),
                                              flatMap(Lambda(VarPat("v"),Call("propagateLineage",List(Var("v")))),
                                                      Var(yv)))))))))
      case reduce(m,x)
        => val nv = newvar
           MatchE(embed(x),
                  List(Case(VarPat(nv),BoolConst(true),
                            prov(e,reduce(m,value(Var(nv))),lineage(Var(nv))))))
      case SmallDataSet(x)
        => embed(x)
      case Merge(x,y)
        => val xv = newvar
           val yv = newvar
           val loc = label(e)
           val ne = flatMap(Lambda(TuplePat(List(VarPat("v"),VarPat("p"))),
                                   Elem(Call("provenance",
                                             List(Var("v"),IntConst(loc),Var("v"),Var("p"))))),
                            Merge(Var(xv),Var(yv)))
           MatchE(embed(x),
                  List(Case(VarPat(xv),BoolConst(true),
                       MatchE(embed(y),
                              List(Case(VarPat(yv),BoolConst(true),
                                        Merge(Merge(ne,
                                                    flatMap(Lambda(VarPat("v"),Call("propagateLineage",List(Var("v")))),
                                                                   Var(xv))),
                                              flatMap(Lambda(VarPat("v"),Call("propagateLineage",List(Var("v")))),
                                                      Var(yv)))))))))
      case Var(v) if repeatVars.contains(v) => e
      case _
        => val loc = label(e)
           flatMap(Lambda(VarPat("v"),
                          Elem(Call("ResultValue",
                                    List(Var("v"),
                                         Constructor("UnaryLineage",
                                                     List(IntConst(loc),Var("v"),Call("List",List()))))))),
                   e)
  }

  /** Lift the expression e of type t to (t,provenance) */
  private def embed_lineage ( e: Expr, isDistr: Expr => Boolean ): Expr
    = if (isDistr(e))
         embed(e)
      else e match {
      case Tuple(s)
        => val ns = s.map(embed_lineage(_,isDistr))
           val vs = s.map(_ => (newvar,newvar))
           MatchE(Tuple(ns),
                  List(Case(TuplePat(vs.map{ case (v,p) => CallPat("ResultValue",List(VarPat(v),VarPat(p))) }),
                            BoolConst(true),
                            prov(e,Tuple(vs.map{ case (v,_) => Var(v) }),
                                 Call("List",vs.map{ case (_,p) => Var(p) })))))
      case Call(f,s)
        => val ns = s.map(embed_lineage(_,isDistr))
           val vs = s.map(_ => (newvar,newvar))
           MatchE(Tuple(ns),
                  List(Case(TuplePat(vs.map{ case (v,p) => CallPat("ResultValue",List(VarPat(v),VarPat(p))) }),
                            BoolConst(true),
                            prov(e,Call(f,vs.map{ case (v,_) => Var(v) }),
                                 Call("List",vs.map{ case (_,p) => Var(p) })))))
      case Constructor(f,s)
        => val ns = s.map(embed_lineage(_,isDistr))
           val vs = s.map(_ => (newvar,newvar))
           MatchE(Tuple(ns),
                  List(Case(TuplePat(vs.map{ case (v,p) => CallPat("ResultValue",List(VarPat(v),VarPat(p))) }),
                            BoolConst(true),
                            prov(e,Constructor(f,vs.map{ case (v,_) => Var(v) }),
                                 Call("List",vs.map{ case (_,p) => Var(p) })))))
      case MethodCall(o,m,s)
        => val os = embed_lineage(o,isDistr)
           val ns = s.map(embed_lineage(_,isDistr))
           val vs = (o::s).map(_ => (newvar,newvar))
           MatchE(Tuple(os::ns),
                  List(Case(TuplePat(vs.map{ case (v,p) => CallPat("ResultValue",List(VarPat(v),VarPat(p))) }),
                            BoolConst(true),
                            prov(e,MethodCall(Var(vs.head._1),m,vs.tail.map{ case (v,_) => Var(v) }),
                                 Call("List",vs.map{ case (_,p) => Var(p) })))))
      case IfE(p,t,f)
        => MatchE(Tuple(List(embed_lineage(p,isDistr),embed_lineage(t,isDistr),embed_lineage(f,isDistr))),
                  List(Case(TuplePat(List(VarPat("p"),VarPat("t"),VarPat("f"))),
                            BoolConst(true),
                            prov(e,IfE(Nth(Var("p"),1),Nth(Var("t"),1),Nth(Var("e"),1)),
                                 Call("List",List(Nth(Var("p"),2),Nth(Var("t"),2),Nth(Var("e"),2)))))))
      case MatchE(x,cs)
        => val ns = cs.map{ case Case(p,b,n) => Case(p,b,embed_lineage(n,isDistr)) }
           MatchE(embed_lineage(x,isDistr),
                  List(Case(VarPat("x"),
                            BoolConst(true),
                            MatchE(MatchE(Nth(Var("x"),1),ns),
                                   List(Case(VarPat("v"),
                                             BoolConst(true),
                                             prov(e,Nth(Var("v"),1),Nth(Var("x"),2),Nth(Var("v"),2))))))))
      case Nth(x,n)
        => MatchE(embed_lineage(x,isDistr),
                  List(Case(VarPat("x"),
                            BoolConst(true),
                            prov(e,Nth(Nth(Var("x"),1),n),Nth(Var("x"),2)))))
      case Elem(x)
        => MatchE(embed_lineage(x,isDistr),
                  List(Case(VarPat("x"),
                            BoolConst(true),
                            prov(e,Elem(Nth(Var("x"),1)),Nth(Var("x"),2)))))
      case _ => MatchE(e,List(Case(VarPat("v"),BoolConst(true),prov(e,Var("v"),Call("List",List())))))
    }

    /** Lift the expression e of type t to (t,provenance) */
  def embedLineage ( e: Expr, isDistr: Expr => Boolean ): Expr = {
    exprs = Nil
    repeatVars = Nil
    embed_lineage(e,isDistr)
  }
}
