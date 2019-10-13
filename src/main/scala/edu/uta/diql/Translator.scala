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

object Translator {
  import AST._

  /** Collect all pattern variables into a list */
  def pv ( p: Pattern, except: List[String] ): List[String] =
    p match {
      case VarPat(s) if s != "_" && !except.contains(s) => List(s)
      case RestPat(s) if s != "_" && !except.contains(s) => List(s)
      case NamedPat(n,np) if !except.contains(n) => n::pv(np,except)
      case _ => accumulatePat[List[String]](p,pv(_,except),_++_,Nil)
    }

  /** Collect the pattern variables of a query qualifier into a list */
  def qv ( q: Qualifier, except: List[String] ): List[String] =
    q match {
      case Generator(p,_) => pv(p,except)
      case LetBinding(p,_) => pv(p,except)
      case _ => List()
    }

  /** Translate a sequence of query qualifiers to an expression */  
  def translateQualifiers ( result: Expr, qs: List[Qualifier] ): Expr
      = qs match {
        case Nil => Elem(translate(result))
        case Generator(p,e)+:ns
          => val te = translate(e)
             val ne = translateQualifiers(result,ns)
             flatMap(Lambda(p,ne),te)
        case LetBinding(p,e)+:ns
          => MatchE(translate(e),List(Case(p,BoolConst(true),
                                           translateQualifiers(result,ns))))
        case Predicate(e)+:ns
          => IfE(translate(e),translateQualifiers(result,ns),Empty())
      }

  private def mkTuple ( ps: List[Expr] ): Expr =
    ps match {
      case List(e) => e
      case _ => Tuple(ps)
    }

  private def mkTuplePat ( ps: List[Pattern] ): Pattern =
    ps match {
      case List(p) => p
      case _ => TuplePat(ps)
    }

  /** Translate select-queries to the algebra */
  def translate ( e: Expr ): Expr =
    e match {
      case SelectDistQuery(out,qs,gb,ob)
        => val nv = newvar
           val mv = newvar
           flatMap(Lambda(TuplePat(List(VarPat(mv),StarPat())),Elem(Var(mv))),
                   groupBy(flatMap(Lambda(VarPat(nv),Elem(Tuple(List(Var(nv),IntConst(0))))),
                                   translate(SelectQuery(out,qs,gb,ob)))))
      case SelectQuery(out,qs,gb,Some(OrderByQual(k)))
        => orderBy(translate(SelectQuery(Tuple(List(k,out)),qs,gb,None)))
      case SelectQuery(out,qs,Some(GroupByQual(p,k,h,None)),None)
        => val groupByVars = pv(p,List())
           val varsUsed = freevars(Tuple(List(out,h)),Nil)
           val liftedVars = qs.flatMap(q => qv(q,groupByVars)) intersect varsUsed
           val lp = mkTuplePat(liftedVars.map(VarPat))
           val s = newvar
           def lift ( x: Expr ) = liftedVars.foldRight(x) {
                                     case (v,r) => subst(v,flatMap(Lambda(lp,Elem(Var(v))),
                                                                   Var(s)),
                                                         r) }
           val liftedOut = lift(translate(out))
           val liftedHaving = lift(translate(h))
           flatMap(Lambda(TuplePat(List(p,VarPat(s))),
                          IfE(liftedHaving,Elem(liftedOut),Empty())),
                   groupBy(translate(SelectQuery(Tuple(List(k,mkTuple(liftedVars.map(Var)))),
                                                 qs,None,None))))
      case SelectQuery(out,qs,Some(GroupByQual(p,k,h,Some(CoGroupQual(qs2,p2,k2)))),None)
        => val varsUsed = freevars(Tuple(List(out,h)),Nil)
           val leftVars = qs.flatMap(q => qv(q,pv(p,List()))) intersect varsUsed
           val rightVars = qs2.flatMap(q => qv(q,pv(p2,List()))) intersect varsUsed
           val left = translate(SelectQuery(Tuple(List(k,mkTuple(leftVars.map(Var)))),
                                            qs,None,None))
           val right = translate(SelectQuery(Tuple(List(k2,mkTuple(rightVars.map(Var)))),
                                             qs2,None,None))
           val x = newvar
           val y = newvar
           def lift ( x: Expr, s: String, vars: List[String] ): Expr
             = vars.foldRight(x){ case (v,r) => subst(v,flatMap(Lambda(mkTuplePat(vars.map(VarPat)),Elem(Var(v))),
                                                                Var(s)),
                                                       r) }
           val liftedOut = lift(lift(translate(out),x,leftVars),y,rightVars)
           val liftedHaving = lift(lift(translate(h),x,leftVars),y,rightVars)
           flatMap(Lambda(TuplePat(List(p,TuplePat(List(VarPat(x),VarPat(y))))),
                          MatchE(AST.toExpr(p),List(Case(p2,BoolConst(true),
                                 IfE(liftedHaving,Elem(liftedOut),Empty()))))),
                   coGroup(left,right))
      case SelectQuery(out,qs,None,None)
        => translateQualifiers(out,qs)
      case SomeQuery(out,qs)
        => reduce(BaseMonoid("||"),
                  qs.foldRight(IfE(translate(out),Elem(BoolConst(true)),Empty()):Expr) {
                        case (Generator(p,de),r) => flatMap(Lambda(p,r),translate(de))
                        case (LetBinding(p,de),r)
                          => MatchE(translate(de),List(Case(p,BoolConst(true),r)))
                        case (Predicate(ce),r) => IfE(translate(ce),r,Empty())
                  })
      case AllQuery(out,qs)
        => reduce(BaseMonoid("&&"),
                  qs.foldRight(Elem(translate(out)):Expr) {
                        case (Generator(p,de),r) => flatMap(Lambda(p,r),translate(de))
                        case (LetBinding(p,de),r)
                          => MatchE(translate(de),List(Case(p,BoolConst(true),r)))
                        case (Predicate(ce),r) => IfE(translate(ce),r,Empty())
                  })
      case Call(f,xs)
        if macro_defs.contains(f)
        => val nxs = xs.map(translate)
           macro_defs(f) match {
               case (vs,b) => translate( (vs zip nxs).foldLeft(b){
                                           case (r,((v,_),x)) => subst(v,x,r)
                                       } )
           }
      case MethodCall(x,f,xs)
        if macro_defs.contains(f)
        => macro_defs(f) match {
             case (vs,b) => translate( (("this",x)::(vs.map(_._1) zip xs)).foldLeft(b){
                                           case (r,(v,z)) => subst(v,z,r)
                                       } )
             }
      case MethodCall(Var(a),"/",List(x))
        if monoids.contains(a)
        => translate(reduce(BaseMonoid(a),x))
      case MethodCall(x,"union",List(y))
        => Merge(translate(x),translate(y))
      case MethodCall(x,"member",List(y))
        => val nv = newvar
           val xv = newvar
           MatchE(translate(x),
                  List(Case(VarPat(xv),BoolConst(true),
                       reduce(BaseMonoid("||"),
                              flatMap(Lambda(VarPat(nv),
                                             IfE(MethodCall(Var(xv),"==",List(Var(nv))),
                                                 Elem(BoolConst(true)),Empty())),
                                     translate(y))))))
      case MethodCall(x,"intersect",List(y))
        => val xv = newvar
           val yv = newvar
           flatMap(Lambda(VarPat(xv),
                          IfE(reduce(BaseMonoid("||"),
                                     flatMap(Lambda(VarPat(yv),
                                                    IfE(MethodCall(Var(xv),"==",List(Var(yv))),
                                                        Elem(BoolConst(true)),Empty())),
                                             translate(y))),
                              Elem(Var(xv)), Empty())),
                   translate(x))
      case MethodCall(x,"minus",List(y))
        => val xv = newvar
           val yv = newvar
           flatMap(Lambda(VarPat(xv),
                          IfE(reduce(BaseMonoid("||"),
                                     flatMap(Lambda(VarPat(yv),
                                                    IfE(MethodCall(Var(xv),"==",List(Var(yv))),
                                                        Elem(BoolConst(true)),Empty())),
                                             translate(y))),
                              Empty(), Elem(Var(xv)))),
                   translate(x))
      case reduce(BaseMonoid("count"),x)
        => reduce(BaseMonoid("+"),
                  flatMap(Lambda(StarPat(),Elem(LongConst(1L))),
                          translate(x)))
      case reduce(BaseMonoid("avg"),x)
        => val nv = newvar
           Call("avg_value",
                List(reduce(BaseMonoid("avg_combine"),
                            flatMap(Lambda(VarPat(nv),
                                           Elem(Constructor("Avg",List(Var(nv),LongConst(1L))))),
                                    translate(x)))))
      case _ => apply(e,translate)
    }
}
