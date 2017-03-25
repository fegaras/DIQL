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

import scala.reflect.macros.whitebox.Context

abstract class Optimizer extends CodeGeneration {
  import AST._

  /** true if e is a distributed dataset that doesn't contain any variables from vars */ 
  def is_distributed ( e: Expr, vars: List[String] ): Boolean
    = isDistributed(e) && freevars(e).intersect(vars).isEmpty

  /** true if e doesn't contain any variables from vars */ 
  def isInMemory ( e: Expr, vars: List[String] ): Boolean
    = freevars(e).intersect(vars).isEmpty

  /** key is a valid join key if all its free variables are from vars */
  def isKey ( key: Expr, vars: List[String] ): Boolean =
    freevars(key,vars).isEmpty && freevars(key,Nil).intersect(vars).nonEmpty

  /** find a pair of terms (k1,k2) such that k1 depends on the variables xs
   *  and k2 depends on the variables ys with k1!=k2 => e==Nil
   */
  def joinCondition ( e: Expr, xs: List[String], ys: List[String] ): Option[(Expr,Expr)] =
    e match {
      case MethodCall(k1,"==",List(k2))
        => if (isKey(k1,xs) && isKey(k2,ys))
              Some((k1,k2))
           else if (isKey(k1,ys) && isKey(k2,xs))
              Some((k2,k1))
           else None
      case MethodCall(a,"&&",List(b))
        => joinCondition(a,xs,ys) match {
              case Some((k1a,k2a))
                => joinCondition(b,xs,ys) match {
                      case Some((k1b,k2b))
                          => Some((Tuple(List(k1a,k1b)),Tuple(List(k2a,k2b))))
                      case _ => Some((k1a,k2a))
                   }
              case _ => joinCondition(b,xs,ys)
          }
      case _ => accumulate[Option[(Expr,Expr)]](e,joinCondition(_,xs,ys),
                          { case (x@Some(_),_) => x; case (_,y) => y },None)
    }

  /** find a join predicate in e that relates the variables xs and ys */
  def joinPredicate ( e: Expr, xs: List[String], ys: List[String] ): Option[(Expr,Expr,Expr=>Expr,Expr=>Expr)] =
    e match {
      case IfE(pred,et,Empty())
        => joinCondition(pred,xs,ys) match {
              case Some((kx,ky)) => Some((kx,ky,identity,identity))
              case _ => joinPredicate(et,xs,ys)
           }
      case flatMap(Lambda(p,b),u)
        if freevars(u).intersect(xs).nonEmpty
        => joinPredicate(b,xs++patvars(p),ys) match {
             case Some((kx,ky,mapx,mapy))
               => Some((kx,ky,x => flatMap(Lambda(p,mapx(x)),u),mapy))
             case _ => joinPredicate(u,xs,ys)
           }
      case flatMap(Lambda(p,b),u)
        if freevars(u).intersect(ys).nonEmpty
        => joinPredicate(b,xs,ys++patvars(p)) match {
             case Some((kx,ky,mapx,mapy))
               => Some((kx,ky,mapx,y => flatMap(Lambda(p,mapy(y)),u)))
             case _ => joinPredicate(u,xs,ys)
           }
      case MatchE(u,List(Case(p,BoolConst(true),b)))
        if freevars(u).intersect(xs).nonEmpty
        => joinPredicate(b,xs++patvars(p),ys) match {
             case Some((kx,ky,mapx,mapy))
               => Some((kx,ky,x => MatchE(u,List(Case(p,BoolConst(true),mapx(x)))),mapy))
             case _ => joinPredicate(u,xs,ys)
           }
      case MatchE(u,List(Case(p,BoolConst(true),b)))
        if freevars(u).intersect(ys).nonEmpty
        => joinPredicate(b,xs,ys++patvars(p)) match {
             case Some((kx,ky,mapx,mapy))
               => Some((kx,ky,mapx,y => MatchE(u,List(Case(p,BoolConst(true),mapy(y))))))
             case _ => joinPredicate(u,xs,ys)
           }
      case _ => accumulate[Option[(Expr,Expr,Expr=>Expr,Expr=>Expr)]](e,joinPredicate(_,xs,ys),
                          { case (x@Some(_),_) => x; case (_,y) => y }, None )
    }

  /** find the left input of the equi-join, if any */
  def findJoinMatch ( e: Expr, xs: List[String], vars: List[String], distrLeft: Boolean ): Option[(Expr,Expr,Expr=>Expr,Expr=>Expr,Expr)] =
    e match {
      case flatMap(Lambda(p,b),u)
        if (distrLeft && isInMemory(u,vars) && freevars(u).intersect(xs).isEmpty)
           || is_distributed(u,vars)
        => joinPredicate(b,xs,patvars(p)) match {
              case Some((kx,ky,mapx,mapy))
                => Some((kx,ky,mapx,mapy,e))
              case _ => findJoinMatch(u,xs,vars,distrLeft)
          }
      case flatMap(Lambda(p,b),u)
        if freevars(u).intersect(xs).nonEmpty
        => findJoinMatch(b,xs++patvars(p),vars++patvars(p),distrLeft) match {
                  case Some((kx,ky,mapx,mapy,d))
                    => Some((kx,ky,x => flatMap(Lambda(p,mapx(x)),u),mapy,d))
                  case _ => findJoinMatch(u,xs,vars,distrLeft)
           }
      case MatchE(u,List(Case(p,BoolConst(true),b)))
        if freevars(u).intersect(xs).nonEmpty
        => findJoinMatch(b,xs++patvars(p),vars++patvars(p),distrLeft) match {
                   case Some((kx,ky,mapx,mapy,d))
                     => Some((kx,ky,x => MatchE(u,List(Case(p,BoolConst(true),mapx(x)))),mapy,d))
                   case _ => findJoinMatch(u,xs,vars,distrLeft)
           }
      case flatMap(Lambda(p,b),u)
        => findJoinMatch(u,xs,vars,distrLeft) orElse
               findJoinMatch(b,xs,vars++patvars(p),distrLeft)
      case MatchE(u,cs)
        => cs.foldLeft[Option[(Expr,Expr,Expr=>Expr,Expr=>Expr,Expr)]](findJoinMatch(u,xs,vars,distrLeft)) {
                        case (r,Case(p,c,b))
                          => r orElse findJoinMatch(b,xs,vars++patvars(p),distrLeft)
                  }
      case _ => accumulate[Option[(Expr,Expr,Expr=>Expr,Expr=>Expr,Expr)]](e,findJoinMatch(_,xs,vars,distrLeft),
                          { case (x@Some(_),_) => x; case (_,y) => y },None)
    }

  /** find equi-joins between datasets in e and convert them to coGroup */
  def deriveJoins ( e: Expr, vars: List[String] ): Expr =
    e match {
      case flatMap(Lambda(px,bx),ex)
        if is_distributed(ex,vars) || isInMemory(ex,vars)
        => findJoinMatch(bx,patvars(px),vars++patvars(px),is_distributed(e,vars)) match {
              case Some((kx,ky,mapx,mapy,cmy@flatMap(Lambda(py,by),ey)))
                => { val xv = newvar
                     val yv = newvar
                     val xs = newvar
                     val ys = newvar
                     val nbx = subst(cmy,flatMap(Lambda(py,by),Var(ys)),bx)
                     val nbb = subst(MethodCall(kx,"==",List(ky)),BoolConst(true),
                                     subst(MethodCall(ky,"==",List(kx)),BoolConst(true),nbx))
                     clean(nbb)   // remove type information
                     if (debug_diql)
                        println("Join between "+ex+"\n         and "+ey+"\n          on "+kx+" == "+ky)
                     flatMap(Lambda(TuplePat(List(StarPat(),TuplePat(List(VarPat(xs),VarPat(ys))))),
                                    flatMap(Lambda(px,nbb),Var(xs))),
                             coGroup(flatMap(Lambda(NamedPat(xv,px),mapx(Elem(Tuple(List(kx,Var(xv)))))),ex),
                                     flatMap(Lambda(NamedPat(yv,py),mapy(Elem(Tuple(List(ky,Var(yv)))))),ey)))
                   }
              case _ => flatMap(Lambda(px,deriveJoins(bx,vars++patvars(px))),
                                deriveJoins(ex,vars))
           }
      case flatMap(Lambda(px,bx),ex)
        => flatMap(Lambda(px,deriveJoins(bx,vars++patvars(px))),
                   deriveJoins(ex,vars))
      case MatchE(x,cs)
        => MatchE(deriveJoins(x,vars),
                  cs.map{ case Case(p,c,b)
                            => Case(p,deriveJoins(c,vars++patvars(p)),
                                    deriveJoins(b,vars++patvars(p))) })
      case _ => apply(e,deriveJoins(_,vars))
    }

  /** find the right input of a cross product */
  def findCrossMatch ( e: Expr, vars: List[String], distrLeft: Boolean ): Option[Expr] =
    if (is_distributed(e,vars))
       Some(e)
    else e match {
       case flatMap(Lambda(px,bx),ex)
         => if (false && distrLeft && isInMemory(ex,vars))
               Some(e)
            else findCrossMatch(ex,vars,distrLeft) match {
                    case nex@Some(_) => nex
                    case _ => findCrossMatch(bx,vars++patvars(px),distrLeft)
                 }
       case MatchE(x,cs)
         => findCrossMatch(x,vars,distrLeft) orElse
               cs.foldLeft[Option[Expr]](None){
                    case (r,Case(p,c,b))
                      => r orElse findCrossMatch(b,patvars(p)++vars,distrLeft)
               }
       case _ => accumulate[Option[Expr]](e,findCrossMatch(_,vars,distrLeft),
                           { case (x@Some(_),_) => x; case (_,y) => y },None)
    }

  /** find all cross products that cannot be joined with any other dataset */
  def deriveCrossProducts ( e: Expr, vars: List[String] ): Expr =
    e match {
      case flatMap(Lambda(px,bx),ex)
        if is_distributed(ex,vars) || isInMemory(ex,vars)
        => findCrossMatch(bx,vars++patvars(px),is_distributed(ex,vars)) match {
              case Some(right)
                => val nv = newvar
                   val nbx = subst(right,Elem(Var(nv)),bx)
                   clean(nbx)
                   if (debug_diql)
                      println("Cross product between "+ex+"\n                  and "+right)
                   flatMap(Lambda(TuplePat(List(px,VarPat(nv))),nbx),
                           cross(ex,right))
              case _ => flatMap(Lambda(px,deriveCrossProducts(bx,vars++patvars(px))),
                                       deriveCrossProducts(ex,vars))
           }
      case flatMap(Lambda(px,bx),ex)
        => flatMap(Lambda(px,deriveCrossProducts(bx,vars++patvars(px))),
                   deriveCrossProducts(ex,vars))
      case MatchE(x,cs)
        => MatchE(deriveCrossProducts(x,vars),
                  cs.map{ case Case(p,c,b)
                            => Case(p,deriveCrossProducts(c,vars++patvars(p)),
                                    deriveCrossProducts(b,vars++patvars(p))) })
      case _ => apply(e,deriveCrossProducts(_,vars))
    }

  /** return a distributed term that has no free vars */
  def findFactor ( e: Expr, vars: List[String] ): Option[Expr] =
    e match {
      case reduce(m,x)
        if freevars(x,Nil).intersect(vars).isEmpty && isDistributed(x)
        => if (debug_diql)
              println("Factor out "+e)
           Some(e)
      case flatMap(Lambda(p,b),x)
        => findFactor(x,vars) orElse findFactor(b,patvars(p)++vars)
      case MatchE(x,cs)
        => findFactor(x,vars) orElse
              cs.foldLeft[Option[Expr]](None){
                  case (r,Case(p,c,b)) => r orElse findFactor(b,patvars(p)++vars)
              }
      case Lambda(p,b)
        => findFactor(b,patvars(p)++vars)
      case _ => accumulate[Option[Expr]](e,findFactor(_,vars),
              { case (x@Some(_),_) => x; case (_,y) => y },None)
    }

  /** return a distributed term that is inside a flatMap functional and has no free vars */
  def getFactor ( e: Expr, vars: List[String] ): Option[Expr] =
    e match {
      case flatMap(Lambda(p,b),x)
        => getFactor(x,vars) orElse findFactor(b,patvars(p)++vars)
      case MatchE(x,cs)
        => getFactor(x,vars) orElse
              cs.foldLeft[Option[Expr]](None){
                  case (r,Case(p,c,b)) => r orElse getFactor(b,patvars(p)++vars)
              }
      case Lambda(p,b)
        => getFactor(b,patvars(p)++vars)
      case _ => accumulate[Option[Expr]](e,getFactor(_,vars),
              { case (x@Some(_),_) => x; case (_,y) => y },None)
  }

  /** pull out all distributed terms with no free vars */
  def pullOutFactors ( e: Expr ): Expr =
    getFactor(e,Nil) match {
      case Some(x)
        => val nv = newvar
           if (debug_diql)
              println("Pull out factor "+x+"\n                from "+e)
           MatchE(x,List(Case(VarPat(nv),BoolConst(true),
                              pullOutFactors(subst(x,Var(nv),e)))))
      case _ => e
    }

  /** split a predicate e into two parts: one that depends on contains variables
   *  but doesn't depend on excludes, and another that doesn't 
   */
  def splitPredicate ( e: Expr, contains: List[String], excludes: List[String] ): Option[(Expr,Expr)] =
    if (e.isInstanceOf[BoolConst])
      None
    else if (freevars(e,Nil).intersect(excludes).isEmpty
             && (contains.isEmpty || freevars(e,Nil).intersect(contains).nonEmpty))
       Some((e,BoolConst(true)))
    else e match {
      case MethodCall(x,"&&",List(y))
         => splitPredicate(x,contains,excludes) match {
               case Some((x1,x2))
                 => splitPredicate(y,contains,excludes) match {
                       case Some((y1,y2))
                         => Some((MethodCall(x1,"&&",List(y1)),
                                  MethodCall(x2,"&&",List(y2))))
                       case _ => Some((x1,MethodCall(x2,"&&",List(y))))
                    }
               case _ => splitPredicate(y,contains,excludes) match {
                           case Some((y1,y2))
                             => Some((y1,MethodCall(x,"&&",List(y2))))
                           case _ => None
                         }
            }
      case _ => None
    }

  /** push filters before coGroups */
  def optimize ( e: Expr ): Expr =
    e match {
      case flatMap(Lambda(p,b@IfE(c,y,Empty())),x)
        => splitPredicate(c,Nil,patvars(p)) match {
             case Some((c1,c2))
               => if (debug_diql)
                     println("Pull predicate "+c1+" outside flatMap")
                  optimize(IfE(c1,flatMap(Lambda(p,IfE(c2,y,Empty())),x),Empty()))
             case _ => flatMap(Lambda(p,optimize(b)),optimize(x))
           }
      case flatMap(Lambda(p@TuplePat(List(k,TuplePat(List(xs,ys)))),
                          b@flatMap(Lambda(px,flatMap(Lambda(py,IfE(c,u,Empty())),_ys)),_xs)),
                   coGroup(x,y))
        if _xs == toExpr(xs) && _ys == toExpr(ys)
        => splitPredicate(c,patvars(px),patvars(py)) match {
             case Some((c1,c2))
               => val nv = newvar
                  val nk = newvar
                  if (debug_diql)
                     println("Push predicate "+c1+" on cogroup left")
                  val nx = flatMap(Lambda(TuplePat(List(VarPat(nk),NamedPat(nv,px))),
                                          IfE(c1,Elem(Tuple(List(Var(nk),Var(nv)))),
                                              Empty())),
                                   x)
                  clean(nx)   // remove type information
                  optimize(flatMap(Lambda(p,flatMap(Lambda(px,flatMap(Lambda(py,IfE(c2,u,Empty())),_ys)),_xs)),
                                   coGroup(nx,y)))
             case _ => splitPredicate(c,patvars(py),patvars(px)) match {
                          case Some((c1,c2))
                            => val nv = newvar
                               val nk = newvar
                               if (debug_diql)
                                  println("Push predicate "+c1+" on cogroup right")
                               val ny = flatMap(Lambda(TuplePat(List(VarPat(nk),NamedPat(nv,py))),
                                                       IfE(c1,Elem(Tuple(List(Var(nk),Var(nv)))),
                                                           Empty())),
                                                y)
                               clean(ny)   // remove type information
                               optimize(flatMap(Lambda(p,flatMap(Lambda(px,flatMap(Lambda(py,IfE(c2,u,Empty())),_ys)),_xs)),
                                                coGroup(x,ny)))
                          case _ => flatMap(Lambda(p,optimize(b)),optimize(coGroup(x,y)))
                        }
           }
      case _ => apply(e,optimize(_))
  }

  def optimizeAll ( e: Expr ): Expr = {
    var olde = e
    var ne = pullOutFactors(e)
    do { olde = ne
         ne = Normalizer.normalizeAll(deriveJoins(ne,Nil))
         if (olde != ne)
            typecheck(ne)
       } while (olde != ne)
    do { olde = ne
         ne = Normalizer.normalizeAll(deriveCrossProducts(ne,Nil))
         if (olde != ne)
            typecheck(ne)
       } while (olde != ne)
    do { olde = ne
         ne = optimize(ne)
         ne = Normalizer.normalizeAll(ne)
       } while (olde != ne)
    ne
  }
}
