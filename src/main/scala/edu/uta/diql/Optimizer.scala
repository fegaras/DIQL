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


abstract class Optimizer extends CodeGeneration {
  import AST._

  /** true if e is a distributed dataset that doesn't contain any variables from vars */ 
  def is_distributed ( e: Expr, vars: List[String] ): Boolean
    = isDistributed(e) && freevars(e).intersect(vars).isEmpty

  /** true if e is a in-memory collection traversal that doesn't contain any variables from vars */ 
  def is_inmemory ( e: Expr, vars: List[String] ): Boolean
    = e match {
        case Elem(_) => false
        case _ => isInMemory(e) && freevars(e).intersect(vars).isEmpty
      }

  /** key is a valid join key if all its free variables are from vars */
  def isKey ( key: Expr, vars: List[String] ): Boolean =
    freevars(key,vars).isEmpty && freevars(key,Nil).intersect(vars).nonEmpty

  /** find pairs of terms (k1,k2) such that k1 depends on the variables xs
   *  and k2 depends on the variables ys with k1!=k2 => e==Nil
   */
  def joinCondition ( e: Expr, xs: List[String], ys: List[String] ): List[(Expr,Expr)] =
    e match {
      case MethodCall(k1,"==",List(k2))
        => if (isKey(k1,xs) && isKey(k2,ys))
              List((k1,k2))
           else if (isKey(k1,ys) && isKey(k2,xs))
              List((k2,k1))
           else Nil
      case MethodCall(a,"&&",List(b))
        => joinCondition(a,xs,ys) ++ joinCondition(b,xs,ys)
      case _ => Nil
    }

  /* does e depend on variables in s but not in vars? */
  def dependsExclusively ( e: Expr, s: List[String], vars: List[String] ): Boolean
    = { val fv = freevars(e)
        fv.intersect(s).nonEmpty && fv.intersect(vars.filter(!s.contains(_))).isEmpty
      }

  /** find a join predicate in e that relates the variables xs and ys.
   *  It returns (kx,ky,mapx,mapy) where kx and ky are the left and right join keys
   *  and mapx and mapy are the flatMap and MatchE to embed in left and right input
   *  to resolve variable dependencies  */
  def joinPredicate ( e: Expr, xs: List[String], ys: List[String], vars: List[String] )
          : List[(Expr,Expr,Expr=>Expr,Expr=>Expr)] =
    e match {
      case IfE(pred,et,Empty())
        => joinCondition(pred,xs,ys).map{ case (kx,ky) => (kx,ky,identity[Expr](_),identity[Expr](_)) } ++
              joinPredicate(et,xs,ys,vars)
      case flatMap(Lambda(p,b),u)
        if dependsExclusively(u,xs,vars)
           && freevars(u).intersect(ys).isEmpty
        => joinPredicate(b,xs++patvars(p),ys,vars)
              .map{ case (kx,ky,mapx,mapy)
                      => (kx,ky,(x:Expr) => flatMap(Lambda(p,mapx(x)),u),mapy) }
           match { case Nil => joinPredicate(u,xs,ys,vars); case ps => ps }
      case flatMap(Lambda(p,b),u)
        if dependsExclusively(u,ys,vars)
           && freevars(u).intersect(xs).isEmpty
        => joinPredicate(b,xs,ys++patvars(p),vars++patvars(p))
              .map{ case (kx,ky,mapx,mapy)
                      => (kx,ky,mapx,(y:Expr) => flatMap(Lambda(p,mapy(y)),u)) }
           match { case Nil => joinPredicate(u,xs,ys,vars); case ps => ps }
      case flatMap(Lambda(p,b),u)
        => joinPredicate(b,xs,ys,vars++patvars(p))
      case MatchE(u,List(Case(p,BoolConst(true),b)))
        if dependsExclusively(u,xs,vars)
           && freevars(u).intersect(ys).isEmpty
        => joinPredicate(b,xs++patvars(p),ys,vars++patvars(p))
              .map{ case (kx,ky,mapx,mapy)
                      => (kx,ky,(x:Expr) => MatchE(u,List(Case(p,BoolConst(true),mapx(x)))),mapy) }
           match { case Nil => joinPredicate(b,xs,ys,vars++patvars(p)); case ps => ps }
      case MatchE(u,List(Case(p,BoolConst(true),b)))
        if dependsExclusively(u,ys,vars)
           && freevars(u).intersect(xs).isEmpty
        => joinPredicate(b,xs,ys++patvars(p),vars++patvars(p))
              .map{ case (kx,ky,mapx,mapy)
                      => (kx,ky,mapx,(y:Expr) => MatchE(u,List(Case(p,BoolConst(true),mapy(y))))) }
           match { case Nil => joinPredicate(b,xs,ys,vars++patvars(p)); case ps => ps }
      case MatchE(u,List(Case(p,BoolConst(true),b)))
        => joinPredicate(b,xs,ys,vars++patvars(p))
      case _ => accumulate[List[(Expr,Expr,Expr=>Expr,Expr=>Expr)]](e,joinPredicate(_,xs,ys,vars),
                          { case (Nil,y) => y; case (x,_) => x }, Nil )
    }

  /** find the left input of the equi-join, if any */
  def findJoinMatch ( e: Expr, xs: List[String], vars: List[String], distrLeft: Boolean )
          : Option[(Expr,Expr,Expr=>Expr,Expr=>Expr,Expr)] =
    e match {
      case flatMap(Lambda(p,b),u)
        if (distrLeft && is_inmemory(u,vars) && freevars(u).intersect(xs).isEmpty)
           || is_distributed(u,vars)
        => joinPredicate(b,xs,patvars(p),vars++patvars(p)) match {
              case Nil
                => findJoinMatch(u,xs,vars,distrLeft)
              case List((kx,ky,mapx,mapy))
                => Some((kx,ky,mapx,mapy,e))
              case cs@(_,_,mapx,mapy)::_
                => Some((Tuple(cs.map(_._1)),Tuple(cs.map(_._2)),mapx,mapy,e))
          }
      case flatMap(Lambda(p,b),u)
        if dependsExclusively(u,xs,vars)
        => findJoinMatch(b,xs++patvars(p),vars++patvars(p),distrLeft) match {
                  case Some((kx,ky,mapx,mapy,d))
                    => Some((kx,ky,x => flatMap(Lambda(p,mapx(x)),u),mapy,d))
                  case _ => findJoinMatch(u,xs,vars,distrLeft)
           }
      case flatMap(Lambda(p,b),u)
        => findJoinMatch(b,xs,vars++patvars(p),distrLeft) orElse
              findJoinMatch(u,xs,vars,distrLeft)
      case MatchE(u,List(Case(p,BoolConst(true),b)))
        if dependsExclusively(u,xs,vars)
        => findJoinMatch(b,xs++patvars(p),vars++patvars(p),distrLeft) match {
                   case Some((kx,ky,mapx,mapy,d))
                     => Some((kx,ky,x => MatchE(u,List(Case(p,BoolConst(true),mapx(x)))),mapy,d))
                   case _ => findJoinMatch(u,xs,vars,distrLeft)
           }
      case MatchE(u,cs)
        => cs.foldLeft[Option[(Expr,Expr,Expr=>Expr,Expr=>Expr,Expr)]](findJoinMatch(u,xs,vars,distrLeft)) {
                        case (r,Case(p,_,b))
                          => r orElse findJoinMatch(b,xs,vars++patvars(p),distrLeft)
                  }
      case _ => accumulate[Option[(Expr,Expr,Expr=>Expr,Expr=>Expr,Expr)]](e,findJoinMatch(_,xs,vars,distrLeft),
                          { case (x@Some(_),_) => x; case (_,y) => y },None)
    }

  /** derive the key-value pair for a coGroup input from e
   *  so that it doesn't generate duplicates in multi-way joins */
  def deriveCoGroupKey ( e: Expr, key: Expr, value: Expr ): Expr =
    e match {
      case Var("@")
        => Elem(Tuple(List(key,value)))
      case flatMap(Lambda(p,b),Var(s))
        if occurrences(s,value) > 0
        => flatMap(Lambda(p,deriveCoGroupKey(b,key,subst(s,Elem(toExpr(p)),value))),Var(s))
      case flatMap(Lambda(p,b),e)
        => flatMap(Lambda(p,deriveCoGroupKey(b,key,value)),e)
      case MatchE(Var(s),List(Case(p,c,b)))
        if occurrences(s,value) > 0
        => MatchE(Var(s),List(Case(p,c,deriveCoGroupKey(b,key,subst(s,toExpr(p),value)))))
      case MatchE(x,List(Case(p,c,b)))
        => MatchE(x,List(Case(p,c,deriveCoGroupKey(b,key,value))))
      case _ => apply(e,deriveCoGroupKey(_,key,value))
    }

  /** find equi-joins between datasets in e and convert them to coGroup */
  def deriveJoins ( e: Expr, vars: List[String] ): Expr =
    e match {
      case flatMap(Lambda(px,bx),ex)
        // must be is_distributed(ex,Nil) not is_distributed(ex,vars)
        if is_distributed(ex,Nil) || is_inmemory(ex,vars)
        => findJoinMatch(bx,patvars(px),vars++patvars(px),is_distributed(e,vars)) match {
                case Some((kx,ky,mapx,mapy,cmy@flatMap(Lambda(py,by),ey)))
                  => val xs = newvar
                     val ys = newvar
                     val nbx = subst(cmy,flatMap(Lambda(py,by),Var(ys)),bx)                    
                     val nbb = (kx,ky) match {
                                 case (Tuple(txs),Tuple(tys))
                                   => (txs zip tys).foldLeft[Expr](nbx){
                                         case (r,(kkx,kky))
                                           => subst(MethodCall(kkx,"==",List(kky)),BoolConst(true),
                                                    subst(MethodCall(kky,"==",List(kkx)),BoolConst(true),r))
                                      }
                                 case _
                                   => subst(MethodCall(kx,"==",List(ky)),BoolConst(true),
                                            subst(MethodCall(ky,"==",List(kx)),BoolConst(true),nbx))
                               }
                     clean(nbb)   // remove type information
                     if (diql_explain)
                        println("Join between "+ex+"\n         and "+ey+"\n          on "+kx+" == "+ky)
                     val left = deriveCoGroupKey(mapx(Var("@")),kx,toExpr(px))
                     val right = deriveCoGroupKey(mapy(Var("@")),ky,toExpr(py))
                     flatMap(Lambda(TuplePat(List(StarPat(),TuplePat(List(VarPat(xs),VarPat(ys))))),
                                    flatMap(Lambda(px,nbb),Var(xs))),
                             coGroup(flatMap(Lambda(px,left),ex),
                                     flatMap(Lambda(py,right),ey)))
              case _ => flatMap(Lambda(px,deriveJoins(bx,vars++patvars(px))),
                                deriveJoins(ex,vars))
           }
      case flatMap(Lambda(px,bx),ex)
        => flatMap(Lambda(px,deriveJoins(bx,vars++patvars(px))),
                   deriveJoins(ex,vars))
      case MatchE(x,cs)
        => MatchE(deriveJoins(x,vars),
                  cs.map{ case Case(p,cc,b)
                            => val nvars = if (is_distributed(x,vars)) vars else vars++patvars(p)
                               Case(p,deriveJoins(cc,nvars),deriveJoins(b,nvars)) })
      case _ => apply(e,deriveJoins(_,vars))
    }

  /** find the right input of a cross product */
  def findCrossMatch ( e: Expr, vars: List[String], distrLeft: Boolean ): Option[Expr] =
    if (is_distributed(e,vars) || (distrLeft && is_inmemory(e,vars)))
       Some(e)             // needed for cross between in-memory and distributed collection
    else e match {
       case flatMap(Lambda(px,bx),ex)
         => findCrossMatch(ex,vars,distrLeft) match {
                  case nex@Some(_) => nex
                  case _ => findCrossMatch(bx,vars++patvars(px),distrLeft)
               }
       case MatchE(x,cs)
         => findCrossMatch(x,vars,distrLeft) orElse
               cs.foldLeft[Option[Expr]](None){
                    case (r,Case(p,_,b))
                      => r orElse findCrossMatch(b,patvars(p)++vars,distrLeft)
               }
       case _ => None
    }

  /** find all cross products that cannot be joined with any other dataset */
  def deriveCrossProducts ( e: Expr, vars: List[String] ): Expr =
    e match {
      case flatMap(Lambda(px,bx),ex)
        if is_distributed(ex,vars) || is_inmemory(ex,vars)
        => findCrossMatch(bx,vars++patvars(px),is_distributed(ex,vars)) match {
              case Some(right)
                => val nv = newvar
                   val nbx = subst(right,Elem(Var(nv)),bx)
                   clean(nbx)
                   if (diql_explain)
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
                  cs.map{ case Case(p,cc,b)
                            => Case(p,deriveCrossProducts(cc,vars++patvars(p)),
                                    deriveCrossProducts(b,vars++patvars(p))) })
      case _ => apply(e,deriveCrossProducts(_,vars))
    }

  /** find the distributed terms inside a flatMap functional */
  def findBroadcastMatch ( e: Expr, vars: List[String] ): Option[Expr] =
    if (is_distributed(e,vars))
       Some(e)
    else e match {
       case flatMap(Lambda(px,bx),ex)
         => findBroadcastMatch(ex,vars) match {
                  case nex@Some(_) => nex
                  case _ => findBroadcastMatch(bx,vars++patvars(px))
               }
       case MatchE(x,cs)
         => findBroadcastMatch(x,vars) orElse
               cs.foldLeft[Option[Expr]](None){
                    case (r,Case(p,_,b))
                      => r orElse findBroadcastMatch(b,patvars(p)++vars)
               }
       case _ => accumulate[Option[Expr]](e,findBroadcastMatch(_,vars),
              { case (x@Some(_),_) => x; case (_,y) => y },None)
    }

  /** find all distributed terms in a flatMap functional that need to be broadcast */
  def deriveBroadcast ( e: Expr, vars: List[String] ): Expr =
    e match {
      case flatMap(Lambda(px,bx),ex)
        if is_distributed(ex,vars)
        => findBroadcastMatch(bx,vars++patvars(px)) match {
              case Some(factor)
                => val nv = newvar
                   val nbx = subst(factor,Var(nv),bx)
                   clean(nbx)
                   if (diql_explain)
                      println("Pull out "+factor+"\n    from "+e)
                   MatchE(factor,List(Case(VarPat(nv),BoolConst(true),
                           deriveBroadcast(flatMap(Lambda(px,nbx),ex),vars))))
              case _ => flatMap(Lambda(px,deriveBroadcast(bx,vars++patvars(px))),
                                       deriveBroadcast(ex,vars))
           }
      case flatMap(Lambda(px,bx),ex)
        => flatMap(Lambda(px,deriveBroadcast(bx,vars++patvars(px))),
                   deriveBroadcast(ex,vars))
      case MatchE(x,cs)
        => MatchE(deriveBroadcast(x,vars),
                  cs.map{ case Case(p,cc,b)
                            => Case(p,deriveBroadcast(cc,vars++patvars(p)),
                                    deriveBroadcast(b,vars++patvars(p))) })
      case _ => apply(e,deriveBroadcast(_,vars))
    }

  /** return a distributed term that has no free vars */
  def findFactor ( e: Expr, vars: List[String] ): Option[Expr] =
    e match {
      case reduce(_,x)
        if freevars(x,Nil).intersect(vars).isEmpty && isDistributed(x)
        => if (diql_explain)
              println("Factor out "+e)
           Some(e)
      case flatMap(Lambda(p,b),x)
        => findFactor(x,vars) orElse findFactor(b,patvars(p)++vars)
      case MatchE(x,cs)
        => findFactor(x,vars) orElse
              cs.foldLeft[Option[Expr]](None){
                  case (r,Case(p,_,b)) => r orElse findFactor(b,patvars(p)++vars)
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
                  case (r,Case(p,_,b)) => r orElse getFactor(b,patvars(p)++vars)
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
           if (diql_explain)
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
      case flatMap(Lambda(p,b@IfE(cc,y,Empty())),x)
        => splitPredicate(cc,Nil,patvars(p)) match {
             case Some((c1,c2))
               => if (diql_explain)
                     println("Pull predicate "+c1+" outside flatMap")
                  optimize(IfE(c1,flatMap(Lambda(p,IfE(c2,y,Empty())),x),Empty()))
             case _ => flatMap(Lambda(p,optimize(b)),optimize(x))
           }
      case flatMap(Lambda(p@TuplePat(List(_,TuplePat(List(xs,ys)))),
                          b@flatMap(Lambda(px,flatMap(Lambda(py,IfE(cc,u,Empty())),_ys)),_xs)),
                   coGroup(x,y))
        if _xs == toExpr(xs) && _ys == toExpr(ys)
        => splitPredicate(cc,patvars(px),patvars(py)) match {
             case Some((c1,c2))
               => val nv = newvar
                  val nk = newvar
                  if (diql_explain)
                     println("Push predicate "+c1+" on cogroup left")
                  val nx = flatMap(Lambda(TuplePat(List(VarPat(nk),NamedPat(nv,px))),
                                          IfE(c1,Elem(Tuple(List(Var(nk),Var(nv)))),
                                              Empty())),
                                   x)
                  clean(nx)   // remove type information
                  optimize(flatMap(Lambda(p,flatMap(Lambda(px,flatMap(Lambda(py,IfE(c2,u,Empty())),_ys)),_xs)),
                                   coGroup(nx,y)))
             case _ => splitPredicate(cc,patvars(py),patvars(px)) match {
                          case Some((c1,c2))
                            => val nv = newvar
                               val nk = newvar
                               if (diql_explain)
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
      case _ => apply(e,optimize)
  }

  /** find all reduce(m,v) in e and replace them with new variables
    * to locate all aggregations after groupBy */
  def pullReduces ( e: Expr ): Expr = {
      def all_reduces ( e: Expr, v: String ): List[Expr] =
        e match {
          case reduce(_,flatMap(_,Var(s)))
            if s == v
            => List(e)
          case _
            => accumulate[List[Expr]](e,all_reduces(_,v),_++_,Nil)
      }
    def has_reduces ( e: Expr, v: String ): Boolean =
      e match {
        case reduce(_,flatMap(_,Var(s)))
          => s == v
        case _
          => accumulate[Boolean](e,has_reduces(_,v),_||_,false)
      }
    e match {
      case flatMap(Lambda(_,MatchE(reduce(_,_),_)),_)
        => apply(e,pullReduces)
      case flatMap(Lambda(p@TuplePat(List(_,VarPat(v))),b),
                   groupBy(x))
        if has_reduces(b,v)
        => val es = all_reduces(b,v)
           val binds = es.map((newvar,_)).toMap
           val nb = binds.foldLeft(b){ case (r,(w,z)) => subst(z,Var(w),r) }
           if (occurrences(v,nb) > 0)
              apply(e,pullReduces)
           else if (es.length == 1) {
             val (w,u) = binds.toList.head
             flatMap(Lambda(p,MatchE(u,List(Case(VarPat(w),BoolConst(true),nb)))),
                     groupBy(x))
           } else {
             val s = binds.map {
                         case (_,reduce(md,flatMap(Lambda(q,Elem(z)),_))) => (md,q,z)
                         case _ => throw new Error("Error in pullReduces")
                     }.toList
             val m = ProductMonoid(s.map(_._1))
             val vars = TuplePat(binds.keys.map(VarPat).toList)
             val w = newvar
             val f = Elem(Tuple(s.map{ case (_,q,z) => MatchE(Var(w),List(Case(q,BoolConst(true),z))) }))
             flatMap(Lambda(p,MatchE(reduce(m,flatMap(Lambda(VarPat(w),f),Var(v))),
                                     List(Case(vars,BoolConst(true),nb)))),
                     groupBy(x))
           }
      case _ => apply(e,pullReduces)
    }
  }

  def optimizeAll ( e: Expr, env: Environment ): Expr = {
    var olde = e
    var ne = pullOutFactors(e)
    do { olde = ne
         ne = Normalizer.normalizeAll(deriveJoins(ne,Nil))
         if (olde != ne)
            typecheck(ne,env)
       } while (olde != ne)
    do { olde = ne
         ne = Normalizer.normalizeAll(deriveCrossProducts(ne,Nil))
         if (olde != ne)
            typecheck(ne,env)
       } while (olde != ne)
    do { olde = ne
         ne = deriveBroadcast(ne,Nil) // Needed for nested loops; needs more work
         ne = pullReduces(ne)
         ne = optimize(ne)
         ne = Normalizer.normalizeAll(ne)
       } while (olde != ne)
    typecheck(ne,env)
    ne
  }
}
