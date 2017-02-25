package edu.uta.diql

import scala.reflect.macros.whitebox.Context

object Optimizer {
  import AST._

  /** true if e is a distributed dataset that doesn't contain any variables from vars */ 
  def is_distributed ( e: Expr, vars: List[String] ): Boolean
    = CodeGeneration.distr(e) && freevars(e).intersect(vars).isEmpty

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
  def joinPredicate ( e: Expr, xs: List[String], ys: List[String] ): Option[(Expr,Expr)] =
    e match {
      case IfE(pred,et,Empty())
        => joinCondition(pred,xs,ys) match {
              case Some((k1,k2)) => Some((k1,k2))
              case _ => joinPredicate(et,xs,ys)
           }
      case MatchE(ey,List(Case(p,BoolConst(true),b)))
        => joinCondition(b,xs,ys++patvars(p)) match {
             case Some((k1,k2)) => Some((k1,MatchE(ey,List(Case(p,BoolConst(true),k2)))))
             case None => joinPredicate(ey,xs,ys)
           }
      case cMap(Lambda(p,b),u)
        => joinPredicate(b,xs,ys)
      case _ => accumulate[Option[(Expr,Expr)]](e,joinPredicate(_,xs,ys),
                          { case (x@Some(_),_) => x; case (_,y) => y }, None )
    }

  /** find the left input of the equi-join, if any */
  def findJoinMatch ( e: Expr, xs: List[String] ): Option[(Expr,Expr,Expr=>Expr,Expr)] =
    e match {
      case cMap(Lambda(py,by),ey)
        if CodeGeneration.distr(ey)
        => joinPredicate(by,xs,patvars(py)) match {
              case Some((kx,ky)) => Some((kx,ky,identity,e))
              case _ => findJoinMatch(ey,xs)
          }
      case cMap(Lambda(py,by),ey)
        => if (freevars(ey).intersect(xs).isEmpty)
              findJoinMatch(by,xs)
           else findJoinMatch(by,xs++patvars(py)) match {
                        case Some((kx,ky,pre,post))
                          => Some((kx,ky,x => cMap(Lambda(py,pre(x)),ey),post))
                        case _ => None
                   }
      case MatchE(ey,List(Case(p,BoolConst(true),b)))
        if freevars(ey).intersect(xs).nonEmpty
        => findJoinMatch(b,xs++patvars(p)) match {
                        case Some((kx,ky,pre,post))
                          => Some((kx,ky,x => MatchE(ey,List(Case(p,BoolConst(true),pre(x)))),post))
                        case _ => None
                   }
      case _ => accumulate[Option[(Expr,Expr,Expr=>Expr,Expr)]](e,findJoinMatch(_,xs),
                          { case (x@Some(_),_) => x; case (_,y) => y },None)
    }

  /** find equi-joins between datasets in e and convert them to coGroup */
  def deriveJoins ( e: Expr ): Expr =
    e match {
      case cMap(Lambda(px,bx),ex)
        if is_distributed(ex,Nil)
        => findJoinMatch(bx,patvars(px)) match {
              case Some((kx,ky,pre,cmy@cMap(Lambda(py,by),ey)))
                => { val xv = newvar
                     val yv = newvar
                     val xs = newvar
                     val ys = newvar
                     val nby = subst(cmy,cMap(Lambda(py,by),Var(ys)),bx)
                     val nbb = subst(MethodCall(kx,"==",List(ky)),BoolConst(true),
                                     subst(MethodCall(ky,"==",List(kx)),BoolConst(true),nby))
                     clean(nbb)   // remove type information
                     cMap(Lambda(TuplePat(List(StarPat(),TuplePat(List(VarPat(xs),VarPat(ys))))),
                                 cMap(Lambda(px,nbb),Var(xs))),
                          coGroup(cMap(Lambda(NamedPat(xv,px),pre(Elem(Tuple(List(kx,Var(xv)))))),ex),
                                  cMap(Lambda(NamedPat(yv,py),Elem(Tuple(List(ky,Var(yv))))),ey)))
                   }
              case _ => cMap(Lambda(px,deriveJoins(bx)),deriveJoins(ex))
           }
      case _ => apply(e,deriveJoins(_))
    }

  /** find the right input of a cross product */
  def findCrossMatch ( e: Expr, vars: List[String] ): Option[Expr] =
    if (is_distributed(e,vars)) Some(e)
    else e match {
             case cMap(Lambda(px,bx),ex)
               => findCrossMatch(ex,vars) match {
                      case nex@Some(_) => nex
                      case _ => findCrossMatch(bx,vars++patvars(px))
                  }
             case MatchE(x,cs)
               => findCrossMatch(x,vars) orElse
                    cs.foldLeft[Option[Expr]](None){
                      case (r,Case(p,c,b)) => r orElse findCrossMatch(b,patvars(p)++vars)
                    }
             case _ => accumulate[Option[Expr]](e,findCrossMatch(_,vars),
                                 { case (x@Some(_),_) => x; case (_,y) => y },None)
    }

  /** find all cross products that cannot be joined with any other dataset */
  def deriveCrossProducts ( e: Expr ): Expr =
    e match {
      case cMap(Lambda(px,bx),ex)
        if CodeGeneration.distr(ex)
        => findCrossMatch(bx,patvars(px)) match {
              case Some(right)
                => val nv = newvar
                   val nbx = subst(right,Elem(Var(nv)),bx)
                   clean(nbx)
                   cMap(Lambda(TuplePat(List(px,VarPat(nv))),nbx),
                        cross(ex,right))
              case _ => cMap(Lambda(px,deriveCrossProducts(bx)),deriveCrossProducts(ex))
           }
      case _ => apply(e,deriveCrossProducts(_))
    }

  def findCommonFactor ( e: Expr, vars: List[String] ): Option[Expr] =
    e match {
      case reduce(m,x)
        if freevars(x,Nil).intersect(vars).isEmpty && CodeGeneration.distr(x)
        => Some(e)
      case cMap(Lambda(p,b),x)
        => findCommonFactor(x,vars) orElse findCommonFactor(b,patvars(p)++vars)
      case MatchE(x,cs)
        => findCommonFactor(x,vars) orElse
              cs.foldLeft[Option[Expr]](None){
                  case (r,Case(p,c,b)) => r orElse findCommonFactor(b,patvars(p)++vars)
              }
      case _ => accumulate[Option[Expr]](e,findCommonFactor(_,vars),
              { case (x@Some(_),_) => x; case (_,y) => y },None)
    }

  def pullOutCommonFactors ( e: Expr ): Expr =
    findCommonFactor(e,Nil) match {
      case Some(x)
        => val nv = newvar
           MatchE(x,List(Case(VarPat(nv),BoolConst(true),
                              pullOutCommonFactors(subst(x,Var(nv),e)))))
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
      case cMap(Lambda(p,b@IfE(c,y,Empty())),x)
        => splitPredicate(c,Nil,patvars(p)) match {
             case Some((c1,c2))
               => optimize(IfE(c1,cMap(Lambda(p,IfE(c2,y,Empty())),x),Empty()))
             case _ => cMap(Lambda(p,optimize(b)),optimize(x))
           }
      case cMap(Lambda(p@TuplePat(List(k,TuplePat(List(xs,ys)))),
                       b@cMap(Lambda(px,cMap(Lambda(py,IfE(c,e,Empty())),_ys)),_xs)),
                coGroup(x,y))
        if _xs == toExpr(xs) || _ys == toExpr(ys)
        => splitPredicate(c,patvars(px),patvars(py)) match {
             case Some((c1,c2))
               => val nv = newvar
                  val nk = newvar
                  optimize(cMap(Lambda(p,
                     cMap(Lambda(px,cMap(Lambda(py,IfE(c2,e,Empty())),_ys)),_xs)),
                          coGroup(cMap(Lambda(TuplePat(List(VarPat(nk),NamedPat(nv,px))),
                                              IfE(c1,Elem(Tuple(List(Var(nk),Var(nv)))),
                                                  Empty())),
                                       x),
                                  y)))
             case _ => splitPredicate(c,patvars(py),patvars(px)) match {
                          case Some((c1,c2))
                            => val nv = newvar
                               val nk = newvar
                               optimize(cMap(Lambda(p,
                                  cMap(Lambda(px,cMap(Lambda(py,IfE(c2,e,Empty())),_ys)),_xs)),
                                       coGroup(x,
                                               cMap(Lambda(TuplePat(List(VarPat(nk),NamedPat(nv,py))),
                                                           IfE(c1,Elem(Tuple(List(Var(nk),Var(nv)))),
                                                               Empty())),
                                                    y))))
                          case _ => cMap(Lambda(p,optimize(b)),optimize(coGroup(x,y)))
                        }
           }
      case _ => apply(e,optimize(_))
  }

  def optimizeAll (c: Context ) ( e: Expr ): Expr = {
    def rec ( c: Context ) ( e: Expr, env: Map[c.Tree,c.Tree] ): c.Tree
        = CodeGeneration.code(c)(e,env,rec(c)(_,_))
    var olde = e
    var ne = pullOutCommonFactors(e)
    do { olde = ne
         ne = Normalizer.normalizeAll(deriveJoins(ne))
         if (olde != ne)
            typecheck(c)(ne)
       } while (olde != ne)
    do { olde = ne
         ne = Normalizer.normalizeAll(deriveCrossProducts(ne))
         if (olde != ne)
            typecheck(c)(ne)
       } while (olde != ne)
    do { olde = ne
      ne = optimize(ne)
         ne = Normalizer.normalizeAll(ne)
       } while (olde != ne)
    ne
  }
}
