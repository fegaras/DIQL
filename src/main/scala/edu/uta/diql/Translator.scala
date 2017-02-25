package edu.uta.diql

object Translator {
  import AST._

  /** Collect all pattern variables into a list */
  def pv ( p: Pattern, except: List[String] ): List[String] =
    p match {
      case VarPat(s) if (s != "_" && !except.contains(s)) => List(s)
      case RestPat(s) if (s != "_" && !except.contains(s)) => List(s)
      case NamedPat(n,p) if !except.contains(n) => n::pv(p,except)
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
             cMap(Lambda(p,ne),te)
        case LetBinding(p,e)+:ns
          => MatchE(translate(e),List(Case(p,BoolConst(true),
                                           translateQualifiers(result,ns))))
        case Predicate(e)+:ns
          => IfE(translate(e),translateQualifiers(result,ns),Empty())
      }

  /** Translate select-queries to the algebra */
  def translate ( e: Expr ): Expr =
    e match {
      case SelectDistQuery(out,qs,gb,ob)
        => val nv = newvar
           val mv = newvar
           cMap(Lambda(TuplePat(List(VarPat(mv),StarPat())),Elem(Var(mv))),
                groupBy(cMap(Lambda(VarPat(nv),Elem(Tuple(List(Var(nv),IntConst(0))))),
                             translate(SelectQuery(out,qs,gb,ob)))))
      case SelectQuery(out,qs,gb,Some(OrderByQual(k)))
        => orderBy(translate(SelectQuery(Tuple(List(k,out)),qs,gb,None)))
      case SelectQuery(out,qs,Some(GroupByQual(p,k,h)),None)
        => val groupByVars = pv(p,List())
           val liftedVars = qs.flatMap(q => qv(q,groupByVars))
           val lp = TuplePat(liftedVars.map(VarPat))
           val s = newvar
           val liftedBinds = liftedVars.foldRight(Elem(translate(out)):Expr) {
                                     case (v,r) => subst(v,cMap(Lambda(lp,Elem(Var(v))),
                                                                Var(s)),
                                                         r) }
           cMap(Lambda(TuplePat(List(p,VarPat(s))),
                       liftedBinds),
                groupBy(translate(SelectQuery(Tuple(List(k,Tuple(liftedVars.map(Var(_))))),
                                              qs,None,None))))
      case SelectQuery(out,qs,None,None)
        => translateQualifiers(out,qs)
      case SomeQuery(out,qs)
        => reduce("||",
                  qs.foldRight(IfE(translate(out),Elem(BoolConst(true)),Empty()):Expr) {
                        case (Generator(p,e),r) => cMap(Lambda(p,r),translate(e))
                        case (LetBinding(p,e),r)
                          => MatchE(translate(e),List(Case(p,BoolConst(true),r)))
                        case (Predicate(e),r) => IfE(translate(e),r,Empty())
                  })
      case AllQuery(out,qs)
        => reduce("&&",
                  qs.foldRight(Elem(translate(out)):Expr) {
                        case (Generator(p,e),r) => cMap(Lambda(p,r),translate(e))
                        case (LetBinding(p,e),r)
                          => MatchE(translate(e),List(Case(p,BoolConst(true),r)))
                        case (Predicate(e),r) => IfE(translate(e),r,Empty())
                  })
      case MethodCall(Var(a),"/",List(x))
        if monoids.contains(a)
        => translate(reduce(a,x))
      case MethodCall(x,"union",List(y))
        => Merge(translate(x),translate(y))
      case reduce("count",x)
        => reduce("+",cMap(Lambda(StarPat(),Elem(LongConst(1L))),
                           translate(x)))
      case reduce("avg",x)
        => val nv = newvar
           MethodCall(reduce("avg_combine",cMap(Lambda(VarPat(nv),Elem(Call("Avg",List(Var(nv),LongConst(1L))))),
                                                translate(x))),"value",null)
      case _ => apply(e,translate(_))
    }
}
