package edu.uta.diql

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
      case cMap(f,cMap(Lambda(p,b),x))
        => normalize(cMap(Lambda(p,cMap(renameVars(f),b)),x))
      case cMap(Lambda(p,b),Empty())
        => Empty()
      case cMap(Lambda(p,b),Elem(x))
        => normalize(MatchE(x,List(Case(p,BoolConst(true),b))))
      case cMap(f,IfE(c,e1,e2))
        => normalize(IfE(c,cMap(f,e1),cMap(f,e2)))
      case groupBy(Empty())
        => Empty()
      case groupBy(groupBy(x))
        => val nv = newvar
           val kv = newvar
           normalize(cMap(Lambda(TuplePat(List(VarPat(kv),VarPat(nv))),
                                 Elem(Tuple(List(Var(kv),Elem(Var(nv)))))),
                          groupBy(x)))
      case coGroup(x,Empty())
        => val nv = newvar
           val kv = newvar
           normalize(cMap(Lambda(TuplePat(List(VarPat(kv),VarPat(nv))),
                                 Elem(Tuple(List(Var(kv),Tuple(List(Var(nv),Empty())))))),
                          groupBy(x)))
      case coGroup(Empty(),x)
        => val nv = newvar
           val kv = newvar
           normalize(cMap(Lambda(TuplePat(List(VarPat(kv),VarPat(nv))),
                                 Elem(Tuple(List(Var(kv),Tuple(List(Empty(),Var(nv))))))),
                          groupBy(x)))
      case IfE(BoolConst(true),e1,e2)
        => normalize(e1)
      case IfE(BoolConst(false),e1,e2)
        => normalize(e2)
      case MatchE(x@Var(_),List(Case(VarPat(v),BoolConst(true),b)))
        => normalize(subst(v,x,b))
      case MatchE(x,List(Case(VarPat(v),BoolConst(true),b)))
        if occurences(v,b) <= 1
        => normalize(subst(v,x,b))
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
