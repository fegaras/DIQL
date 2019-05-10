/*
 * Copyright © 2019 University of Texas at Arlington
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
package edu.uta.diablo

object Optimizer {
  import AST._
  import Normalizer._
  import Translator.bag

  /* general span for comprehensions; if a qualifier matches, split there and continue with cont */
  def matchQ ( qs: List[Qualifier], filter: Qualifier => Boolean,
               cont: List[Qualifier] => Option[List[Qualifier]] ): Option[List[Qualifier]]
    = qs match {
        case q::r
          if filter(q)
          => cont(qs) match {
               case r@Some(s) => r
               case _ => matchQ(r,filter,cont)
             }
        case _::r
          => matchQ(r,filter,cont)
        case _ => None
      }

  val inverses = Map( "+"->"-", "*"->"/", "-"->"+", "/"->"*" )

  /* for dst=e(src), find g such that src=f(dst) */
  def inverse ( e: Expr, src: String, dst: Expr ): Option[Expr]
    = e match {
        case Var(v)
          if v==src
          => Some(dst)
        case Call(op,List(u,c))
          if (inverses.contains(op) && constantKey(c))
          => inverse(u,src,Call(inverses(op),List(dst,c)))
        case Call(op,List(c,u))
          if (inverses.contains(op) && constantKey(c))
          => inverse(u,src,Call(inverses(op),List(dst,c)))
        case _ => None
      }

  /* matches ...,i<-range(...),...,p<-e,...,v==i,... where p contains v */
  def findRangeGen ( qs: List[Qualifier] ): Option[List[Qualifier]]
    = matchQ(qs,{ case Generator(_,Call("range",_)) => true; case _ => false },
                { case (ig@Generator(VarPat(index),Call("range",_)))::r
                    => matchQ(r,{ case Generator(_,_) => true; case _ => false },
                                { case (g@Generator(p,_))::s
                                    => matchQ(s,{ case Predicate(Call("==",List(Var(v),ie)))
                                                    => patvars(p).contains(v) &&
                                                       inverse(ie,index,Var(v)).nonEmpty
                                                  case _ => false },
                                              { case c::_
                                                  => Some(List(ig,g,c))
                                                case _ => None })
                                  case _ => None })
                  case _ => None })

  /* finds a sequence of predicates in qs that imply x=y */
  def findEqPred ( x: String, y: String, qs: List[Qualifier] ): Option[List[Qualifier]]
    = matchQ(qs,{ case Predicate(Call("==",List(Var(v1),Var(v2))))
                    => v1==x || v1==y || v2==x || v2==y
                  case _ => false },
                { case (p@Predicate(Call("==",List(Var(v1),Var(v2)))))::s
                    => (if ((v1==x && v2==y) || (v2==x && v1==y))
                           Some(Nil)
                        else if (v1==x) findEqPred(v2,y,s)
                        else if (v1==y) findEqPred(x,v2,s)
                        else if (v2==x) findEqPred(v1,y,s)
                        else findEqPred(x,v1,s)).map(p::_)
                  case _ => None })

  /* matches ...,(i1,x1)<-e,...,(i2,x2)<-e,...,i1=i2,...
   * or      ...,((i1,j1),x1)<-e,...,((i2,j2),x2)<-e,...,i1=i2,...,j1=j2,...   */
  def findEqGen ( qs: List[Qualifier] ): Option[List[Qualifier]]
    = matchQ(qs,{ case Generator(_,x) if isArray(x) => true; case _ => false },
                { case (g1@Generator(TuplePat(List(VarPat(i1),_)),x))::r
                    => matchQ(r,{ case Generator(_,y) if x==y => true; case _ => false },
                                { case (g2@Generator(TuplePat(List(VarPat(i2),_)),y))::s
                                    => for { p <- findEqPred(i1,i2,s)
                                           } yield g1::g2::p
                                  case _ => None })
                  case (g1@Generator(TuplePat(List(TuplePat(List(VarPat(i1),VarPat(j1))),_)),x))::r
                    => matchQ(r,{ case Generator(_,y) if x==y => true; case _ => false },
                                { case (g2@Generator(TuplePat(List(TuplePat(List(VarPat(i2),VarPat(j2))),_)),x))::s
                                    => for { p1 <- findEqPred(i1,i2,s)
                                             p2 <- findEqPred(j1,j2,s)
                                           } yield g1::g2::(p1++p2)
                                  case _ => None })
                  case _ => None })

  def isArray ( e: Expr ): Boolean
    = e.tpe match {
        case ParametricType("vector",_) => true
        case ParametricType("matrix",_) => true
        case _ => false
      }

  /* true if the group-by key is a constant; then there will be just one group */
  def constantKey ( key: Expr ): Boolean
    = key match {
         case Tuple(ts) => ts.forall(constantKey)
         case IntConst(_) => true
         case LongConst(_) => true
         case DoubleConst(_) => true
         case BoolConst(_) => true
         case CharConst(_) => true
         case StringConst(_) => true
         case _ => false
      }

  /* true if the group-by key is unique, then the groups are singletons */
  def uniqueKey ( key: Expr, qs: List[Qualifier] ): Boolean = {
     val is = qs.takeWhile(!_.isInstanceOf[GroupByQual]).flatMap {
                  case Generator(VarPat(i),Call("range",_))
                    => List(i)
                  case Generator(TuplePat(List(pi,pv)),e)
                    if isArray(e)
                    => patvars(pi)
                  case Generator(_,_)
                    => return false
                  case _ => Nil
              }
     def comps ( k: Expr ): List[String]
       = k match {
            case Tuple(ts) => ts.flatMap(comps)
            case Var(i) => List(i)
            case Project(u,_) => comps(u)
            case Nth(u,_) => comps(u)
            case Call(op,List(u,c))
              if (Seq("+","-","*").contains(op) && constantKey(c))
              => comps(u)
            case Call(op,List(c,u))
              if (Seq("+","-","*").contains(op) && constantKey(c))
              => comps(u)
            case _ => List("%") // will fail to match
         }
     comps(key).toSet.equals(is.toSet)
  }

  private def replace[T] ( x: T, y: T, s: List[T] )
    = s.map{ i => if (i == x) y else i }

  var QLcache: Option[List[Qualifier]] = None

  def optimize ( e: Expr ): Expr =
    e match {
      case Comprehension(m,h,qs)
        if { QLcache = findRangeGen(qs); QLcache.nonEmpty }
        => // eliminate a range generator
           QLcache match {
             case Some(List( ig@Generator(VarPat(i),Call("range",List(n1,n2,n3))),
                             g@Generator(p,u),
                             c@Predicate(Call("==",List(Var(v),ie))) ))
                => val m1 = subst(i,n1,ie)
                   val m2 = subst(i,n2,ie)
                   val m13 = subst(i,Call("+",List(n1,n3)),ie)
                   val m3 = Call("/",List(Call("-",List(m13,m1)),n3))
                   val gs = List(Generator(p,u),
                                 LetBinding(VarPat(i),inverse(ie,i,Var(v)).get),
                                 Predicate(Call("inRange",List(Var(v),m1,m2,m3))))
                   val nqs = qs.diff(List(ig,c)).flatMap( x => if (x==g) gs else List(x))
                   optimize(Comprehension(m,h,nqs))
             case _ => apply(e,optimize)
           }
      case Comprehension(m,h,qs)
        if { QLcache = findEqGen(qs); QLcache.nonEmpty }
        => // eliminate duplicate generators over arrays that have equal index value
           QLcache match {
             case Some( (g1@Generator(p1,_))::(g2@Generator(p2,_))::c )
               => val nqs = replace(g2,LetBinding(p2,toExpr(p1)),qs)
                  optimize(Comprehension(m,h,nqs))
             case _ => apply(e,optimize)
           }
      case Comprehension(m,h,qs)
        => qs.span{ case GroupByQual(p,k) if constantKey(k) => false; case _ => true } match {
              case (r,GroupByQual(VarPat(k),u)::s)
                => // a group-by on a constant key can be eliminated
                   val vs = comprVars(r).map(v => LetBinding(VarPat(v),Comprehension(bag,Var(v),r)))
                   val bs = LetBinding(VarPat(k),u)::vs
                   Comprehension(m,h,bs++s)
              case _
                => qs.span{ case GroupByQual(p,k) if uniqueKey(k,qs) => false; case _ => true } match {
                      case (r,GroupByQual(VarPat(k),u)::s)
                        => // a group-by on a unique key can be eliminated after lifting each var v to {v}
                           val vs = comprVars(r).map(v => LetBinding(VarPat(v),Elem(bag,Var(v))))
                           val bs = LetBinding(VarPat(k),u)+:vs
                           Comprehension(m,h,r++bs++s)
                      case _ => apply(e,optimize)
                   }
           }
      case _ => apply(e,optimize)
    }

  def movePredicates ( qs: List[Qualifier] ): List[Qualifier]
    = qs match {
        case (p@Predicate(_))::r
          => movePredicates(r):+p
        case q::r
          => q::movePredicates(r)
        case Nil => Nil
      }

  def movePredicates ( e: Expr ): Expr
    = e match {
        case Comprehension(m,h,qs)
          => qs.span{ case GroupByQual(_,_) => false; case _ => true } match {
               case (r,GroupByQual(p,u)::s)
                 => val Comprehension(_,_,ss) = movePredicates(Comprehension(m,h,s))
                    Comprehension(m,movePredicates(h),
                                  movePredicates(r)++(GroupByQual(p,movePredicates(u))+:ss))
               case _ => Comprehension(m,movePredicates(h),movePredicates(qs))
             }
        case _ => apply(e,movePredicates)
      }

  def optimizeAll ( e: Expr ): Expr = {
    var olde = e
    var ne = movePredicates(olde)
    do { olde = ne
         ne = normalizeAll(optimize(ne))
       } while (olde != ne)
    ne
  }
}
