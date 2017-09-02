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

import scala.util.parsing.input.Positional

sealed abstract class Type
    case class TupleType ( components: List[Type] ) extends Type
    case class ParametricType ( name: String, components: List[Type] ) extends Type
    case class BasicType ( name: String ) extends Type

sealed abstract class Pattern ( var tpe: Any = null )
    case class TuplePat ( components: List[Pattern] ) extends Pattern
    case class NamedPat ( name: String, pat: Pattern ) extends Pattern
    case class CallPat ( name: String, args: List[Pattern] ) extends Pattern
    case class MethodCallPat ( obj: Pattern, method: String, args: List[Pattern] ) extends Pattern
    case class StringPat ( value: String ) extends Pattern {
         override def toString: String = "StringPat(\""+value+"\")"
    }
    case class CharPat ( value: Char ) extends Pattern
    case class IntPat ( value: Int ) extends Pattern
    case class LongPat ( value: Long ) extends Pattern
    case class DoublePat ( value: Double ) extends Pattern
    case class BooleanPat ( value: Boolean ) extends Pattern
    case class VarPat ( name: String ) extends Pattern
    case class RestPat ( name: String ) extends Pattern
    case class StarPat () extends Pattern

sealed abstract class Qualifier
    case class Generator ( pattern: Pattern, domain: Expr ) extends Qualifier
    case class LetBinding ( pattern: Pattern, domain: Expr ) extends Qualifier
    case class Predicate ( predicate: Expr ) extends Qualifier

case class CoGroupQual ( qualifiers: List[Qualifier], pattern: Pattern, key: Expr )
case class GroupByQual ( pattern: Pattern, key: Expr, having: Expr, cogroup: Option[CoGroupQual]  )
case class OrderByQual ( key: Expr )

case class Case ( pat: Pattern, condition: Expr, body: Expr )

sealed abstract class Expr ( var tpe: Any = null ) extends Positional  // tpe contains type information
    case class flatMap ( function: Lambda, input: Expr ) extends Expr
    case class groupBy ( input: Expr ) extends Expr
    case class orderBy ( input: Expr ) extends Expr
    case class coGroup ( left: Expr, right: Expr ) extends Expr
    case class cross ( left: Expr, right: Expr ) extends Expr
    case class reduce ( monoid: String, input: Expr ) extends Expr
    case class repeat ( function: Lambda, init: Expr, condition: Lambda, n: Expr ) extends Expr
    case class SelectQuery ( output: Expr, qualifiers: List[Qualifier],
                             groupBy: Option[GroupByQual],
                             orderBy: Option[OrderByQual] ) extends Expr
    case class SelectDistQuery ( output: Expr, qualifiers: List[Qualifier],
                                 groupBy: Option[GroupByQual],
                                 orderBy: Option[OrderByQual] ) extends Expr
    case class SomeQuery ( output: Expr, qualifiers: List[Qualifier] ) extends Expr
    case class AllQuery ( output: Expr, qualifiers: List[Qualifier] ) extends Expr
    case class SmallDataSet ( input: Expr ) extends Expr
    case class Lambda ( pattern: Pattern, body: Expr ) extends Expr
    case class TypedLambda ( args: List[(String,Type)], body: Expr ) extends Expr
    case class Call ( name: String, args: List[Expr] ) extends Expr
    case class Constructor ( name: String, args: List[Expr] ) extends Expr
    case class MethodCall ( obj: Expr, method: String, args: List[Expr] ) extends Expr
    case class IfE ( predicate: Expr, thenp: Expr, elsep: Expr ) extends Expr
    case class Tuple ( args: List[Expr] ) extends Expr
    case class MatchE ( expr: Expr, cases: List[Case] ) extends Expr
    case class Nth ( base: Expr, num: Int ) extends Expr
    case class Empty () extends Expr
    case class Elem ( elem: Expr ) extends Expr
    case class Merge ( left: Expr, right: Expr ) extends Expr
    case class Var ( name: String ) extends Expr
    case class StringConst ( value: String ) extends Expr {
         override def toString: String = "StringConst(\""+value+"\")"
    }
    case class CharConst ( value: Char ) extends Expr
    case class IntConst ( value: Int ) extends Expr
    case class LongConst ( value: Long ) extends Expr
    case class DoubleConst ( value: Double ) extends Expr
    case class BoolConst ( value: Boolean ) extends Expr


object AST {

  private var count = 0

  /** return a fresh variable name */
  def newvar = { count = count+1; "diql$"+count }

  /** apply f to every pattern in p */
  def apply ( p: Pattern, f: Pattern => Pattern ): Pattern =
    p match {
      case TuplePat(ps) => TuplePat(ps.map(f(_)))
      case NamedPat(n,p) => NamedPat(n,f(p))
      case CallPat(n,ps) => CallPat(n,ps.map(f(_)))
      case MethodCallPat(o,m,ps) => MethodCallPat(f(o),m,ps.map(f(_)))
      case _ => p
    }

  /** apply f to every qualifier in q */
  def apply ( q: Qualifier, f: Expr => Expr ): Qualifier =
    q match {
      case Generator(p,x) => Generator(p,f(x))
      case LetBinding(p,x) => LetBinding(p,f(x))
      case Predicate(x) => Predicate(f(x))
    }

  /** apply f to every expression in e */
  def apply ( e: Expr, f: Expr => Expr ): Expr =
    { val res = e match {
      case flatMap(Lambda(p,b),x)
        => flatMap(Lambda(p,f(b)),f(x))
      case groupBy(x) => groupBy(f(x))
      case orderBy(x) => orderBy(f(x))
      case coGroup(x,y) => coGroup(f(x),f(y))
      case cross(x,y) => cross(f(x),f(y))
      case reduce(m,x) => reduce(m,f(x))
      case repeat(Lambda(p,b),x,Lambda(pp,w),n)
        => repeat(Lambda(p,f(b)),f(x),Lambda(pp,f(w)),f(n))
      case SelectQuery(o,qs,gb,ob)
        => SelectQuery(f(o),qs.map(apply(_,f)),
                       gb match { case Some(GroupByQual(p,k,h,None))
                                    => Some(GroupByQual(p,f(k),f(h),None))
                                  case Some(GroupByQual(p,k,h,Some(CoGroupQual(qs2,p2,k2))))
                                    => Some(GroupByQual(p,f(k),f(h),
                                                Some(CoGroupQual(qs2.map(apply(_,f)),p2,f(k2)))))
                                  case x => x },
                       ob match { case Some(OrderByQual(k))
                                        => Some(OrderByQual(f(k)))
                                  case x => x })
      case SelectDistQuery(o,qs,gb,ob)
        => SelectDistQuery(f(o),qs.map(apply(_,f)),
                       gb match { case Some(GroupByQual(p,k,h,None))
                                    => Some(GroupByQual(p,f(k),f(h),None))
                                  case Some(GroupByQual(p,k,h,Some(CoGroupQual(qs2,p2,k2))))
                                    => Some(GroupByQual(p,f(k),f(h),
                                                Some(CoGroupQual(qs2.map(apply(_,f)),p2,f(k2)))))
                                  case x => x },
                           ob match { case Some(OrderByQual(k))
                                        => Some(OrderByQual(f(k)))
                                      case x => x })
      case SomeQuery(o,qs) => SomeQuery(f(o),qs.map(apply(_,f)))
      case AllQuery(o,qs) => AllQuery(f(o),qs.map(apply(_,f)))
      case SmallDataSet(x) => SmallDataSet(f(x))
      case Lambda(p,b) => Lambda(p,f(b))
      case TypedLambda(args,b) => TypedLambda(args,f(b))
      case Call(n,es) => Call(n,es.map(f(_)))
      case Constructor(n,es) => Constructor(n,es.map(f(_)))
      case MethodCall(o,m,null) => MethodCall(f(o),m,null)
      case MethodCall(o,m,es) => MethodCall(f(o),m,es.map(f(_)))
      case IfE(p,x,y) => IfE(f(p),f(x),f(y))
      case MatchE(e,cs) => MatchE(f(e),cs.map{ case Case(p,c,b) => Case(p,f(c),f(b)) })
      case Tuple(es) => Tuple(es.map(f(_)))
      case Nth(x,n) => Nth(f(x),n)
      case Elem(x) => Elem(f(x))
      case Merge(x,y) => Merge(f(x),f(y))
      case _ => e
    }
    res.tpe = e.tpe   // copy type information as is
    res
    }

  /** fold over patterns */
  def accumulatePat[T] ( p: Pattern, f: Pattern => T, acc: (T,T) => T, zero: T ): T =
    p match {
      case TuplePat(ps) => ps.map(f(_)).fold(zero)(acc)
      case NamedPat(n,p) => f(p)
      case CallPat(n,ps) => ps.map(f(_)).fold(zero)(acc)
      case MethodCallPat(o,m,ps) => ps.map(f(_)).fold(f(o))(acc)
      case _ => zero
    }

  /** fold over qualifiers */
  def accumulateQ[T] ( q: Qualifier, f: Expr => T, acc: (T,T) => T, zero: T ): T =
    q match {
      case Generator(p,x) => f(x)
      case LetBinding(p,x) => f(x)
      case Predicate(x) => f(x)
    }

  /** fold over expressions */
  def accumulate[T] ( e: Expr, f: Expr => T, acc: (T,T) => T, zero: T ): T =
    e match {
      case flatMap(b,x) => acc(f(b),f(x))
      case groupBy(x) => f(x)
      case orderBy(x) => f(x)
      case coGroup(x,y) => acc(f(x),f(y))
      case cross(x,y) => acc(f(x),f(y))
      case reduce(m,x) => f(x)
      case repeat(b,x,w,n) => acc(acc(f(b),acc(f(w),f(x))),f(n))
      case SelectQuery(o,qs,gb,ob)
        => acc(qs.map(accumulateQ(_,f,acc,zero)).fold(f(o))(acc),
               acc(gb match { case Some(GroupByQual(p,k,h,None))
                                => acc(f(k),f(h))
                              case Some(GroupByQual(p,k,h,Some(CoGroupQual(qs2,p2,k2))))
                                => acc(f(k),acc(f(h),
                                         qs2.map(accumulateQ(_,f,acc,zero)).fold(f(k2))(acc)))
                              case x => zero },
                   ob match { case Some(OrderByQual(k))
                                => f(k)
                              case x => zero }))
      case SelectDistQuery(o,qs,gb,ob)
        => acc(qs.map(accumulateQ(_,f,acc,zero)).fold(f(o))(acc),
               acc(gb match { case Some(GroupByQual(p,k,h,None))
                                => acc(f(k),f(h))
                              case Some(GroupByQual(p,k,h,Some(CoGroupQual(qs2,p2,k2))))
                                => acc(f(k),acc(f(h),
                                         qs2.map(accumulateQ(_,f,acc,zero)).fold(f(k2))(acc)))
                              case x => zero },
                   ob match { case Some(OrderByQual(k))
                                => f(k)
                              case x => zero }))
      case SomeQuery(o,qs)
        => qs.map(accumulateQ(_,f,acc,zero)).fold(f(o))(acc)
      case AllQuery(o,qs)
        => qs.map(accumulateQ(_,f,acc,zero)).fold(f(o))(acc)
      case SmallDataSet(x) => f(x)
      case Lambda(p,b) => f(b)
      case TypedLambda(p,b) => f(b)
      case Call(n,es) => es.map(f(_)).fold(zero)(acc)
      case Constructor(n,es) => es.map(f(_)).fold(zero)(acc)
      case MethodCall(o,m,null) => f(o)
      case MethodCall(o,m,es) => es.map(f(_)).fold(f(o))(acc)
      case IfE(p,x,y) => acc(f(p),acc(f(x),f(y)))
      case MatchE(e,cs) => cs.map{ case Case(p,c,b) => acc(f(c),f(b)) }.fold(f(e))(acc)
      case Tuple(es) => es.map(f(_)).fold(zero)(acc)
      case Nth(x,n) => f(x)
      case Elem(x) => f(x)
      case Merge(x,y) => acc(f(x),f(y))
      case _ => zero
    }

  /** return the list of all variables in the pattern p */
  def patvars ( p: Pattern ): List[String] = 
    p match {
      case VarPat(s) => List(s)
      case RestPat(s) if (s != "_") => List(s)
      case NamedPat(n,p) => n::patvars(p)
      case _ => accumulatePat[List[String]](p,patvars(_),_++_,Nil)
    }

  /** true if the variable v is captured in the pattern p */
  def capture ( v: String, p: Pattern ): Boolean =
    p match {
      case VarPat(s) => v==s
      case RestPat(s) => v==s
      case _ => accumulatePat[Boolean](p,capture(v,_),_||_,false)
    }

  /** beta reduction: substitute every occurrence of variable v in e with te */
  def subst ( v: String, te: Expr, e: Expr ): Expr =
    e match {
      case flatMap(Lambda(p,b),x) if capture(v,p)
        => flatMap(Lambda(p,b),subst(v,te,x))
      case MatchE(expr,cs)
        => MatchE(subst(v,te,expr),
                  cs.map{ case cp@Case(p,c,b)
                            => if (capture(v,p)) cp
                               else Case(p,subst(v,te,c),subst(v,te,b)) })
      case lp@Lambda(p,b) if capture(v,p) => lp
      case lp@TypedLambda(args,b) if args.map(x => capture(v,VarPat(x._1))).reduce(_||_) => lp
      case Var(s) => if (s==v) te else e
      case _ => apply(e,subst(v,te,_))
    }

  /** substitute every occurrence of term 'from' in pattern p with 'to' */
  def subst ( from: String, to: String, p: Pattern ): Pattern =
    p match {
      case VarPat(s) if s==from => VarPat(to)
      case NamedPat(n,p) if n==from => NamedPat(to,p)
      case _ => apply(p,subst(from,to,_))
  }

  /** substitute every occurrence of the term 'from' in e with 'to' */
  def subst ( from: Expr, to: Expr, e: Expr ): Expr =
    if (e == from) to else apply(e,subst(from,to,_))

  /** number of times the variable v is accessed in e */
  def occurrences ( v: String, e: Expr ): Int =
    e match {
      case Var(s) => if (s==v) 1 else 0
      case flatMap(Lambda(p,b),x) if capture(v,p)
        => occurrences(v,x)
      case repeat(f,init,p,n)   // assume loop is executed 10 times
        => occurrences(v,f)*10+occurrences(v,init)+occurrences(v,n)+occurrences(v,p)*10
      case MatchE(expr,cs)
        => if (occurrences(v,expr) > 0)
              10  // if v gets pattern-matched, assume its components are used 10 times 
           else cs.map{ case Case(p,c,b)
                          => if (capture(v,p)) 0
                             else occurrences(v,c)+occurrences(v,b) }
                  .reduce(_+_)
      case Lambda(p,b) if capture(v,p) => 0
      case TypedLambda(args,b) if args.map(x => capture(v,VarPat(x._1))).reduce(_||_) => 0
      case _ => accumulate[Int](e,occurrences(v,_),_+_,0)
    }

  /** number of times the variables in vs are accessed in e */
  def occurrences ( vs: List[String], e: Expr ): Int
    = vs.map(occurrences(_,e)).reduce(_+_)

  /** return the list of free variables in e that do not appear in except */
  def freevars ( e: Expr, except: List[String] ): List[String] =
    e match {
      case Var(s)
        => if (except.contains(s)) Nil else List(s)
      case flatMap(Lambda(p,b),x)
        => freevars(b,except++patvars(p))++freevars(x,except)
      case MatchE(expr,cs)
        => cs.map{ case Case(p,c,b)
                     => val pv = except++patvars(p)
                        freevars(b,pv)++freevars(c,pv)
                     }.fold(freevars(expr,except))(_++_)
      case Lambda(p,b)
        => freevars(b,except++patvars(p))
      case TypedLambda(args,b)
        => freevars(b,except++args.map(_._1))
      case _ => accumulate[List[String]](e,freevars(_,except),_++_,Nil)
    }

  /** return the list of free variables in e */
  def freevars ( e: Expr ): List[String] = freevars(e,Nil)

  /** remove all type information from e */
  def clean ( e: Expr ): Int = {
    e.tpe = null
    accumulate[Int](e,clean(_),_+_,0)
  }

  /** Convert a pattern to an expression */
  def toExpr ( p: Pattern ): Expr
      = p match {
        case TuplePat(ps) => Tuple(ps.map(toExpr))
        case VarPat(n) => Var(n)
        case NamedPat("_",p) => toExpr(p)
        case NamedPat(n,p) => Var(n)
        case StringPat(s) => StringConst(s)
        case CharPat(s) => CharConst(s)
        case LongPat(n) => LongConst(n)
        case DoublePat(n) => DoubleConst(n)
        case BooleanPat(n) => BoolConst(n)
        case CallPat(s,ps) => Call(s,ps.map(toExpr))
        case MethodCallPat(p,m,null) => MethodCall(toExpr(p),m,null)
        case MethodCallPat(p,m,ps) => MethodCall(toExpr(p),m,ps.map(toExpr))
        case _ => Tuple(Nil)
      }
}
