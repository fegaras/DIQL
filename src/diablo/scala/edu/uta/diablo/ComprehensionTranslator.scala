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

object ComprehensionTranslator {
  import AST._
  import edu.uta.diql.core

  var datasetClassPath = ""   // must be set to DistributedCodeGenerator.datasetClassPath

  val arrayClassName = "Array"

  var datasetClass = ""

  def translate ( p: Pattern ): core.Pattern =
    p match {
      case VarPat(v)
        => core.VarPat(v)
      case TuplePat(ps)
        => core.TuplePat(ps.map(translate))
      case StarPat()
        => core.StarPat()
    }

  def translate ( m: Monoid ): core.Monoid =
    m match {
      case BaseMonoid(n)
        => core.BaseMonoid(n)
      case ProductMonoid(ms)
        => core.ProductMonoid(ms.map(translate))
      case ParametricMonoid(n,pm)
        => core.ParametricMonoid(n,translate(pm))
    }

  def translate ( tp: Type ): core.Type =
    tp match {
      case BasicType(n)
        => core.BasicType(n)
      case TupleType(ts)
        => core.TupleType(ts.map(translate))
      case RecordType(ms)
        => core.TupleType(ms.map(x => translate(x._2)).toList)
      case ParametricType("vector",List(etp))
        => core.ParametricType(datasetClass,
                               List(core.TupleType(List(core.BasicType("Long"),
                                                        translate(etp)))))
      case ParametricType("matrix",List(etp))
        => core.ParametricType(datasetClass,
                               List(core.TupleType(List(core.TupleType(List(core.BasicType("Long"),
                                                                            core.BasicType("Long"))),
                                                        translate(etp)))))
      case ParametricType("map",List(ktp,vtp))
        => core.ParametricType(datasetClass,
                               List(core.TupleType(List(translate(ktp),
                                                        translate(vtp)))))
      case ParametricType("option",cs)
        => core.ParametricType("Option",cs.map(translate))
      case ParametricType(n,cs)
        => core.ParametricType(n,cs.map(translate))
      case _ => throw new Error("Unrecognized type: "+tp)
    }

  def translate ( tp: core.Type ): Type =
    tp match {
      case core.ParametricType(ds,List(core.TupleType(List(core.BasicType("Long"),etp))))
        if ds == datasetClassPath || ds == arrayClassName
        => ParametricType("vector",List(translate(etp)))
      case core.ParametricType(ds,List(core.TupleType(List(core.TupleType(List(core.BasicType("Long"),
                                                                               core.BasicType("Long"))),etp))))
        if ds == datasetClassPath || ds == arrayClassName
        => ParametricType("matrix",List(translate(etp)))
      case core.ParametricType(ds,List(core.TupleType(List(ktp,vtp))))
        if ds == datasetClassPath || ds == arrayClassName
        => ParametricType("map",List(translate(ktp),translate(vtp)))
      case core.BasicType(n)
        => BasicType(n)
      case core.TupleType(ts)
        => TupleType(ts.map(translate))
      case core.ParametricType("Option",cs)
        => ParametricType("option",cs.map(translate))
      case core.ParametricType(n,cs)
        => ParametricType(n,cs.map(translate))
      case _ => throw new Error("Unrecognized type: "+tp)
    }

  /** Translate a sequence of query qualifiers to an expression */  
  def translateQualifiers ( m: Monoid, result: core.Expr, qs: List[Qualifier] ): core.Expr =
    qs match {
      case Nil
        => result
      case Generator(p,e)+:ns
        if (e.tpe match { case ParametricType("option",_) => true; case _ => false })
        => core.MatchE(translate(e),
                       List(core.Case(core.CallPat("Some",List(translate(p))),
                                      core.BoolConst(true),
                                      translateQualifiers(m,result,ns))))
      case Generator(p,e)+:ns
        if m == BaseMonoid("option")
        => val te = translate(e)
           val ne = translateQualifiers(BaseMonoid("bag"),result,ns)
           core.Elem(core.Call("element",List(core.flatMap(core.Lambda(translate(p),ne),te))))
      case Generator(p,e)+:ns
        => val te = translate(e)
           val ne = translateQualifiers(m,result,ns)
           core.flatMap(core.Lambda(translate(p),ne),te)
      case LetBinding(p,e)+:ns
        => core.MatchE(translate(e),
                       List(core.Case(translate(p),
                                      core.BoolConst(true),
                                      translateQualifiers(m,result,ns))))
      case Predicate(e)+:ns
        => translateQualifiers(m,core.IfE(translate(e),
                                          result,
                                          if (m == BaseMonoid("option"))
                                            core.Var("None")
                                          else core.Empty()),ns)
      case q::_ => throw new Error("Unrecognized qualifier: "+q)
    }

  /** Translate comprehensions to the algebra */
  def translate ( e: Expr ): core.Expr =
    e match {
      case ExternalVar(v,tp)
        => core.Var(v)
      case Var(v)
        => core.Var(v)
      case Nth(x,n)
        => core.Nth(translate(x),n)
      case Project(x,a)
        => x.tpe match {
            case RecordType(rs)
              => core.Nth(translate(x),rs.keys.toList.indexOf(a)+1)
            case _
              => core.MethodCall(translate(x),a,null)
            }
      case reduce(m,x)
        => core.reduce(translate(m),translate(x))
      case Tuple(es)
        => core.Tuple(es.map(translate))
      case Lambda(p,b)
        => core.Lambda(translate(p),translate(b))
      case Let(p,v,b)
        => core.MatchE(translate(v),List(core.Case(translate(p),core.BoolConst(true),translate(b))))
      case Call(f,es)
        => core.Call(f,es.map(translate))
      case MethodCall(o,m,null)
        => core.MethodCall(translate(o),m,null)
      case MethodCall(o,m,es)
        => core.MethodCall(translate(o),m,es.map(translate))
      case IfE(c,x,y)
        => core.IfE(translate(c),translate(x),translate(y))
      case Record(rs)
        => core.Tuple(rs.map(x => translate(x._2)).toList)
      case Collection(_,cs)
        => core.Call("Seq",cs.map(translate))
      case Comprehension(m,result,qs)
        => qs.span{ case GroupByQual(_,_) => false; case _ => true } match {
              case (r,GroupByQual(p,k)::s)
                => val groupByVars = patvars(p)
                   val liftedVars = freevars(Comprehension(BaseMonoid(""),result,s),groupByVars)
                                              .intersect(Normalizer.comprVars(r))
                   val lp = core.TuplePat(liftedVars.map(core.VarPat))
                   val vs = newvar
                   def lift ( x: core.Expr ): core.Expr
                     = liftedVars.foldRight(x) {
                         case (v,r) => core.AST.subst(v,core.flatMap(core.Lambda(lp,core.Elem(core.Var(v))),
                                                                     core.Var(vs)),
                                                      r) }
                   val re = lift(translate(Comprehension(m,result,s)))
                   val nh = translate(Elem(m,Tuple(List(k,Tuple(liftedVars.map(Var))))))
                   core.flatMap(core.Lambda(core.TuplePat(List(translate(p),core.VarPat(vs))),re),
                                core.groupBy(translateQualifiers(m,nh,r)))
              case _ => val nh = if (false && m == BaseMonoid("option"))  // wrong
                                    core.Call("Some",List(translate(result)))
                                 else core.Elem(translate(result))
                        translateQualifiers(m,nh,qs)
           }
      case Empty(BaseMonoid("option"))
        => core.Var("None")
      case Empty(m)
        => core.Empty()
      case Elem(BaseMonoid("option"),x)
        => core.Call("Some",List(translate(x)))
      case Elem(m,x)
        => core.Elem(translate(x))
      case Merge(x,y)
        if Optimizer.isArray(x)
        => val i = newvar
           val xs = newvar
           val ys = newvar
           core.flatMap(core.Lambda(core.TuplePat(List(core.VarPat(i),
                                                       core.TuplePat(List(core.VarPat(xs),
                                                                          core.VarPat(ys))))),
                                    core.Elem(core.Tuple(List(core.Var(i),
                                                              core.IfE(core.MethodCall(core.Var(ys),"nonEmpty",null),
                                                                       core.MethodCall(core.Var(ys),"head",null),
                                                                       core.MethodCall(core.Var(xs),"head",null)))))),
                        core.coGroup(translate(x),translate(y)))
      case Merge(x,y)
        => core.Merge(translate(x),translate(y))
      case StringConst(c)
        => core.StringConst(c)
      case CharConst(c)
        => core.CharConst(c)
      case IntConst(c)
        => core.IntConst(c)
      case LongConst(c)
        => core.LongConst(c)
      case DoubleConst(c)
        => core.DoubleConst(c)
      case BoolConst(c)
        => core.BoolConst(c)
      case _ => throw new Error("Unrecongnized calculus term: "+e)
    }
  }
