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

object DefaultTranslator {
  import AST._
  import Typechecker._

  val composition = BaseMonoid("composition")
  val vector = BaseMonoid("vector")
  val matrix = BaseMonoid("matrix")
  val option = BaseMonoid("option")
  def some ( x: Expr ) = Elem(option,x)

  var externals: Map[String,Type] = Map()

  def translate ( e: Expr, state: Expr, globals: Environment, locals: Environment ): Expr
    = e match {
        case Var(n)
          if locals.contains(n)
          => some(Var(n))
        case Var(n)
          => val st = newvar
             Comprehension(option,
                           Project(Var(st),n),
                           List(Generator(VarPat(st),state)))
        case VectorIndex(u,ii)
          => val i = newvar
             val v = newvar
             val A = newvar
             val iv = newvar
             Comprehension(option,
                           Var(v),
                           List(Generator(VarPat(A),translate(u,state,globals,locals)),
                                Generator(VarPat(iv),translate(ii,state,globals,locals)),
                                Generator(TuplePat(List(VarPat(i),VarPat(v))),Var(A)),
                                Predicate(Call("==",List(Var(i),Var(iv))))))
        case MatrixIndex(u,ii,jj)
          => val i = newvar
             val j = newvar
             val v = newvar
             val A = newvar
             val iv = newvar
             val jv = newvar
             Comprehension(option,
                           Var(v),
                           List(Generator(VarPat(A),translate(u,state,globals,locals)),
                                Generator(VarPat(iv),translate(ii,state,globals,locals)),
                                Generator(VarPat(jv),translate(jj,state,globals,locals)),
                                Generator(TuplePat(List(TuplePat(List(VarPat(i),VarPat(j))),
                                                        VarPat(v))),
                                          Var(A)),
                                Predicate(Call("==",List(Var(i),Var(iv)))),
                                Predicate(Call("==",List(Var(j),Var(jv))))))
        case Let(p,u,b)
          => val v = newvar
             val nlocals = bindPattern(p,typecheck(u,globals,locals),locals)
             Comprehension(option,
                           Var(v),
                           List(Generator(p,translate(u,state,globals,locals)),
                                Generator(VarPat(v),translate(b,state,globals,nlocals))))
        case Nth(x,n)
          => val v = newvar
             Comprehension(option,
                           Nth(Var(v),n),
                           List(Generator(VarPat(v),translate(x,state,globals,locals))))
        case Project(x,n)
          => val v = newvar
             Comprehension(option,
                           Project(Var(v),n),
                           List(Generator(VarPat(v),translate(x,state,globals,locals))))
        case Call(f,es)
          => val vs = es.map(_ => newvar)
             Comprehension(option,
                           Call(f,vs.map(Var)),
                           (vs zip es).map {
                               case (v,a)
                                 => Generator(VarPat(v),translate(a,state,globals,locals))
                           })
        case IfE(p,x,y)
          => val vp = newvar
             val v = newvar
             Comprehension(option,
                           Var(v),
                           List(Generator(VarPat(vp),translate(p,state,globals,locals)),
                                Generator(VarPat(v),
                                          IfE(Var(vp),
                                              translate(x,state,globals,locals),
                                              translate(y,state,globals,locals)))))
        case Tuple(es)
          => val vs = es.map(_ => newvar)
             Comprehension(option,
                           Tuple(vs.map(Var)),
                           (vs zip es).map {
                               case (v,a)
                                 => Generator(VarPat(v),translate(a,state,globals,locals))
                           })
        case Record(es)
          => val vs = es.map(_ => newvar)
             Comprehension(option,
                           Record((vs zip es).map{ case (v,(s,_)) => (s,Var(v)) }.toMap),
                           (vs zip es).map {
                               case (v,(_,a))
                                 => Generator(VarPat(v),translate(a,state,globals,locals))
                           }.toList)
        case Collection(k,es)
          => val vs = es.map(_ => newvar)
             Comprehension(option,
                           Collection(k,vs.map(Var)),
                           (vs zip es).map {
                               case (v,a)
                                 => Generator(VarPat(v),translate(a,state,globals,locals))
                           })
        case Elem(m,x)
          => val v = newvar
             Comprehension(option,
                           Elem(m,Var(v)),
                           List(Generator(VarPat(v),translate(x,state,globals,locals))))
        case Merge(x,y)
          => val vx = newvar
             val vy = newvar
             Comprehension(option,
                           Merge(Var(vx),Var(vy)),
                           List(Generator(VarPat(vx),translate(x,state,globals,locals)),
                                Generator(VarPat(vy),translate(y,state,globals,locals))))
        case _ => some(apply(e,translate(_,state,globals,locals)))
      }

  def update ( dest: Expr, value: Expr, state: Expr, globals: Environment, locals: Environment ): Expr
    = dest match {
        case Var(n)
          if globals.contains(n)
          => val nv = newvar
             val st = newvar
             Comprehension(option,
                           Record(globals.map {
                               case (v,_)
                                 => v -> (if (v == n) Var(nv) else Project(Var(st),v))
                           }),
                           List(Generator(VarPat(st),state),
                                Generator(VarPat(nv),value)))
        case Var(n)
          if locals.contains(n)
          => throw new Error("Local variable "+n+" cannot be updated")
        case Var(n)
          => throw new Error("No such variable: "+n)
        case Project(u,a)
          => typecheck(u,globals,locals) match {
                case RecordType(cs)
                  => val nv = newvar
                     val w = newvar
                     update(u,
                            Comprehension(option,
                                          Record(cs.map {
                                              case (v,_)
                                                => v -> (if (v == a) Var(nv) else Project(Var(w),v))
                                          }),
                                          List(Generator(VarPat(w),translate(u,state,globals,locals)),
                                               Generator(VarPat(nv),value))),
                            state,globals,locals)
                case t => throw new Error("Record projection "+dest+" must be over a record (found "+t+")")
             }
        case Nth(u,n)
          => typecheck(u,globals,locals) match {
                case TupleType(cs)
                  => val nv = newvar
                     val w = newvar
                     update(u,
                            Comprehension(option,
                                          Tuple((1 to cs.length).map {
                                              i => if (i == n) Var(nv) else Nth(Var(w),i)
                                          }.toList),
                                          List(Generator(VarPat(w),translate(u,state,globals,locals)),
                                               Generator(VarPat(nv),value))),
                            state,globals,locals)
                case t => throw new Error("Tuple projection "+dest+" must be over a tuple (found "+t+")")
             }
        case VectorIndex(u,i)
          => val v = newvar
             val x = newvar
             val ii = newvar
             update(u,Comprehension(option,
                                    Merge(Var(v),Elem(vector,Tuple(List(Var(ii),Var(x))))),
                                    List(Generator(VarPat(v),translate(u,state,globals,locals)),
                                         Generator(VarPat(ii),translate(i,state,globals,locals)),
                                         Generator(VarPat(x),value))),
                    state,globals,locals)
        case MatrixIndex(u,i,j)
          => val v = newvar
             val x = newvar
             val ii = newvar
             val jj = newvar
             update(u,Comprehension(option,
                                    Merge(Var(v),Elem(matrix,Tuple(List(Tuple(List(Var(ii),Var(jj))),
                                                                        Var(x))))),
                                    List(Generator(VarPat(v),translate(u,state,globals,locals)),
                                         Generator(VarPat(ii),translate(i,state,globals,locals)),
                                         Generator(VarPat(jj),translate(j,state,globals,locals)),
                                         Generator(VarPat(x),value))),
                    state,globals,locals)
        case _ => throw new Error("Illegal destination: "+dest)
    }

  def translate ( s: Stmt, state: Expr, globals: Environment, locals: Environment ): Expr
    = s match {
          case Assign(d,e)
            => update(d,translate(e,state,globals,locals),
                      state,globals,locals)
          case Block(ss)
            => val (nstate,_,_) = ss.foldLeft(( state, globals, locals )) {
                    case ((ns,gs,ls),DeclareExternal(v,t))
                      => externals = externals+((v,t))
                         ( update(Var(v),Elem(option,ExternalVar(v,t)),ns,gs+((v,t)),ls),
                           gs+((v,t)), ls )
                    case ((ns,gs,ls),DeclareVar(v,t,None))
                      => ( update(Var(v),Elem(option,ExternalVar(v,t)),ns,gs+((v,t)),ls),
                           gs+((v,t)), ls )
                    case ((ns,gs,ls),DeclareVar(v,t,Some(e)))
                      => val te = translate(e,state,globals,locals)
                         ( update(Var(v),te,ns,gs+((v,t)),ls),
                           gs+((v,t)), ls )
                    case ((ns,gs,ls),stmt)
                      => ( translate(stmt,ns,gs,ls),
                           gs, ls )
                    }
               nstate
          case ForeachS(v,e,b)
            => typecheck(e,globals,locals) match {
                  case ParametricType(_,List(tp))
                    => val nv = newvar
                       val i = newvar
                       val st = newvar
                       Apply(Comprehension(composition,
                                           Lambda(VarPat(st),
                                                  translate(b,Var(st),globals,locals+((v,tp)))),
                                           List(Generator(VarPat(nv),
                                                          translate(e,state,globals,locals)),
                                                Generator(TuplePat(List(VarPat(i),VarPat(v))),Var(nv)))),
                             state)
                  case _ => throw new Error("Foreach statement must be over a collection: "+s)
               }
          case ForS(v,n1,n2,n3,b)
            => val nv = newvar
               val st = newvar
               Apply(Comprehension(composition,
                                   Lambda(VarPat(st),
                                          translate(b,Var(st),globals,locals+((v,intType)))),
                                   List(Generator(VarPat(nv),
                                                  translate(Call("range",List(n1,n2,n3)),
                                                            state,globals,locals)),
                                        Generator(VarPat(v),Var(nv)))),
                     state)
          case IfS(p,te,ee)
            => val np = newvar
               val nv = newvar
               Comprehension(option,
                             Var(nv),
                             List(Generator(VarPat(np),translate(p,state,globals,locals)),
                                  Generator(VarPat(nv),
                                            IfE(Var(np),
                                                translate(te,state,globals,locals),
                                                translate(ee,state,globals,locals)))))
          case _ => throw new Error("Illegal statement: "+s)
    }

  def translate ( s: Stmt ): Expr
    = translate(s,some(Record(Map())),Map():Environment,Map():Environment)
}
