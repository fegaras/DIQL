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

object Translator {
  import AST._
  import Typechecker._

  val composition = BaseMonoid("composition")
  val vector = BaseMonoid("vector")
  val matrix = BaseMonoid("matrix")
  val option = BaseMonoid("option")
  val bag = BaseMonoid("bag")
  def some ( x: Expr ) = Elem(option,x)

  var external_variables: Map[String,Type] = Map()
  var global_variables: Map[String,Type] = Map()
  var functions: Map[String,(Map[String,Type],Type,Stmt)] = Map()

  def translate ( e: Expr, globals: Environment, locals: Environment ): Expr
    = e match {
        case Var(n)
          => some(Var(n))
        case VectorIndex(u,ii)
          => val i = newvar
             val v = newvar
             val A = newvar
             val iv = newvar
             Comprehension(option,
                           Var(v),
                           List(Generator(VarPat(A),translate(u,globals,locals)),
                                Generator(VarPat(iv),translate(ii,globals,locals)),
                                Generator(TuplePat(List(VarPat(i),VarPat(v))),
                                          Var(A)),
                                Predicate(MethodCall(Var(i),"==",List(Var(iv))))))
        case MatrixIndex(u,ii,jj)
          => val i = newvar
             val j = newvar
             val v = newvar
             val A = newvar
             val iv = newvar
             val jv = newvar
             Comprehension(option,
                           Var(v),
                           List(Generator(VarPat(A),translate(u,globals,locals)),
                                Generator(VarPat(iv),translate(ii,globals,locals)),
                                Generator(VarPat(jv),translate(jj,globals,locals)),
                                Generator(TuplePat(List(TuplePat(List(VarPat(i),VarPat(j))),
                                                        VarPat(v))),
                                          Var(A)),
                                Predicate(MethodCall(Var(i),"==",List(Var(iv)))),
                                Predicate(MethodCall(Var(j),"==",List(Var(jv))))))
        case Let(p,u,b)
          => val v = newvar
             val nlocals = bindPattern(p,typecheck(u,globals,locals),locals)
             Comprehension(option,
                           Var(v),
                           List(Generator(p,translate(u,globals,locals)),
                                Generator(VarPat(v),translate(b,globals,nlocals))))
        case Nth(x,n)
          => val v = newvar
             Comprehension(option,
                           Nth(Var(v),n),
                           List(Generator(VarPat(v),translate(x,globals,locals))))
        case Project(x,n)
          => val v = newvar
             Comprehension(option,
                           Project(Var(v),n),
                           List(Generator(VarPat(v),translate(x,globals,locals))))
        case Call("exists",List(u))
          => some(MethodCall(translate(u,globals,locals),"isDefined",null))
        case Call(f,es)
          => val vs = es.map(_ => newvar)
             Comprehension(option,
                           Call(f,vs.map(Var)),
                           (vs zip es).map {
                               case (v,a)
                                 => Generator(VarPat(v),translate(a,globals,locals))
                           })
        case MethodCall(o,":",List(x))
          => Merge(translate(o,globals,locals),
                   translate(x,globals,locals))
        case MethodCall(o,m,null)
          => val vo = newvar
             Comprehension(option,
                           MethodCall(Var(vo),m,null),
                           List(Generator(VarPat(vo),translate(o,globals,locals))))
        case MethodCall(o,m,es)
          => val vs = es.map(_ => newvar)
             val vo = newvar
             Comprehension(option,
                           MethodCall(Var(vo),m,vs.map(Var)),
                           Generator(VarPat(vo),translate(o,globals,locals)) ::
                               (vs zip es).map {
                                   case (v,a)
                                     => Generator(VarPat(v),translate(a,globals,locals))
                               })
        case IfE(p,x,y)
          => val vp = newvar
             val v = newvar
             Comprehension(option,
                           Var(v),
                           List(Generator(VarPat(vp),translate(p,globals,locals)),
                                Generator(VarPat(v),
                                          IfE(Var(vp),
                                              translate(x,globals,locals),
                                              translate(y,globals,locals)))))
        case Tuple(es)
          => val vs = es.map(_ => newvar)
             Comprehension(option,
                           Tuple(vs.map(Var)),
                           (vs zip es).map {
                               case (v,a)
                                 => Generator(VarPat(v),translate(a,globals,locals))
                           })
        case Record(es)
          => val vs = es.map(_ => newvar)
             Comprehension(option,
                           Record((vs zip es).map{ case (v,(s,_)) => (s,Var(v)) }.toMap),
                           (vs zip es).map {
                               case (v,(_,a))
                                 => Generator(VarPat(v),translate(a,globals,locals))
                           }.toList)
        case Collection(k,es)
          => val vs = es.map(_ => newvar)
             Comprehension(option,
                           Collection(k,vs.map(Var)),
                           (vs zip es).map {
                               case (v,a)
                                 => Generator(VarPat(v),translate(a,globals,locals))
                           })
        case Elem(m,x)
          => val v = newvar
             Comprehension(option,
                           Elem(m,Var(v)),
                           List(Generator(VarPat(v),translate(x,globals,locals))))
        case Merge(x,y)
          => val vx = newvar
             val vy = newvar
             Comprehension(option,
                           Merge(Var(vx),Var(vy)),
                           List(Generator(VarPat(vx),translate(x,globals,locals)),
                                Generator(VarPat(vy),translate(y,globals,locals))))
        case _ => some(apply(e,translate(_,globals,locals)))
      }

  def key ( d: Expr, globals: Environment, locals: Environment ): Expr
    = d match {
        case Var(v)
          => some(Tuple(Nil))
        case Project(u,_)
          => key(u,globals,locals)
        case Nth(u,_)
          => key(u,globals,locals)
        case VectorIndex(u,i)
          => val k = newvar
             val ii = newvar
             Comprehension(option,
                           Tuple(List(Var(k),Var(ii))),
                           List(Generator(VarPat(k),key(u,globals,locals)),
                                Generator(VarPat(ii),translate(i,globals,locals))))
        case MatrixIndex(u,i,j)
          => val k = newvar
             val ii = newvar
             val jj = newvar
             Comprehension(option,
                           Tuple(List(Var(k),Tuple(List(Var(ii),Var(jj))))),
                           List(Generator(VarPat(k),key(u,globals,locals)),
                                Generator(VarPat(ii),translate(i,globals,locals)),
                                Generator(VarPat(jj),translate(j,globals,locals))))
        case _ => throw new Error("Illegal destination: "+d)
      }

  def destination ( d: Expr, k: Expr ): Expr
    = d match {
        case Var(v)
          => some(Var(v))
        case Project(u,a)
          => val v = newvar
             Comprehension(option,
                           Project(Var(v),a),
                           List(Generator(VarPat(v),destination(u,k))))
        case Nth(u,n)
          => val v = newvar
             Comprehension(option,
                           Nth(Var(v),n),
                           List(Generator(VarPat(v),destination(u,k))))
        case VectorIndex(u,i)
          => val v = newvar
             val A = newvar
             val ii = newvar
             val ku = newvar
             val w = newvar
             Comprehension(option,
                           Var(v),
                           List(LetBinding(TuplePat(List(VarPat(ku),VarPat(w))),k),
                                Generator(VarPat(A),destination(u,Var(ku))),
                                Generator(TuplePat(List(VarPat(ii),VarPat(v))),
                                          Var(A)),
                                Predicate(MethodCall(Var(ii),"==",List(Var(w))))))
        case MatrixIndex(u,i,j)
          => val v = newvar
             val A = newvar
             val ii = newvar
             val jj = newvar
             val ku = newvar
             val w1 = newvar
             val w2 = newvar
             Comprehension(option,
                           Var(v),
                           List(LetBinding(TuplePat(List(VarPat(ku),TuplePat(List(VarPat(w1),VarPat(w2))))),k),
                                Generator(VarPat(A),destination(u,Var(ku))),
                                Generator(TuplePat(List(TuplePat(List(VarPat(ii),VarPat(jj))),VarPat(v))),
                                          Var(A)),
                                Predicate(MethodCall(Var(ii),"==",List(Var(w1)))),
                                Predicate(MethodCall(Var(jj),"==",List(Var(w2))))))
        case _ => throw new Error("Illegal destination: "+d)
      }

  def simpleDest ( e: Expr ): Boolean
    = accumulate[Boolean](e,{ case VectorIndex(_,_) => false
                              case MatrixIndex(_,_,_) => false
                              case _ => true },
                          _&&_,true)

  def update ( dest: Expr, value: Expr, globals: Environment, locals: Environment ): List[Code[Expr]]
    = dest match {
        case Var(n)
          if locals.contains(n)
          => throw new Error("Local variable "+n+" cannot be updated")
        case Var(n)
          => val nv = newvar
             val k = newvar
             List(Assignment(n,Comprehension(option,Var(nv),
                                             List(Generator(TuplePat(List(VarPat(k),VarPat(nv))),value)))))
        case Project(u,a)
          => typecheck(u,globals,locals) match {
                case RecordType(cs)
                  => val nv = newvar
                     val k = newvar
                     val w = newvar
                     update(u,
                            Comprehension(option,
                                          Tuple(List(Var(k),Record(cs.map {
                                              case (v,_)
                                                => v -> (if (v == a) Var(nv) else Project(Var(w),v))
                                          }))),
                                          List(Generator(TuplePat(List(VarPat(k),VarPat(nv))),value),
                                               Generator(VarPat(w),destination(u,Var(k))))),
                            globals,locals)
                  case _
                  => val nv = newvar
                     val k = newvar
                     val w = newvar
                     update(u,
                            Comprehension(option,
                                          Tuple(List(Var(k),Call("recordUpdate",
                                                                 List(Var(w),StringConst(a),Var(nv))))),
                                          List(Generator(TuplePat(List(VarPat(k),VarPat(nv))),value),
                                               Generator(VarPat(w),destination(u,Var(k))))),
                            globals,locals)

//                case t => throw new Error("Record projection "+dest+" must be over a record (found "+t+")")
             }
        case Nth(u,n)
          => typecheck(u,globals,locals) match {
                case TupleType(cs)
                  => val nv = newvar
                     val k = newvar
                     val w = newvar
                     update(u,
                            Comprehension(option,
                                          Tuple(List(Var(k),Tuple((1 to cs.length).map {
                                              i => if (i == n) Var(nv) else Nth(Var(w),i)
                                          }.toList))),
                                          List(Generator(TuplePat(List(VarPat(k),VarPat(nv))),value),
                                               Generator(VarPat(w),destination(u,Var(k))))),
                            globals,locals)
                case t => throw new Error("Tuple projection "+dest+" must be over a tuple (found "+t+")")
             }
        case VectorIndex(u,i)
          if simpleDest(u)
          => val A = newvar
             val k = newvar
             val w = newvar
             val v = newvar
             val ce = Comprehension(bag,Tuple(List(Var(w),Var(v))),
                                    List(Generator(TuplePat(List(TuplePat(List(VarPat(k),VarPat(w))),
                                                                 VarPat(v))),
                                                   value)))
             update(u,Comprehension(option,
                                    Tuple(List(Var(k),Merge(Var(A),ce))),
                                    List(Generator(VarPat(A),destination(u,Var(k))))),  // Var(k) is not used
                    globals,locals)
        case MatrixIndex(u,i,j)
          if simpleDest(u)
          => val A = newvar
             val k = newvar
             val w1 = newvar
             val w2 = newvar
             val v = newvar
             val ce = Comprehension(bag,Tuple(List(Tuple(List(Var(w1),Var(w2))),Var(v))),
                                    List(Generator(TuplePat(List(TuplePat(List(VarPat(k),
                                                                               TuplePat(List(VarPat(w1),VarPat(w2))))),
                                                                 VarPat(v))),
                                                   value)))
             update(u,Comprehension(option,
                                    Tuple(List(Var(k),Merge(Var(A),ce))),
                                    List(Generator(VarPat(A),destination(u,Var(k))))),  // Var(k) is not used
                    globals,locals)
        case VectorIndex(u,i)
          => val A = newvar
             val s = newvar
             val k = newvar
             val w = newvar
             val v = newvar
             update(u,Comprehension(option,
                                    Tuple(List(Var(k),Merge(Var(A),Var(s)))),
                                    List(Generator(TuplePat(List(TuplePat(List(VarPat(k),VarPat(w))),
                                                                 VarPat(v))),
                                                   value),
                                         LetBinding(VarPat(s),Tuple(List(Var(w),Var(v)))),
                                         GroupByQual(VarPat(k),Var(k)),
                                         Generator(VarPat(A),destination(u,Var(k))))),
                    globals,locals)
        case MatrixIndex(u,i,j)
          => val A = newvar
             val s = newvar
             val k = newvar
             val w1 = newvar
             val w2 = newvar
             val v = newvar
             update(u,Comprehension(option,
                                    Tuple(List(Var(k),Merge(Var(A),Var(s)))),
                                    List(Generator(TuplePat(List(TuplePat(List(VarPat(k),
                                                                               TuplePat(List(VarPat(w1),VarPat(w2))))),
                                                                 VarPat(v))),
                                                   value),
                                         LetBinding(VarPat(s),Tuple(List(Tuple(List(Var(w1),Var(w2))),Var(v)))),
                                         GroupByQual(VarPat(k),Var(k)),
                                         Generator(VarPat(A),destination(u,Var(k))))),
                    globals,locals)
        case _ => throw new Error("Illegal destination: "+dest)
    }

  def substE ( e: Expr, env: Map[String,String] ): Expr
    = env.foldLeft[Expr](e) { case (r,(v,nv)) => subst(v,Var(nv),r) }

  def renameStmt ( s: Stmt, env: Map[String,String] ): Stmt
    = s match {
        case Block(ss)
          => val nenv = env ++ ss.flatMap{
                  case DeclareExternal(v,_) => List(v -> newvar)
                  case DeclareVar(v,_,_) => List(v -> newvar)
                  case _ => Nil
             }
             Block(ss.map(renameStmt(_,nenv)))
        case DeclareVar(v,t,u)
          => DeclareVar(env(v),t,u.map(substE(_,env)))
        case DeclareExternal(v,t)
          => DeclareExternal(env(v),t)
        case Assign(d,u)
          => Assign(substE(d,env),substE(u,env))
        case ForS(v,e1,e2,e3,b)
          => ForS(v,substE(e1,env),substE(e2,env),substE(e3,env),renameStmt(b,env))
        case ForeachS(v,e,b)
          => ForeachS(v,substE(e,env),renameStmt(b,env))
        case WhileS(p,b)
          => WhileS(substE(p,env),renameStmt(b,env))
        case IfS(p,s1,s2)
          => IfS(substE(p,env),renameStmt(s1,env),renameStmt(s2,env))
        case Return(e)
          => Return(substE(e,env))
        case CodeE(e)
          => CodeE(substE(e,env))
        case _ => s
      }

  var unfolded_function_bodies: List[Code[Expr]] = Nil

  def unfold_calls ( e: Expr, quals: List[Qualifier], globals: Environment, locals: Environment ): Expr
    = e match {
        case Call(f,es)
          if functions.contains(f)
          => val (ps,tp,b) = functions(f)
             val rv = newvar
             val decls = (ps zip es).map{ case ((v,tp),u) => DeclareVar(v,tp,Some(u)) }.toList
             val c = translate(renameStmt(Block((DeclareVar(rv,tp,None)::decls):+b),Map()),
                               quals,rv,globals,locals)
             unfolded_function_bodies = unfolded_function_bodies ++ c
             global_variables = global_variables + ((rv,tp))
             Var(rv)
        case _
          => apply(e,unfold_calls(_,quals,globals,locals))
      }

  def unfoldCalls ( e: Expr, quals: List[Qualifier], globals: Environment, locals: Environment ): (List[Code[Expr]],Expr)
    = { unfolded_function_bodies = Nil
        val ne = unfold_calls(e,quals,globals,locals)
        (unfolded_function_bodies,ne)
      }

  def translate ( s: Stmt, quals: List[Qualifier], return_var: String,
                  globals: Environment, locals: Environment ): List[Code[Expr]]
    = s match {
          case Assign(d@VectorIndex(Var(a),i),MethodCall(x,op,List(e)))
            if d == x
            => val v = newvar
               val k = newvar
               val ii = newvar
               val (calls,ne) = unfoldCalls(e,quals,globals,locals)
               calls ++
               List(Assignment(a,Call("increment",
                                      List(Var(a),StringConst(op),
                                           Comprehension(bag,
                                                         Tuple(List(Var(k),reduce(BaseMonoid(op),Var(v)))),
                                                         quals++List(Generator(VarPat(v),translate(ne,globals,locals)),
                                                                     Generator(VarPat(ii),translate(i,globals,locals)),
                                                                     GroupByQual(VarPat(k),Var(ii))))))))
          case Assign(d@MatrixIndex(Var(a),i,j),MethodCall(x,op,List(e)))
            if d == x
            => val v = newvar
               val k = newvar
               val ii = newvar
               val jj = newvar
               val (calls,ne) = unfoldCalls(e,quals,globals,locals)
               calls ++
               List(Assignment(a,Call("increment",
                                      List(Var(a),StringConst(op),
                                           Comprehension(bag,
                                                         Tuple(List(Var(k),reduce(BaseMonoid(op),Var(v)))),
                                                         quals++List(Generator(VarPat(v),translate(ne,globals,locals)),
                                                                     Generator(VarPat(ii),translate(i,globals,locals)),
                                                                     Generator(VarPat(jj),translate(j,globals,locals)),
                                                                     GroupByQual(VarPat(k),Tuple(List(Var(ii),Var(jj))))))))))
          case Assign(d@Var(a),MethodCall(x,op,List(e)))
            if d == x
            => val v = newvar
               val (calls,ne) = unfoldCalls(e,quals,globals,locals)
               calls ++
               List(Assignment(a,Elem(option,MethodCall(d,op,
                                         List(reduce(BaseMonoid(op),
                                                     Comprehension(bag,Var(v),
                                                          quals++List(Generator(VarPat(v),translate(ne,globals,locals))))))))))
          case Assign(d@VectorIndex(Var(a),i),e)
            => val v = newvar
               val ii = newvar
               val (calls,ne) = unfoldCalls(e,quals,globals,locals)
               calls ++
               List(Assignment(a,Call("update",
                                      List(Var(a),
                                           Comprehension(bag,
                                                         Tuple(List(Var(ii),Var(v))),
                                                         quals++List(Generator(VarPat(v),translate(ne,globals,locals)),
                                                                     Generator(VarPat(ii),translate(i,globals,locals))))))))
          case Assign(d@MatrixIndex(Var(a),i,j),e)
            => val v = newvar
               val ii = newvar
               val jj = newvar
               val (calls,ne) = unfoldCalls(e,quals,globals,locals)
               calls ++
               List(Assignment(a,Call("update",
                                      List(Var(a),
                                           Comprehension(bag,
                                                         Tuple(List(Tuple(List(Var(ii),Var(jj))),Var(v))),
                                                         quals++List(Generator(VarPat(v),translate(ne,globals,locals)),
                                                                     Generator(VarPat(ii),translate(i,globals,locals)),
                                                                     Generator(VarPat(jj),translate(j,globals,locals))))))))
          case Assign(d,MethodCall(x,op,List(e)))
            if d == x
            => val v = newvar
               val k = newvar
               val w = newvar
               val (calls,ne) = unfoldCalls(e,quals,globals,locals)
               calls ++
               update(d,Comprehension(bag,
                                      Tuple(List(Var(k),MethodCall(Var(w),op,List(reduce(BaseMonoid(op),Var(v)))))),
                                      quals++List(Generator(VarPat(v),translate(ne,globals,locals)),
                                                  Generator(VarPat(k),key(d,globals,locals)),
                                                  GroupByQual(VarPat(k),Var(k)),
                                                  Generator(VarPat(w),destination(d,Var(k))))),
                      globals,locals)
          case Assign(d,e)
            => val v = newvar
               val k = newvar
               val (calls,ne) = unfoldCalls(e,quals,globals,locals)
               calls ++
               update(d,Comprehension(bag,
                                      Tuple(List(Var(k),Var(v))),
                                      quals++List(Generator(VarPat(v),translate(ne,globals,locals)),
                                                  Generator(VarPat(k),key(d,globals,locals)))),
                      globals,locals)
          case CodeE(e)
            => List(CodeC(Comprehension(option,translate(e,globals,locals),quals)))
          case ForS(v,e1,e2,e3,b)
            => val nv = newvar
               translate(b,
                         quals++List(Generator(VarPat(nv),
                                               translate(Call("range",List(e1,e2,e3)),
                                                         globals,locals)),
                                     Generator(VarPat(v),Var(nv))),
                         return_var,globals,locals+((v,intType)))
          case ForeachS(v,e,b)
            => typecheck(e,globals,locals) match {
                  case ParametricType(f,List(tp))
                    if List("vector","matrix").contains(f)
                    => val A = newvar
                       val i = newvar
                       translate(b,
                                 quals++List(Generator(VarPat(A),
                                                       translate(e,globals,locals)),
                                             Generator(TuplePat(List(VarPat(i),VarPat(v))),Var(A))),
                                 return_var,globals,locals+((v,tp)))
                  case ParametricType("map",List(t1,t2))
                    => val A = newvar
                       translate(b,
                                 quals++List(Generator(VarPat(A),
                                                       translate(e,globals,locals)),
                                             Generator(VarPat(v),Var(A))),
                                 return_var,globals,locals+((v,TupleType(List(t1,t2)))))
                  case ParametricType(_,List(tp))
                    => val A = newvar
                       translate(b,
                                 quals++List(Generator(VarPat(A),
                                                       translate(e,globals,locals)),
                                             Generator(VarPat(v),Var(A))),
                                 return_var,globals,locals+((v,tp)))
                  case _ => throw new Error("Foreach statement must be over a collection: "+s)
               }
          case WhileS(e,ts)
            => List(WhileLoop(translate(e,globals,locals),
                              CodeBlock(translate(ts,quals,return_var,globals,locals))))
          case IfS(e,ts,Block(Nil))
            => val p = newvar
               translate(ts,quals++List(Generator(VarPat(p),translate(e,globals,locals)),
                                        Predicate(Var(p))),
                         return_var,globals,locals)
          case IfS(e,ts1,ts2)
            => val p = newvar
               val pc = translate(e,globals,locals)
               translate(ts1,quals++List(Generator(VarPat(p),pc),Predicate(Var(p))),
                         return_var,globals,locals) ++
               translate(ts2,quals++List(Generator(VarPat(p),pc),Predicate(MethodCall(Var(p),"!",null))),
                         return_var,globals,locals)
          case Return(e)
            => if (return_var == null)
                  throw new Error("A Return statement can only appear inside a function body: "+e)
               translate(Assign(Var(return_var),e),quals,null,globals,locals)
          case Block(ss)
            => val (m,_,_) = ss.foldLeft(( Nil:List[Code[Expr]], globals, locals )) {
                    case ((ns,gs,ls),DeclareExternal(v,t))
                      => external_variables = external_variables+((v,t))
                         ( ns, gs+((v,t)), ls )
                    case ((ns,gs,ls),DeclareVar(v,t,None))
                      => global_variables = global_variables+((v,t))
                         ( ns, gs+((v,t)), ls )
                    case ((ns,gs,ls),DeclareVar(v,t,Some(e)))
                      => global_variables = global_variables+((v,t))
                         val ts = translate(Assign(Var(v),e),quals,return_var,gs+((v,t)),ls)
                         ( ns++ts, gs+((v,t)), ls )
                    case ((ns,gs,ls),Def(f,ps,tp,b))
                      => functions = functions + ((f,(ps,tp,b)))
                         typecheck(b,tp,ps,globals)
                         val ftp = FunctionType(TupleType(ps.values.toList),tp)
                         ( ns, gs+((f,ftp)), ls )
                    case ((ns,gs,ls),stmt)
                      => ( ns++translate(stmt,quals,return_var,gs,ls), gs, ls )
                    }
               m
          case _ => throw new Error("Illegal statement: "+s)
    }

  def translate ( s: Stmt ): Code[Expr]
    = CodeBlock(translate(s,Nil,null,Map():Environment,Map():Environment))
}
