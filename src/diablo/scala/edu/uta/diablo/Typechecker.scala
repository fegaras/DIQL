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

object Typechecker {
    import AST._

    type Environment = Map[String,Type]

    val intType = BasicType("Int")
    val longType = BasicType("Long")
    val boolType = BasicType("Boolean")
    val doubleType = BasicType("Double")
    val stringType = BasicType("String")

    // hooks to the Scala compiler; set at v_impl in DIQL.scala
    var typecheck_call: ( String, List[Type] ) => Option[Type] = null
    var typecheck_method: ( Type, String, List[Type] ) => Option[Type] = null
    var typecheck_var: ( String ) => Option[Type] = null

    val collection_names = List("vector","matrix","bag","list", "map",
                                ComprehensionTranslator.arrayClassName,
                                ComprehensionTranslator.datasetClassPath)

    def isCollection ( f: String ): Boolean
      = collection_names.contains(f)

    def typeMatch ( t1: Type, t2: Type ): Boolean
      = ((t1 == AnyType() || t2 == AnyType())
         || t1 == t2
         || ((t1,t2) match {
                case (ParametricType("vector",List(ts1)),
                      ParametricType("bag",List(TupleType(List(it,ts2)))))
                  => typeMatch(ts1,ts2) && typeMatch(it,longType)
                case (ParametricType("matrix",List(ts1)),
                      ParametricType("bag",List(TupleType(List(TupleType(List(it1,it2)),ts2)))))
                  => typeMatch(ts1,ts2) && typeMatch(it1,longType) && typeMatch(it2,longType)
                case (ParametricType(n1,ts1),ParametricType(n2,ts2))
                  if n1==n2 && ts1.length == ts2.length
                  => (ts1 zip ts2).map{ case (tp1,tp2) => typeMatch(tp1,tp2) }.reduce(_&&_)
                case (TupleType(ts1),TupleType(ts2))
                  if ts1.length == ts2.length
                  => (ts1 zip ts2).map{ case (tp1,tp2) => typeMatch(tp1,tp2) }.reduce(_&&_)
                case (RecordType(cs1),RecordType(cs2))
                  if cs1.size == cs2.size
                  => (cs1 zip cs2).map{ case ((_,tp1),(_,tp2)) => typeMatch(tp1,tp2) }.reduce(_&&_)
                case (FunctionType(dt1,tt1),FunctionType(dt2,tt2))
                  => typeMatch(dt1,dt2) && typeMatch(tt1,tt2)
                case _ => false
            }))

    def bindPattern ( pat: Pattern, tp: Type, env: Environment ): Environment
      = (pat,tp) match {
          case (TuplePat(cs),TupleType(ts))
            => if (cs.length != ts.length)
                 throw new Error("Pattern "+pat+" does not match the type "+tp)
               else cs.zip(ts).foldRight(env){ case ((p,t),r) => bindPattern(p,t,r) }
          case (VarPat(v),t)
            => env+((v,t))
          case (StarPat(),_)
            => env
          case _ => throw new Error("Pattern "+pat+" does not match the type "+tp)
      }

    def typecheck ( e: Expr, globals: Environment, locals: Environment ): Type
      = try { val tpe = e match {
          case ExternalVar(v,tp)
            => tp
          case Var("null")
            => AnyType()
          case Var(v)
            => if (globals.contains(v))
                  globals(v)
               else if (locals.contains(v))
                  locals(v)
               else typecheck_var(v). // call the Scala typechecker to find var v
                          getOrElse(throw new Error("Undefined variable: "+v))
          case Nth(u,n)
            => typecheck(u,globals,locals) match {
                  case TupleType(cs)
                    => if (n<=0 || n>cs.length)
                          throw new Error("Tuple does not contain a "+n+" element")
                       cs(n-1)
                  case t => throw new Error("Tuple projection "+e+" must be over a tuple (found "+t+")")
               }
          case Project(u,a)
            => typecheck(u,globals,locals) match {
                  case RecordType(cs)
                    => if (cs.contains(a))
                         cs(a)
                       else throw new Error("Unknown record attribute: "+a)
                  case ParametricType("vector",_) if a == "length"
                    => longType
                  case ParametricType("matrix",_) if a == "rows" || a == "cols"
                    => longType
                  case _ => typecheck(MethodCall(u,a,null),globals,locals)
               }
          case VectorIndex(u,i)
            => val itp = typecheck(i,globals,locals)
               typecheck(u,globals,locals) match {
                  case ParametricType("vector",List(t))
                    => if ( itp != longType && itp != intType )
                          throw new Error("Vector indexing "+e+" must use an integer index: "+i)
                       t
                  case ParametricType("map",List(k,v))
                    => if ( itp != k )
                          throw new Error("Map indexing "+e+" must use an index of type "+k)
                       v
                  case t => throw new Error("Vector indexing "+e+" must be over a vector (found "+t+")")
               }
          case MatrixIndex(u,i,j)
            => val itp = typecheck(i,globals,locals)
               val jtp = typecheck(j,globals,locals)
               if ( itp != longType && itp != intType )
                  throw new Error("Matrix indexing "+e+" must use an integer row index: "+i)
               else if ( jtp != longType && jtp != intType )
                  throw new Error("Matrix indexing "+e+" must use an integer column index: "+j)
               else typecheck(u,globals,locals) match {
                  case ParametricType("matrix",List(t))
                    => t
                  case t => throw new Error("Matrix indexing "+e+" must be over a matrix (found "+t+")")
               }
          case Let(p,u,b)
            => typecheck(b,globals,bindPattern(p,typecheck(u,globals,locals),locals))
          case Collection("matrix",args)
            => val tp = args.foldRight(AnyType():Type){ case (a,r)
                                        => typecheck(a,globals,locals) match {
                                               case TupleType(tps)
                                                 => tps.foldRight(r){ case (t,s)
                                                        => if (s != AnyType() && t != s)
                                                              throw new Error("Incompatible types in matrix "+e)
                                                           else t }
                                               case _ => throw new Error("Matrix elements must be tuples: "+e) } }
               ParametricType("matrix",List(tp))
          case Collection(f,args)
            if isCollection(f)
            => val tp = args.foldRight(AnyType():Type){ case (a,r)
                                        => val t = typecheck(a,globals,locals)
                                           if (r != AnyType() && t != r)
                                              throw new Error("Incompatible types in collection "+e)
                                           else t }
               ParametricType(f,List(tp))
          case Comprehension(m,h,qs)
            => var nenv = locals             // extended binding environment
               var pvs: List[String] = Nil   // pattern variables in comprehension
               for ( q <- qs ) {
                  q match {
                    case Generator(p,d)
                      => typecheck(d,globals,nenv) match {
                            case ParametricType("vector",List(t))
                              => nenv = bindPattern(p,TupleType(List(longType,t)),nenv)
                            case ParametricType("matrix",List(t))
                              => nenv = bindPattern(p,TupleType(List(TupleType(List(longType,longType)),t)),nenv)
                            case ParametricType("map",List(k,v))
                              => nenv = bindPattern(p,TupleType(List(k,v)),nenv)
                            case ParametricType(_,List(t))
                              => nenv = bindPattern(p,t,nenv)
                            case t => throw new Error("Expected a collection type in generator "+d+" (found "+t+")")
                         }
                         pvs = pvs ++ patvars(p)
                    case LetBinding(p,d)
                      => nenv = bindPattern(p,typecheck(d,globals,nenv),nenv)
                         pvs = pvs ++ patvars(p)
                    case Predicate(p)
                      => if (typecheck(p,globals,nenv) != boolType)
                           throw new Error("The comprehension predicate "+p+" must be a boolean")
                    case GroupByQual(p,k)
                      => val nvs = patvars(p)
                         val ktp = typecheck(k,globals,nenv)
                         // lift all pattern variables to bags
                         nenv = nenv ++ pvs.diff(nvs).map{ v => (v,ParametricType("bag",List(nenv(v)))) }.toMap
                         nenv = bindPattern(p,ktp,nenv)
                         pvs = pvs ++ nvs
                  }
              }
              m match {
                case BaseMonoid("option")
                  => ParametricType("option",List(typecheck(h,globals,nenv)))
                case BaseMonoid("bag")
                  => ParametricType("bag",List(typecheck(h,globals,nenv)))
                case ParametricMonoid(f,_)
                  if isCollection(f)
                  => ParametricType(f,List(typecheck(h,globals,nenv)))
                case _ => throw new Error("The comprehension monoid "+m+" must be a collection monoid")
              }
          case Call("exists",List(u))
            => typecheck(u,globals,locals)
               boolType
          case Call("increment",List(u,_,s))
            => typecheck(s,globals,locals)
               typecheck(u,globals,locals)
          case Call("update",List(u,s))
            => typecheck(s,globals,locals)
               typecheck(u,globals,locals)
          case Call("recordUpdate",List(x,_,_))
            => typecheck(x,globals,locals)
          case Call(f,args)
            if globals.contains(f)
            => val tp = typecheck(Tuple(args),globals,locals)
               globals(f) match {
                  case FunctionType(dt,rt)
                    => if (typeMatch(dt,tp)) rt
                       else throw new Error("Function "+f+" on "+dt+" cannot be applied to "+args+" of type "+tp);
                  case _ => throw new Error(f+" is not a function: "+e)
               }
          case Call(f,args)
            => // call the Scala typechecker to find function f
               typecheck_call(f,args.map(typecheck(_,globals,locals))).
                          getOrElse(throw new Error("Wrong function call: "+e))
          case MethodCall(o,":",List(x))
            => val tp = typecheck(o,globals,locals)
               if (!typeMatch(tp,typecheck(o,globals,locals)))
                  throw new Error("The default value in "+e+" must have type: "+tp)
               return tp
          case MethodCall(u,m,null)
            => // call the Scala typechecker to find method m
               typecheck_method(typecheck(u,globals,locals),m,null).
                          getOrElse(throw new Error("Wrong method call: "+e))
          case MethodCall(u,m,args)
            => // call the Scala typechecker to find method m
               typecheck_method(typecheck(u,globals,locals),m,args.map(typecheck(_,globals,locals))).
                          getOrElse(throw new Error("Wrong method call: "+e))
          case IfE(p,a,b)
            => if (typecheck(p,globals,locals) != boolType)
                 throw new Error("The if-expression condition "+p+" must be a boolean")
               val tp = typecheck(a,globals,locals)
               if (!typeMatch(typecheck(b,globals,locals),tp))
                 throw new Error("Ill-typed if-expression"+e)
               tp
          case Apply(Lambda(p,b),arg)
            => typecheck(b,globals,bindPattern(p,typecheck(arg,globals,locals),locals))
          case Apply(Comprehension(BaseMonoid("composition"),Lambda(VarPat(v),b),qs),x)
            => val nv = newvar
               typecheck(Comprehension(BaseMonoid("option"),Var(nv),qs:+Generator(VarPat(nv),b)),globals,
                         locals+(v->typecheck(x,globals,locals)))
          case Apply(f,arg)
            => val tp = typecheck(arg,globals,locals)
               typecheck(f,globals,locals) match {
                  case FunctionType(dt,rt)
                    => if (typeMatch(dt,tp)) rt
                       else throw new Error("Function "+f+" cannot be applied to "+arg+" of type "+tp);
                  case _ => throw new Error("Expected a function "+f)
               }
          case Tuple(cs)
            => TupleType(cs.map{ typecheck(_,globals,locals) })
          case Record(cs)
            => RecordType(cs.map{ case (v,u) => v->typecheck(u,globals,locals) })
          case Merge(x,y)
            => val xtp = typecheck(x,globals,locals)
               val ytp = typecheck(y,globals,locals)
               if (!typeMatch(xtp,ytp))
                   throw new Error("Incompatible types in Merge: "+e+" (found "+xtp+" and "+ytp+")")
               xtp
          case Elem(BaseMonoid("vector"),x)
            => typecheck(x,globals,locals) match {
                  case TupleType(List(BasicType("Long"),tp))
                    => ParametricType("vector",List(tp))
                  case _ => throw new Error("Wrong vector: "+e)
               }
          case Elem(BaseMonoid("matrix"),x)
            => typecheck(x,globals,locals) match {
                  case TupleType(List(TupleType(List(BasicType("Long"),BasicType("Long"))),tp))
                    => ParametricType("matrix",List(tp))
                  case _ => throw new Error("Wrong matrix: "+e)
               }
          case Elem(BaseMonoid(m),x)
            => ParametricType(m,List(typecheck(x,globals,locals)))
          case Empty(BaseMonoid(f))
            => ParametricType(f,List(AnyType()))
          case reduce(m,u)
            => typecheck(u,globals,locals) match {
                  case ParametricType(_,List(tp))
                    => tp
                  case tp => throw new Error("Reduction "+e+" must must be over a collection (found "+tp+")")
               }
          case StringConst(_) => stringType
          case IntConst(_) => intType
          case LongConst(_) => longType
          case DoubleConst(_) => doubleType
          case BoolConst(_) => boolType
          case _ => throw new Error("Illegal expression: "+e)
        }
        e.tpe = tpe
        tpe
      } catch { case m: Error => throw new Error(m.getMessage+"\nFound in: "+e) }


    def typecheck ( s: Stmt, return_type: Type, globals: Environment, locals: Environment ): Environment
      = try { s match {
          case DeclareVar(v,t,_)
            => globals+((v,t))
          case DeclareExternal(v,t)
            => globals+((v,t))
          case Def(f,ps,tp,b)
            => globals+((f,FunctionType(TupleType(ps.values.toList),tp)))
          case Block(cs)
            => cs.foldLeft(globals){ case (r,c) => typecheck(c,return_type,r,locals) }
          case Assign(d,v)
            => if (!typeMatch(typecheck(d,globals,locals),typecheck(v,globals,locals)))
                  throw new Error("Incompatible source in assignment: "+s)
               else globals
          case CodeE(e)
            => globals    // don't typecheck e
          case IfS(p,x,y)
            => if (typecheck(p,globals,locals) != boolType)
                  throw new Error("The if-statement condition "+p+" must be a boolean")
               typecheck(x,return_type,globals,locals)
               typecheck(y,return_type,globals,locals)
          case ForS(v,a,b,c,u)
            => val at = typecheck(a,globals,locals)
               val bt = typecheck(b,globals,locals)
               val ct = typecheck(c,globals,locals)
               if (at != longType && at != intType)
                  throw new Error("For loop "+s+" must use an integer or long initial value: "+a)
               else if (bt != longType && bt != intType)
                  throw new Error("For loop "+s+" must use an integer or long final value: "+b)
               else if (ct != longType && ct != intType)
                  throw new Error("For loop "+s+" must use an integer or long step: "+c)
               else typecheck(u,return_type,globals,locals+((v,longType)))
          case ForeachS(v,c,b)
            => typecheck(c,globals,locals) match {
                  case ParametricType("map",List(t1,t2))
                    => typecheck(b,return_type,globals,locals+((v,TupleType(List(t1,t2)))))
                  case ParametricType(f,List(tp))
                    if isCollection(f)
                    => typecheck(b,return_type,globals,locals+((v,tp)))
                  case tp => throw new Error("Foreach statement must be over a collection: "+s+" (found "+tp+")")
               }
          case WhileS(p,b)
            => if (typecheck(p,globals,locals) != boolType)
                  throw new Error("The while-statement condition "+p+" must be a boolean")
               typecheck(b,return_type,globals,locals)
          case Return(e)
            => if (return_type == null)
                  throw new Error("A Return statement can only appear inside a function body: "+s)
               if (!typeMatch(typecheck(e,globals,locals),return_type))
                  throw new Error(e+" must return a value of type: "+return_type)
               globals
          case _ => throw new Error("Illegal statement: "+s)
    } } catch { case m: Error => throw new Error(m.getMessage+"\nFound in: "+s) }

    val globalEnv: Environment = Map()
    val localEnv: Environment = Map()

    def typecheck ( e: Expr, globals: Environment ): Type = typecheck(e,globals,localEnv)

    def typecheck ( s: Stmt ) { typecheck(s,null,globalEnv,localEnv) }
}
