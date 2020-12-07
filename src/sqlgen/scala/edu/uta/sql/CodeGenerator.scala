/*
 * Copyright © 2020 University of Texas at Arlington
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

package edu.uta.sql

import scala.collection.mutable.HashMap

object CodeGenerator {
  var hm: HashMap[String,String] = HashMap.empty[String,String]
  var hm2: HashMap[String,String] = HashMap.empty[String,String]
  var hm3: HashMap[String,String] = HashMap.empty[String,String]


  def query(e: sql.Expr): String = {
    e match {
      case sql.Query(s, f, w, g) =>
        s match {
          case sql.Select(e) =>
            f match {
              case sql.From(t) => {
                w match {
                  case Some(sql.Where(q)) =>
                    g match {
                      case Some(sql.GroupBy(gb)) =>
                        "SELECT " + e.map(query).mkString("") +
                          " FROM " + t.map(query).mkString("") +
                          " WHERE " + query(sql.Where(q)) +
                          query(sql.GroupBy(gb))

                    }
                  case _ => ""
                }
              }
            }
        }


      case sql.Where(q) =>
        if (q.last.toString == "Empty()") q.map(query).mkString(" ")
        else //interpolation to do
          q.map(query).mkString(" AND ")

      case sql.GroupBy(g) =>
        g match {
          case List(sql.Empty()) => ""
          case _ =>
            hm.clear
            " GROUP BY " + g.map(query).mkString(" ")
        }

      case sql.Attr(c, ac, a) =>
        ac match {
          case Some(x) =>
            a match {
              case Some(y) =>
                val com1 = c match {
                  case sql.Empty() => ""
                  case _ =>
                    ac match {
                      case Some(List(sql.Empty())) =>
                        a match {
                          case Some(List(sql.Empty())) => ""
                          case _ => ", "
                        }
                      case _ =>
                        ", "
                    }
                }

                val com2 = ac match {
                  case Some(List(sql.Empty())) =>
                    a match {
                      case Some(List(sql.Empty())) => ""
                      case _ => ""
                    }
                  case _ =>
                    a match {
                      case Some(List(sql.Empty())) => ""
                      case _ => ", "
                    }
                }

                var aggc =
                  ac match {
                    case Some(List(sql.Empty())) => ""
                    case Some(x) =>
                      val r = x.map(query).mkString(" ")
                      val aggcs = r.split(",")

                      aggcs match {
                        case x if aggcs.length == 1 => x.mkString("") + " AS _1"
                        case x if aggcs.length > 1 =>
                          var count = 0
                          var aggcl = aggcs.map {
                            case x =>
                              count += 1
                              x + s" AS _1_$count"
                          }
                          aggcl.mkString(", ")
                      }
                  }

                val aggf =
                  a match {
                    case Some(List(sql.Empty())) => ""
                    case Some(y) => y.map(query).mkString(" ") + " AS _2"
                  }

                query(c) + com1 + aggc + com2 + aggf
            }
        }

      case sql.Column(cols) =>
        val cols2 = cols.filterNot(elm => elm == sql.Empty())
        if(cols2.length == 1) {
          cols2.map(query).mkString(", ")
        }
        else {
          //          cols2.map(query).zipWithIndex.map { case (line, i) =>
          //            val indx = i + 1
          //            line + s" AS _$indx "}.mkString(", ")
          //        }
          cols2 match {
            case List(sql.Tuple(x), sql.Column(y)) =>
              x.map(query).zipWithIndex.map {
              case (line, i) =>
                val indx = i + 1
                line + s" AS _1_$indx "
            }.mkString(", ") + "," + y.map(query).mkString(" ") + " AS _2 "
            case _ => cols2.map(query).zipWithIndex.map { case (line, i) =>
              val indx = i + 1
              line + s" AS _$indx "}.mkString(", ")
          }
        }
//        val test = cols2.map(query)
//        println("test")
//        println(test)
//        if (test.length == 1){
//          //cols2.map(query).mkString(", ")
//          test.mkString(", ")
//        }
//        else {
//                  //cols2.map(query).zipWithIndex.map{case (line, i) =>
//
//                  test.zipWithIndex.map{case (line, i) =>
//                    println(test)
//                    val indx = i+1
//                    line + s" AS _1_$indx "}.mkString(", ")
//        }


      case sql.Conj(c, n, cn) => c.map(query).mkString(" ") + " " + n + " " + cn.map(query).mkString(" ")
      case sql.From(j) => j.map(query).mkString(" ")

      case sql.Join(t1, t2, c) =>
        if (hm.contains(t1)&&(!hm.contains(t2))) {
          hm += (t2 -> "true")
          " JOIN " + t2 + " ON " + query(c)
        }
        else if (hm.contains(t2)&&(!hm.contains(t1))) {
          hm += (t1 -> "true")
          " JOIN " + t1 + " ON " + query(c)
        }
        else if (!(hm.contains(t1)) && (!hm.contains(t2))){
          hm += (t1 -> "true", t2 -> "true")
          if (t1.takeRight(1).matches("^\\d+$")) {
            if (t2.takeRight(1).matches("^\\d+$")) {
              t1.dropRight(1) + " " + t1 + " JOIN " + t2.dropRight(1) + " " + t2 + " ON " + query(c)
            }
            else t1.dropRight(1) + " " + t1 + " join " + " " + t2 + " on " + query(c)

          }
          else t1 + " JOIN " + t2 + " ON " + query(c)
        }
        else {
            " AND " + query(c)
    }

      case sql.Cond(l, o, r) => query(l) + o + query(r)
      case sql.MethodCall(o, m, a) =>
        o match {
          case sql.MethodCall(_, _, _) => "(" + query(o) + ")" + " " + m + " " + "(" + a.map(query).mkString(" ") + ")"
          case _ =>
            query(o) + " " + m + " " + a.map(query).mkString(" ")
        }

      case sql.Call(n, a) => n + "(" + a.map(query).mkString("") + ")"
      case sql.Var(v) => v.toString
      case sql.Nth(t, n) =>
        t match{
          case sql.Nth(sql.Var(v), m) =>
            if(hm2.contains(v)){
              v + "._"+m+"_"+n
          }
            else query(t) + "._" + n
          case _ => query(t) + "._" + n
    }

      case sql.Project(c1, c2) => query(c1) + "." + c2
      case sql.Tuple(ts) => ts.map(query).mkString(", ")
      case sql.IntConst(v) => v.toString
      case sql.DoubleConst(v) => v.toString
      case sql.BoolConst(v) => v.toString
      case _ => ""
    }
  }

  def translate_query(e: sql.Expr): sql.Expr ={
    e match {
      case sql.Assignment(v, sql.Query(a, b, c, d)) =>{
        if (!hm2.contains(v)) {
          hm2 += (v -> "true")
        }
        val qs = query(sql.Query(a, b, c, d))
        sql.Assignment(v, sql.StringConst(qs))
    }
      case sql.Assignment(v, sql.MethodCall(o, m, List(l))) =>
        sql.Assignment(v, sql.MethodCall(o, m, List(l)))

      case sql.Call(x,y) => sql.Call(x,y)
        //sql.Empty()

      case _ => sql.Empty()
    }
  }

  def translate(e: sql.Expr): List[sql.Expr] ={
    e match {
      case sql.listExp(le) =>
        le.map(translate_query)

    }

  }

}
