/**
  * Created by ashiqimran on 3/6/17.
    */
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkConf

  object Graph2 {
    def main(args: Array[ String ]) {
          val conf = new SparkConf().setAppName("Graph")
          conf.setMaster("local[2]")
          val sc = new SparkContext(conf)
          var m = sc.textFile("small-graph.txt")
            .map( line => { val a = line.split(",").toList
              (a.head.toLong,a.head.toLong,a.tail.map(_.toLong))
            } )
          for(i <- 1 to 5)
            m = m.flatMap{ case (group,id,adj) => (id,group)::adj.map((_,group)) }
              .reduceByKey(_ min _)
              .join(m.map{ case (_,id,adj) => (id,adj) })
              .map{ case (id,(group,adj)) => (group,id,adj) }
          m.map{ case (group,id,adj) => (group,id) }
            .countByKey.foreach(println)
          sc.stop()
        }

  }
