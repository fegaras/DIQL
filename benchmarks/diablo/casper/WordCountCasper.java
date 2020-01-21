import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.lang.Integer;
import java.lang.String;
import java.util.Iterator;

public class WordCountCasper {

    static Map<String,Integer> counts = new HashMap<String,Integer>();

    public static Map<String,Integer> countWords(JavaRDD<java.lang.String> rdd_0_0) {
        {

JavaPairRDD<Tuple2<Integer,String>, java.lang.Integer> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<java.lang.String, Tuple2<Integer,String>, java.lang.Integer>() {
	public Iterator<Tuple2<Tuple2<Integer,String>, java.lang.Integer>> call(java.lang.String words_j) throws Exception {
		List<Tuple2<Tuple2<Integer,String>, java.lang.Integer>> emits = new ArrayList<Tuple2<Tuple2<Integer,String>, java.lang.Integer>>();
		
		emits.add(new Tuple2(new Tuple2(0,words_j), 1));

		
		return emits.iterator();
	}
});

JavaPairRDD<Tuple2<Integer,String>, java.lang.Integer> reduceEmits = mapEmits.reduceByKey(new Function2<java.lang.Integer,java.lang.Integer,java.lang.Integer>(){
	public java.lang.Integer call(java.lang.Integer val1, java.lang.Integer val2) throws Exception {
		return val2 + val1;
	}
});

Map<Tuple2<Integer,String>, java.lang.Integer> output_rdd_0_0 = reduceEmits.collectAsMap();
for(Tuple2<Integer,String> output_rdd_0_0_k : output_rdd_0_0.keySet()){
	counts.put(output_rdd_0_0_k._2, output_rdd_0_0.get(output_rdd_0_0_k));
};
        }
        return counts;
    }
    
    public WordCountCasper() { super(); }
}
