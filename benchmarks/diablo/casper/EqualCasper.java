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
import java.lang.Integer;
import java.util.Iterator;

public class EqualCasper {
    
    public static void main(String[] args) {
        List<Integer> numbers = null;
        numbers = Arrays.asList(10, 10, 5, 10, 10);
        equal(numbers);
    }
    
    public static boolean equal(List<Integer> data) {
        boolean equal = false;
        equal = true;
        int val = 0;
        val = data.get(0);
        {
            int i = 0;
            i = 0;
            boolean loop$0 = false;
            loop$0 = false;
            SparkConf conf = new SparkConf().setAppName("spark");
JavaSparkContext sc = new JavaSparkContext(conf);

JavaRDD<java.lang.Integer> rdd_0_0 = sc.parallelize(data);

final boolean loop0_final = loop$0;
final int val_final = val;


JavaPairRDD<Integer, Boolean> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<java.lang.Integer, Integer, Boolean>() {
	public Iterator<Tuple2<Integer, Boolean>> call(java.lang.Integer data_i) throws Exception {
		List<Tuple2<Integer, Boolean>> emits = new ArrayList<Tuple2<Integer, Boolean>>();
		
		if(val_final != data_i) emits.add(new Tuple2(0,false));

		
		return emits.iterator();
	}
});

JavaPairRDD<Integer, Boolean> reduceEmits = mapEmits.reduceByKey(new Function2<Boolean,Boolean,Boolean>(){
	public Boolean call(Boolean val1, Boolean val2) throws Exception {
		return val2 || val2;
	}
});

Map<Integer, Boolean> output_rdd_0_0 = reduceEmits.collectAsMap();
equal = output_rdd_0_0.get(0);
        }
        return equal;
    }
    
    public EqualCasper() { super(); }
}
