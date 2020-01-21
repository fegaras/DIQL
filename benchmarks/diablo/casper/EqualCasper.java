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
    
    public static boolean equal(JavaRDD<java.lang.String> rdd_0_0) {
        boolean equal = false;
        equal = true;
        String val_final = rdd_0_0.first();
        {

JavaPairRDD<Integer, Boolean> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<java.lang.String, Integer, Boolean>() {
	public Iterator<Tuple2<Integer, Boolean>> call(java.lang.String data_i) throws Exception {
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
