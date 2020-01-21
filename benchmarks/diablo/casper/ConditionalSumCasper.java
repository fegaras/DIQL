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

public class ConditionalSumCasper {
    
    public static double sumList(JavaRDD<java.lang.Double> rdd_0_0) {
        double sum = 0;
        sum = 0;
        {

JavaPairRDD<Integer, Double> mapEmits = rdd_0_0.flatMapToPair(new PairFlatMapFunction<java.lang.Double, Integer, Double>() {
	public Iterator<Tuple2<Integer, Double>> call(java.lang.Double data_i) throws Exception {
		List<Tuple2<Integer, Double>> emits = new ArrayList<Tuple2<Integer, Double>>();
		
		if(data_i < 100) emits.add(new Tuple2(0,data_i));

		
		return emits.iterator();
	}
});

JavaPairRDD<Integer, Double> reduceEmits = mapEmits.reduceByKey(new Function2<Double,Double,Double>(){
	public Double call(Double val1, Double val2) throws Exception {
		return val2 + val1;
	}
});

Map<Integer, Double> output_rdd_0_0 = reduceEmits.collectAsMap();
sum = output_rdd_0_0.get(0);
        }
        return sum;
    }
    
    public ConditionalSumCasper() { super(); }
}
