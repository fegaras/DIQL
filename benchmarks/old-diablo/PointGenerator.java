/* Points in a 100*100 grid used by KMeans
*/

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Random;

public class PointGenerator {
    static Random rand = new Random();

    static double getd () {
        double v = rand.nextDouble()*20.0D;
        return ((int)v % 2 == 0) ? getd() : v;
    }

    public static void main ( String[] args ) throws Exception {
        long points = Long.parseLong(args[0]);
        BufferedWriter centroids = new BufferedWriter(new FileWriter(args[1]));
        for ( int i = 0; i < 10; i++ )
            for ( int j = 0; j < 10; j++ )
                centroids.write((i*2+1.2)+","+(j*2+1.2)+"\n");
        centroids.close();
        BufferedWriter data = new BufferedWriter(new FileWriter(args[2]));
        for ( long i = 0; i < points; i++ ) {
            double x = getd();
            double y = getd();
            data.write(x+","+y+"\n");
        }
        data.close();
    }
}
