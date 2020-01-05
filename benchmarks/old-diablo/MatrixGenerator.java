/* Create matrixes for multiplication/addition
*/

import java.io.*;
import java.util.*;

public class MatrixGenerator {
    static Random rand = new Random();

    static void randomMatrix ( int N, int M, int max, String file ) throws IOException {
        BufferedWriter m = new BufferedWriter(new FileWriter(file));
        List<Integer> l = new ArrayList<Integer>();
        for ( int i = 0; i < N-1; i++ )
            l.add(i);
        Collections.shuffle(l);
        List<Integer> r = new ArrayList<Integer>();
        for ( int i = 0; i < M-1; i++ )
            r.add(i);
        Collections.shuffle(r);
        for ( Integer i: l )
            for ( Integer j: r )
                m.write(i+","+j+","+(rand.nextDouble()*max)+"\n");
        m.close();
    }

    public static void main ( String[] args ) throws Exception {
        int n = Integer.parseInt(args[0]);
        randomMatrix(n,n,10,args[1]);
        randomMatrix(n,n,10,args[2]);
    }
}
