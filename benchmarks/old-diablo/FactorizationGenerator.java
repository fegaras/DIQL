
/* Create matrixes for matrix factorization
*/

import java.io.*;
import java.util.*;

public class FactorizationGenerator {
    static Random rand = new Random();

    static void randomMatrix ( int N, int M, String file ) throws IOException {
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
                if (rand.nextDouble() > 0.1)
                   m.write(i+","+j+","+((int)(Math.floor(rand.nextDouble()*5+1)))+"\n");
        m.close();
    }

    public static void main ( String[] args ) throws Exception {
        int n = Integer.parseInt(args[0]);
        randomMatrix(n,n,args[1]);
    }
}
