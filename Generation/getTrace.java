import java.io.*;
import java.util.*;

/**
 * Created by lcheng on 5/27/2017.
 */
public class getTrace {
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        String log = args[0];
        String otr = args[1];
        int N = Integer.valueOf(args[2]);
        //String path = "/home/lcheng/HybridM/dataset/"; // path where data will be generated

        FileWriter fw = new FileWriter(new File(otr + "_" + N + ".tr"));
        BufferedWriter out = new BufferedWriter(fw);

        String line;
        List<String> trace = new ArrayList<String>();
        BufferedReader r1 = new BufferedReader(new FileReader(new File(log)));
        //BufferedReader r1 = new BufferedReader(new FileReader(new File(path + "event3.log")));
        String tr;
        while ((line = r1.readLine()) != null) {
            int idx = line.indexOf(",");
            tr=line.substring(idx+1);
            trace.add(tr);
        }
        r1.close();
        System.out.println("the number of traces is " + trace.size());

        //output the trace in a required format
        int count = 0;
        for (int x = 0; x < N; x++) {
            for(String tra : trace){
                out.write(String.valueOf(count));
                out.write("," + tra);
                out.write("\n");
                count++;
            }
            out.flush();
        }

        out.close();
        System.out.println("job is done, number of traces are " + count);

    }
}

