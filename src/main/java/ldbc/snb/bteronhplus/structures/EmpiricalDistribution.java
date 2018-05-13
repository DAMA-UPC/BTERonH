package ldbc.snb.bteronhplus.structures;

import ldbc.snb.bteronhplus.tools.FileTools;
import org.apache.hadoop.conf.Configuration;
import umontreal.iro.lecuyer.probdist.EmpiricalDist;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;
import umontreal.iro.lecuyer.rng.LFSR113;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class EmpiricalDistribution {

    RandomVariateGen randomVariateGen = null;

    public EmpiricalDistribution(String filename, Configuration conf, double min) {

        // reading the sequence from the file
        ArrayList<Double> sequence = new ArrayList<Double>();
        BufferedReader reader = null;
        try {
            reader = FileTools.getFile(filename, conf);
            String line;
            line = reader.readLine();
            while(line!=null) {
                double next = Double.parseDouble(line);
                if(next >= min) {
                    sequence.add(next);
                }
                line = reader.readLine();
            }
        } catch (IOException e){
            e.printStackTrace();
            System.exit(1);
        }
        initialize(sequence);
    }

    public EmpiricalDistribution(ArrayList<Double> sequence) {
        initialize(sequence);
    }

    private void initialize(ArrayList<Double> sequence ) {
        Collections.sort(sequence);

        double [] finalSequence = new double[sequence.size()];
        for(int i = 0; i < sequence.size(); ++i){
            finalSequence[i] = sequence.get(i);
        }
        EmpiricalDist distribution = new EmpiricalDist(finalSequence);


        LFSR113 random = new LFSR113();
        /*int [] seeds = new int[4];
        seeds[0] = 128+sequence.size();
        seeds[1] = 128+sequence.size()+1;
        seeds[2] = 128+sequence.size()+2;
        seeds[3] = 128+sequence.size()+3;
        random.setSeed(seeds);*/
        randomVariateGen = new RandomVariateGen(random,distribution);
    }

    public double getNext() {
        return randomVariateGen.nextDouble();
    }

}
