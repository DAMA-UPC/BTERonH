package ldbc.snb.bteronh;

import javafx.util.Pair;
import ldbc.snb.bteronh.algorithms.Algorithms;
import ldbc.snb.bteronh.structures.BTERStats;
import umontreal.iro.lecuyer.probdist.EmpiricalDist;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;
import umontreal.iro.lecuyer.rng.LFSR113;
import umontreal.iro.lecuyer.rng.RandomStream;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

class BTERMain {
    public static void main(String [] args) {
        EmpiricalDist degreeDistribution = null;
        try {
            String graphPath = "/degreeSequences/"+args[1];
            System.out.println("Loading empirical distribution from "+graphPath);

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(BTERMain.class.getResourceAsStream(graphPath), "UTF-8"));
            String line;
            ArrayList<Integer> fileData = new ArrayList<Integer>();
            while ((line = reader.readLine()) != null) {
                fileData.add(Integer.parseInt(line));
            }
            reader.close();
            double [] degreeSequence = new double[fileData.size()];
            int index = 0;
            for(Integer i : fileData) {
                degreeSequence[index] = i;
                index++;
            }

            Arrays.sort(degreeSequence);
            degreeDistribution = new EmpiricalDist(degreeSequence);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println("Generating Degree Sequence");

        RandomStream random = new LFSR113();
        RandomVariateGen randomVariateGen = new RandomVariateGen(random,degreeDistribution);
        int maxDegree = Integer.MIN_VALUE;
        int numNodes = Integer.parseInt(args[2]);
        int [] degreeSequence = new int[numNodes];
        for(int i = 0; i < numNodes; ++i) {
            degreeSequence[i] = (int)randomVariateGen.nextDouble();
            maxDegree = degreeSequence[i] > maxDegree ? degreeSequence[i] : maxDegree;
        }

        double [] cc = new double[maxDegree+1];
        System.out.println("Loading CC distribution");
        ArrayList<Pair<Long,Double>> ccDistribution = new ArrayList<Pair<Long,Double>>();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(BTERMain.class.getResourceAsStream("/CCs/"+args[1]), "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                String data[] = line.split(" ");
                ccDistribution.add(new Pair<Long, Double>(Long.parseLong(data[0]), Double.parseDouble(data[1])));
            }
            reader.close();
        } catch( IOException e) {
            e.printStackTrace();
        }

        cc[0] = 0.0;
        cc[1] = 0.0;
        for(int i = 2; i < maxDegree+1; ++i) {
            int degree = i;
            int pos = Algorithms.BinarySearch(ccDistribution,(long)degree);
            if(ccDistribution.get(pos).getKey() == degree || pos == (ccDistribution.size() - 1)) {
                cc[degree] = ccDistribution.get(pos).getValue();
            } else if( pos < ccDistribution.size() - 1 ){
                long min = ccDistribution.get(pos).getKey();
                long max = ccDistribution.get(pos+1).getKey();
                double ratio = (degree - min) / (max - min);
                double minCC = ccDistribution.get(pos).getValue();
                double maxCC = ccDistribution.get(pos+1).getValue();
                double cc_current = ratio * (maxCC - minCC ) + minCC;
                cc[degree] = cc_current;
            }
        }

        BTERStats stats = new BTERStats();
        System.out.println("Initializing BTER stats");
        stats.initialize(degreeSequence,cc);

        try {
            System.out.println("Generating edges");
            FileOutputStream fileOutputStream = new FileOutputStream("graph.dat");
            Algorithms.BTERSample(stats,fileOutputStream);
            fileOutputStream.close();

        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
}
