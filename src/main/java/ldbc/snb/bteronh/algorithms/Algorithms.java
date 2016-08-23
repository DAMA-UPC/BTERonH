package ldbc.snb.bteronh.algorithms;

import javafx.util.Pair;
import ldbc.snb.bteronh.structures.BTERStats;
import ldbc.snb.bteronh.structures.Edge;
import umontreal.iro.lecuyer.probdist.EmpiricalDist;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;
import umontreal.iro.lecuyer.rng.LFSR113;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

/**
 * Created by aprat on 14/07/16.
 */
public class Algorithms {
        public static int BinarySearch(ArrayList<Pair<Long,Double>> array, Long degree) {

        int min = 0;
        int max = array.size();
        while(min <= max) {
            int midPoint = (max - min) / 2 + min;
            if(midPoint >= array.size()) return array.size()-1;
            if(midPoint < 0) return 0;
            if(array.get(midPoint).getKey() > degree ) {
                max = midPoint - 1;
            } else if(array.get(midPoint).getKey() < degree) {
                min = midPoint + 1;
            } else {
                return midPoint;
            }
        }
        return max;
    }

    public static int [] GenerateDegreeSequence( String empiricalDegreeSequenceFile, int numNodes, int seed ) {

        EmpiricalDist degreeDistribution = null;
        try {

            BufferedReader reader = new BufferedReader(new FileReader(empiricalDegreeSequenceFile));
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

        LFSR113 random = new LFSR113();
        int [] seeds = new int[4];
        seeds[0] = 128+seed;
        seeds[1] = 128+seed;
        seeds[2] = 128+seed;
        seeds[3] = 128+seed;
        //LFSR113.setPackageSeed(seeds);
        random.setSeed(seeds);
        RandomVariateGen randomVariateGen = new RandomVariateGen(random,degreeDistribution);
        int maxDegree = Integer.MIN_VALUE;
        int [] degreeSequence = new int[numNodes];
        for(int i = 0; i < numNodes; ++i) {
            degreeSequence[i] = (int)randomVariateGen.nextDouble();
            maxDegree = degreeSequence[i] > maxDegree ? degreeSequence[i] : maxDegree;
        }
        return degreeSequence;
    }

    public static double [] GenerateCCperDegree( String empiricalCCperDegreeFile, int maxDegree) {

        double [] cc = new double[maxDegree+1];
        System.out.println("Loading CC distribution");
        ArrayList<Pair<Long,Double>> ccDistribution = new ArrayList<Pair<Long,Double>>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(empiricalCCperDegreeFile));
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
        return cc;
    }



    public static int SampleCumulative(double [] cumulative, Random random) {
        double randomDis = random.nextDouble();
        int lowerBound = 0;
        int upperBound = cumulative.length-1;
        int midPoint = (upperBound + lowerBound) / 2;
        while (upperBound >= lowerBound) {
            if (cumulative[midPoint] < randomDis) {
                lowerBound = midPoint+1;
            } else if (cumulative[midPoint] > randomDis){
                upperBound = midPoint-1;
            } else {
                return midPoint;
            }
            midPoint = (upperBound + lowerBound) / 2;
        }
        return midPoint;
    }

    public static Edge BTERSample(BTERStats stats, Random random) throws IOException {

        int totalWeight = stats.getWeightPhase1()+stats.getWeightPhase2();
        double prob = random.nextDouble();
        if(prob < stats.getWeightPhase1()/(double)(totalWeight)) {
            return BTERSamplePhase1(stats,random);
        }
        return BTERSamplePhase2(stats,random);
    }


    public static Edge BTERSamplePhase1(BTERStats stats, Random random) throws IOException{
        int group = SampleCumulative(stats.getCumulativeGroups(),random);
        double r1 = random.nextDouble();
        int offset = stats.getGroupIndex(group) + (int)Math.floor(r1*stats.getGroupNumBuckets(group))*stats.getGroupBucketSize(group);
        double r2 = random.nextDouble();
        int firstNode = (int)Math.floor(r2*stats.getGroupBucketSize(group)) + offset;
        double r3 = random.nextDouble();
        int secondNode = (int)Math.floor(r3*(stats.getGroupBucketSize(group)-1)) + offset;
        if( secondNode >= firstNode )  {
            secondNode+=1;
        }
        return new Edge(firstNode,secondNode);
    }

    public static Edge BTERSamplePhase2(BTERStats stats, Random random) throws IOException {
        int firstNode = BTERSampleNodePhase2(stats, random);
        int secondNode = BTERSampleNodePhase2(stats, random);
        return new Edge(firstNode, secondNode);
    }

    public static int BTERSampleNodePhase2(BTERStats stats, Random random) {
        int degree = SampleCumulative(stats.getCumulativeDegrees(),random)+1;
        double r1 = random.nextDouble();
        double r2 = random.nextDouble();
        if(r1 < stats.getDegreeWeightRatio(degree)) {
            return (int)Math.floor(r2*stats.getDegreeNFill(degree)) + stats.getDegreeIndex(degree);
        } else {
            return (int)Math.floor(r2*(stats.getDegreeN(degree) - stats.getDegreeNFill(degree))) + stats.getDegreeIndex(degree) + stats.getDegreeNFill(degree);
        }
    }
}
