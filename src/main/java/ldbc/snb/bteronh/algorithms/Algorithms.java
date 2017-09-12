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
import java.util.*;

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

    public static RandomVariateGen GetDegreeSequenceSampler(List<Integer> observedSequence, long numNodes, int seed ) {

        System.out.println("Creating sampler for degree sequence generation");
        observedSequence.sort( new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        });
        double [] sequence = new double[observedSequence.size()];
        for(int i = 0; i < observedSequence.size(); ++i){
            sequence[i] = observedSequence.get(i);
        }
        EmpiricalDist degreeDistribution = new EmpiricalDist(sequence);


        LFSR113 random = new LFSR113();
        int [] seeds = new int[4];
        seeds[0] = 128+seed;
        seeds[1] = 128+seed;
        seeds[2] = 128+seed;
        seeds[3] = 128+seed;
        //LFSR113.setPackageSeed(seeds);
        random.setSeed(seeds);
        RandomVariateGen randomVariateGen = new RandomVariateGen(random,degreeDistribution);
        return randomVariateGen;
    }

    public static double [] GenerateCCperDegree( ArrayList<Pair<Long,Double>> ccDistribution, int maxDegree) {

        System.out.println("Loading CC distribution");
        double [] cc = new double[maxDegree+1];
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
        /*int lowerBound = 0;
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
        */
        int res = Arrays.binarySearch(cumulative, randomDis);
        if(res < 0) {
            return -(res+1);
        }
        return res;
    }

    public static Edge BTERSample(BTERStats stats, Random random) throws IOException {

        long totalWeight = stats.getWeightPhase1()+stats.getWeightPhase2();
        double prob = random.nextDouble();
        if(prob < stats.getWeightPhase1()/(double)(totalWeight)) {
            return BTERSamplePhase1(stats,random);
        }
        return BTERSamplePhase2(stats,random);
    }


    public static Edge BTERSamplePhase1(BTERStats stats, Random random) throws IOException{
        int group = SampleCumulative(stats.getCumulativeGroups(),random);
        double r1 = random.nextDouble();
        long offset = (stats.getGroupIndex(group) + (long)Math.floor(r1*stats.getGroupNumBuckets(group))*stats.getGroupBucketSize(group));
        double r2 = random.nextDouble();
        long firstNode = (long)Math.floor(r2*stats.getGroupBucketSize(group)) + offset;
        double r3 = random.nextDouble();
        long secondNode = (long)Math.floor(r3*(stats.getGroupBucketSize(group)-1)) + offset;
        if( secondNode >= firstNode )  {
            secondNode+=1;
        }
        return new Edge(firstNode,secondNode);
    }

    public static Edge BTERSamplePhase2(BTERStats stats, Random random) throws IOException {
        long firstNode = BTERSampleNodePhase2(stats, random);
        long secondNode = BTERSampleNodePhase2(stats, random);
        return new Edge(firstNode, secondNode);
    }

    public static long BTERSampleNodePhase2(BTERStats stats, Random random) {
        int degree = SampleCumulative(stats.getCumulativeDegrees(),random);
        double r1 = random.nextDouble();
        double r2 = random.nextDouble();
        if(r1 < stats.getDegreeWeightRatio(degree)) {
            return (long)Math.floor(r2*stats.getDegreeNFill(degree)) + stats.getDegreeIndex(degree);
        } else {
            return (long)Math.floor(r2*(stats.getDegreeN(degree) - stats.getDegreeNFill(degree))) + stats.getDegreeIndex(degree) + stats.getDegreeNFill(degree);
        }
    }
}
