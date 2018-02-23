package ldbc.snb.bteronhplus.algorithms;

import ldbc.snb.bteronhplus.structures.BTERStats;
import ldbc.snb.bteronhplus.structures.Edge;
import org.apache.commons.math3.util.Pair;
import umontreal.iro.lecuyer.probdist.EmpiricalDist;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;
import umontreal.iro.lecuyer.rng.LFSR113;

import java.io.IOException;
import java.util.*;

/**
 * Created by aprat on 14/07/16.
 */
public class BTER {
    public static int binarySearch(ArrayList<Pair<Integer,Double>> array, Integer degree) {

        int pos = Collections.binarySearch(array, new Pair<Integer,Double>(degree, 0.0), new Comparator<Pair<Integer,
                Double>> ( ){

            @Override
            public int compare(Pair<Integer, Double> o1, Pair<Integer, Double> o2) {
                if(o1.getKey() < o2.getKey()) return -1;
                return 1;
            }
        });

        if(pos < 0) {
            return -(pos+1);
        }
        return pos;
    }

    public static RandomVariateGen getDegreeSequenceSampler(List<Integer> observedSequence, long numNodes, int seed) {

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

    public static double [] generateCCperDegree(HashMap<Integer,Double> ccPerDegree, int maxDegree) {

        ArrayList<Pair<Integer,Double>> ccDistribution = new ArrayList<Pair<Integer,Double>>();
        for(Map.Entry<Integer,Double> entry : ccPerDegree.entrySet()) {
            ccDistribution.add(new Pair<Integer,Double>(entry.getKey(),entry.getValue()));
        }

        Collections.sort(ccDistribution, new Comparator<Pair<Integer,Double>>() {

            @Override
            public int compare(Pair<Integer, Double> pair1, Pair<Integer, Double> pair2) {
                return pair1.getFirst() - pair2.getFirst();
            }
        });

        System.out.println("Loading CC distribution");
        double [] cc = new double[maxDegree+1];
        cc[0] = 0.0;
        cc[1] = 0.0;
        for(int i = 2; i < maxDegree+1; ++i) {
            int degree = i;
            int pos = BTER.binarySearch(ccDistribution,degree);
            if(ccDistribution.get(pos).getKey() == degree || pos == (ccDistribution.size() - 1)) {
                cc[degree] = ccDistribution.get(pos).getValue();
            } else if( pos < ccDistribution.size() - 1 ){
                long min = ccDistribution.get(pos).getKey();
                long max = ccDistribution.get(pos+1).getKey();
                double ratio = (degree - min) / (double)(max - min);
                double minCC = ccDistribution.get(pos).getValue();
                double maxCC = ccDistribution.get(pos+1).getValue();
                double cc_current = ratio * (maxCC - minCC ) + minCC;
                cc[degree] = cc_current;
            }
        }
        return cc;
    }



    public static int sampleCumulative(double [] cumulative, Random random) {
        double randomDis = random.nextDouble();
        int res = Arrays.binarySearch(cumulative, randomDis);
        if(res < 0) {
            return -(res+1);
        }
        return res;
    }

    public static Edge BTERSample(BTERStats stats, Random random, long nodeIdOffset) throws IOException {

        long totalWeight = stats.getWeightPhase1()+stats.getWeightPhase2();
        double prob = random.nextDouble();
        if(prob < stats.getWeightPhase1()/(double)(totalWeight)) {
            return BTERSamplePhase1(stats,random, nodeIdOffset);
        }
        return BTERSamplePhase2(stats,random, nodeIdOffset);
    }


    public static Edge BTERSamplePhase1(BTERStats stats, Random random, long nodeIdOffset) throws IOException{
        int group = sampleCumulative(stats.getCumulativeGroups(),random);
        double r1 = random.nextDouble();
        long offset = (stats.getGroupIndex(group) + (long)Math.floor(r1*stats.getGroupNumBuckets(group))*stats.getGroupBucketSize(group));
        double r2 = random.nextDouble();
        long firstNode = (long)Math.floor(r2*stats.getGroupBucketSize(group)) + offset;
        double r3 = random.nextDouble();
        long secondNode = (long)Math.floor(r3*(stats.getGroupBucketSize(group)-1)) + offset;
        if( secondNode >= firstNode )  {
            secondNode+=1;
        }
        return new Edge(nodeIdOffset+firstNode,nodeIdOffset+secondNode);
    }

    public static Edge BTERSamplePhase2(BTERStats stats, Random random, long nodeIdOffset) throws IOException {
        long firstNode = BTERSampleNodePhase2(stats, random);
        long secondNode = BTERSampleNodePhase2(stats, random);
        return new Edge(nodeIdOffset+firstNode, nodeIdOffset+secondNode);
    }

    public static long BTERSampleNodePhase2(BTERStats stats, Random random) {
        int degree = sampleCumulative(stats.getCumulativeDegrees(),random);
        double r1 = random.nextDouble();
        double r2 = random.nextDouble();
        if(r1 < stats.getDegreeWeightRatio(degree)) {
            return (long)Math.floor(r2*stats.getDegreeNFill(degree)) + stats.getDegreeIndex(degree);
        } else {
            return (long)Math.floor(r2*(stats.getDegreeN(degree) - stats.getDegreeNFill(degree))) + stats.getDegreeIndex(degree) + stats.getDegreeNFill(degree);
        }
    }
}
