package ldbc.snb.bteronh.algorithms;

import javafx.util.Pair;
import ldbc.snb.bteronh.structures.BTERStats;
import org.apache.commons.math3.random.EmpiricalDistribution;
import umontreal.iro.lecuyer.probdist.EmpiricalDist;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
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

    public static EmpiricalDistribution buildDistributionFromFrequencies(double [] input) {
        double smallestGroupWeight = 0;
        double totalGroupWeight = 0;
        for(int i = 0; i < input.length; ++i) {
            totalGroupWeight += input[i];
            smallestGroupWeight = input[i] < smallestGroupWeight ? input[i] : smallestGroupWeight;
        }
        int factor = Math.max((int)Math.ceil(1/(smallestGroupWeight/totalGroupWeight)), 100000);
        int amount = 0;
        for(int i = 0; i < input.length; ++i ) {
            amount += (int)Math.ceil(factor*input[i]);
        }
        double [] data = new double[amount];
        for(int i = 0; i < input.length; ++i ) {
            int num = (int) Math.ceil(factor * input[i]);
            for(int j = 0; j < num; ++i) {
                data[j] = i;
            }
        }
        EmpiricalDistribution distribution = new EmpiricalDistribution();
        distribution.load(data);
        return distribution;
    }

    public static void BTERSample(BTERStats stats, OutputStream ostream) throws IOException {
        EmpiricalDistribution groupDistribution = buildDistributionFromFrequencies(stats.getGroupWeights());
        EmpiricalDistribution degreeDistribution = buildDistributionFromFrequencies(stats.getDegreeWeights());
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        int numGroups = stats.getNumGroups();
        int weightPhase1 = 0;
        int weightPhase2 = 0;
        for(int i = 0; i < numGroups; ++i) {
            weightPhase1+=stats.getGroupWeight(i);
            weightPhase2+=stats.getDegreeWBulk(i) + stats.getDegreeWFill(i);
        }
        int totalWeight = weightPhase1+weightPhase2;
        for(int i = 0; i < totalWeight; ++i) {
            double prob = random.nextDouble();
            if(prob < weightPhase1/(double)(weightPhase1+weightPhase2)) {
                BTERSamplePhase1(stats, groupDistribution, ostream);
            } else {
                BTERSamplePhase2(stats,degreeDistribution, ostream);
            }
        }
    }


    public static void BTERSamplePhase1(BTERStats stats, EmpiricalDistribution groupDistribution, OutputStream ostream) throws IOException{
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        int group = (int)groupDistribution.getNextValue();
        double r1 = random.nextDouble();
        int offset = stats.getGroupIndex(group) + (int)Math.floor(r1*stats.getGroupBucketSize(group))*stats.getGroupNumBuckets(group);
        double r2 = random.nextDouble();
        int firstNode = (int)Math.floor(r2*stats.getGroupBucketSize(group)) + offset;
        double r3 = random.nextDouble();
        int secondNode = (int)Math.floor(r2*stats.getGroupBucketSize(group)) + offset;
        if( secondNode >= firstNode )  {
            secondNode+=1;
        }
        String edge = new String(firstNode+" "+secondNode);
        try {
            ostream.write(edge.getBytes());
        }catch(IOException e) {
            throw e;
        }
    }

    public static void BTERSamplePhase2(BTERStats stats, EmpiricalDistribution degreeDistribution, OutputStream ostream) throws IOException {
        int firstNode = BTERSampleNodePhase2(stats,degreeDistribution,ostream);
        int secondNode = BTERSampleNodePhase2(stats,degreeDistribution,ostream);
        String edge = new String(firstNode+" "+secondNode);
        try {
            ostream.write(edge.getBytes());
        }catch(IOException e) {
            throw e;
        }
    }

    public static int BTERSampleNodePhase2(BTERStats stats, EmpiricalDistribution degreeDistribution, OutputStream ostream) {
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        int degree = (int)degreeDistribution.getNextValue();
        double r1 = random.nextDouble();
        double r2 = random.nextDouble();
        if(r1 < stats.getDegreeWeightRatio(degree)) {
            return (int)Math.floor(r2*stats.getDegreeNFill(degree)) + stats.getDegreeIndex(degree);
        } else {
            return (int)Math.floor(r2*(stats.getDegreeN(degree) - stats.getDegreeNFill(degree))) + stats.getDegreeIndex(degree) + stats.getDegreeNFill(degree);
        }
    }
}
