package ldbc.snb.bteronh.algorithms;

import cern.jet.random.Empirical;
import javafx.util.Pair;
import ldbc.snb.bteronh.structures.BTERStats;
import org.apache.commons.math3.random.EmpiricalDistribution;
import umontreal.iro.lecuyer.probdist.EmpiricalDist;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;
import umontreal.iro.lecuyer.rng.LFSR113;
import umontreal.iro.lecuyer.rng.RandomStream;

import java.io.IOException;
import java.io.OutputStream;
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

    public static void BTERSample(BTERStats stats, OutputStream ostream) throws IOException {
        Random random = new Random();
        random.setSeed(System.currentTimeMillis());
        int weightPhase1 = 0;
        int weightPhase2 = 0;
        for(int i = 0; i < stats.getNumGroups(); ++i) {
            weightPhase1+=stats.getGroupWeight(i);
        }

        for(int i = 0; i < (stats.getMaxDegree()+1); ++i) {
            weightPhase2+=stats.getDegreeWeight(i);
        }

        double [] cumulativeGroups = new double[stats.getNumGroups()];
        cumulativeGroups[0] = stats.getGroupWeight(0) / (double)weightPhase1;
        for(int i = 1; i < stats.getNumGroups(); ++i) {
            cumulativeGroups[i] = Math.min(cumulativeGroups[i-1] + stats.getGroupWeight(i) / (double)weightPhase1, 1.0);
        }

        double [] cumulativeDegrees = new double[stats.getMaxDegree()+1];
        cumulativeGroups[0] = stats.getDegreeWeight(0) / (double)weightPhase2;
        for(int i = 1; i < (stats.getMaxDegree()+1); ++i) {
            cumulativeDegrees[i] = Math.min(cumulativeDegrees[i-1] + stats.getDegreeWeight(i) / (double)weightPhase2,1.0);
        }

        int totalWeight = weightPhase1+weightPhase2;
        for(int i = 0; i < totalWeight; ++i) {
            double prob = random.nextDouble();
            if(prob < weightPhase1/(double)(totalWeight)) {
                BTERSamplePhase1(stats, cumulativeGroups, ostream,random);
            } else {
                BTERSamplePhase2(stats, cumulativeDegrees, ostream,random);
            }
        }
    }


    public static void BTERSamplePhase1(BTERStats stats, double [] cumulativeGroups, OutputStream ostream, Random random) throws IOException{
        int group = SampleCumulative(cumulativeGroups,random);
        double r1 = random.nextDouble();
        int offset = stats.getGroupIndex(group) + (int)Math.floor(r1*stats.getGroupNumBuckets(group))*stats.getGroupBucketSize(group);
        double r2 = random.nextDouble();
        int firstNode = (int)Math.floor(r2*stats.getGroupBucketSize(group)) + offset;
        double r3 = random.nextDouble();
        int secondNode = (int)Math.floor(r3*(stats.getGroupBucketSize(group)-1)) + offset;
        if( secondNode >= firstNode )  {
            secondNode+=1;
        }
        String edge = new String(firstNode+" "+secondNode+"\n");
        try {
            ostream.write(edge.getBytes());
        }catch(IOException e) {
            throw e;
        }
    }

    public static void BTERSamplePhase2(BTERStats stats, double [] cumulativeDegrees, OutputStream ostream, Random random) throws IOException {
        int firstNode = BTERSampleNodePhase2(stats,cumulativeDegrees,ostream, random);
        int secondNode = BTERSampleNodePhase2(stats,cumulativeDegrees,ostream, random);
        String edge = new String(firstNode+" "+secondNode+"\n");
        try {
            ostream.write(edge.getBytes());
        }catch(IOException e) {
            throw e;
        }
    }

    public static int BTERSampleNodePhase2(BTERStats stats, double [] cumulativeDegrees, OutputStream ostream, Random random) {
        int degree = SampleCumulative(cumulativeDegrees,random)+1;
        double r1 = random.nextDouble();
        double r2 = random.nextDouble();
        if(r1 < stats.getDegreeWeightRatio(degree)) {
            return (int)Math.floor(r2*stats.getDegreeNFill(degree)) + stats.getDegreeIndex(degree);
        } else {
            return (int)Math.floor(r2*(stats.getDegreeN(degree) - stats.getDegreeNFill(degree))) + stats.getDegreeIndex(degree) + stats.getDegreeNFill(degree);
        }
    }
}
