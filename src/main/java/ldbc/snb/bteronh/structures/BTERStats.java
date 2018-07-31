package ldbc.snb.bteronh.structures;

import javafx.util.Pair;
import ldbc.snb.bteronh.algorithms.Algorithms;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;

import java.util.*;
import java.util.function.Consumer;

/**
 * Created by aprat on 14/07/16.
 */
public class BTERStats {

    int maxDegree = Integer.MIN_VALUE;

    //Phase 1 data
    long [] i_g = null; //index of group in degree sequence
    long[] n_g = null; //number of buckets in group
    long[] b_g = null; //size of bucket in group
    double [] w_g = null; //weight of buckets in group

    //Phase 2 data
    long [] nfill_d = null;
    long [] nbulk_d = null;
    double [] wfill_d = null;
    double [] wbulk_d = null;
    double [] w_d = null;
    double [] r_d = null;
    long [] i_d = null;
    long [] n_d = null;


    double [] cumulativeGroups = null;
    double [] cumulativeDegrees = null;
    long weightPhase1;
    long weightPhase2;

    public void initialize(long numNodes, ArrayList<Integer> observedDegreeSequence, ArrayList<Pair<Long,Double>> observedCCPerDegree, int seed, Consumer<Long> continuation) {

        maxDegree = Integer.MIN_VALUE;
        RandomVariateGen randomVariateGen = Algorithms.GetDegreeSequenceSampler(observedDegreeSequence, numNodes, seed);

        System.out.println("Generating Degree Sequence");
        HashMap<Integer,Long> degrees = new HashMap<Integer,Long>();
        for(long i = 0; i < numNodes; ++i) {
            int degree = (int)randomVariateGen.nextDouble();
            degrees.compute(degree,(k,v)-> v == null ? 1 : v + 1 );
            maxDegree = degree > maxDegree ? degree : maxDegree;
            if(i % 1000000 == 0) {
                continuation.accept(i);
            }
        }

        Collections.sort(observedCCPerDegree, new Comparator<Pair<Long,Double>>(){

            @Override
            public int compare(Pair<Long, Double> entry1, Pair<Long, Double> entry2) {
                if(entry1.getKey() < entry2.getKey()) return -1;
                if(entry1.getKey() == entry2.getKey()) return 0;
                return 1;
            }
        } );
        double [] ccPerDegree = Algorithms.GenerateCCperDegree(observedCCPerDegree,maxDegree);
        initialize(numNodes, degrees,ccPerDegree);
    }

    public void initialize(long numNodes, HashMap<Integer,Long> degrees, double [] ccPerDegree) {

        if(maxDegree == Integer.MIN_VALUE) {
            maxDegree = Integer.MIN_VALUE;
            for (Integer key : degrees.keySet()) {
                maxDegree = key > maxDegree ? key : maxDegree;
            }
        }

        i_g = new long[maxDegree+1];
        b_g = new long[maxDegree+1];
        n_g = new long[maxDegree+1];
        w_g = new double[maxDegree+1];
        nfill_d = new long[maxDegree+1];
        nbulk_d = new long[maxDegree+1];
        wfill_d = new double[maxDegree+1];
        wbulk_d = new double[maxDegree+1];
        w_d = new double[maxDegree+1];
        r_d = new double[maxDegree+1];
        n_d = new long[maxDegree+1];
        i_d = new long[maxDegree+1];
        for(int i = 0; i < maxDegree+1; ++i) {
            i_g[i] = 0;
            b_g[i] = 0;
            n_g[i] = i+1;
            w_g[i] = 0;

            nfill_d[i] = 0;
            nbulk_d[i] = 0;
            wfill_d[i] = 0;
            wbulk_d[i] = 0;

            w_d[i] = 0;
            r_d[i] = 0;
            i_d[i] = 0;
            n_d[i] = 0;
        }

        // initializing n_d
        for(Map.Entry<Integer,Long> pair : degrees.entrySet()) {
            n_d[pair.getKey()] = (long)pair.getValue();
        }


        // initializing i_d
        i_d[2] = 0;
        for(int i = 3; i <= maxDegree; ++i) {
            i_d[i] = i_d[i-1] + n_d[i-1];
        }
        i_d[1] = i_d[maxDegree] + n_d[maxDegree];
        i_g[1] = i_d[maxDegree] + n_d[maxDegree];

        long [] n_dPrima = new long[maxDegree+1];
        int accum = 0;
        for(int i = 0; i <= maxDegree; ++i) {
            accum+=n_d[i];
            n_dPrima[i]=numNodes-accum;
        }

        //handling degree 1 nodes
        nfill_d[1] = 10*n_d[1];
        wfill_d[1] = 0.5*n_d[1];
        wbulk_d[1] = 0;
        w_d[1] = wbulk_d[1] + wfill_d[1];
        r_d[1] = 1;

        // Initializing group arrays.
        long nFillPrevious = 0;
        long g = -1;
        double dInternalPrevious = 0;
        for(int i = 2; i <= maxDegree; ++i) {
            if(nFillPrevious > 0) {
                nfill_d[i] = Math.min(nFillPrevious, n_d[i]);
                nFillPrevious-=nfill_d[i];
                wfill_d[i] = 0.5*nfill_d[i]*(i-dInternalPrevious);
            } else {
                nfill_d[i] = 0;
                wfill_d[i] = 0;
            }
            nbulk_d[i] = n_d[i] - nfill_d[i];
            if(nbulk_d[i] > 0) {
                g+=1;
                i_g[(int)g] = i_d[i] + nfill_d[i];
                b_g[(int)g] = (int)Math.ceil(nbulk_d[i]/(double)(i+1));
                n_g[(int)g] = i+1;
                if(b_g[(int)g]*(i+1) > (n_dPrima[i]+nbulk_d[i])) {
                    n_g[(int)g] = (n_dPrima[i]+nbulk_d[i]);
                }
                double p = Math.pow(ccPerDegree[i],1/3.0);
                dInternalPrevious = (n_g[(int)g]-1)*p;
                wbulk_d[i] = 0.5*nbulk_d[i]*(i-dInternalPrevious);
                w_g[(int)g] = b_g[(int)g]*0.5*n_g[(int)g]*(n_g[(int)g]-1)*Math.log(1/(1.0-p));
                nFillPrevious = (b_g[(int)g]*n_g[(int)g]) - nbulk_d[i];
            } else {
                wbulk_d[i] = 0;
            }
            w_d[i] = wfill_d[i] + wbulk_d[i];
            r_d[i] = wfill_d[i] / w_d[i];
        }

        long [] newi_g = new long[(int)g+1];
        long [] newn_g = new long[(int)g+1]; //number of buckets in group
        long [] newb_g = new long[(int)g+1]; //size of bucket in group
        double [] neww_g = new double[(int)g+1]; //weight of buckets in group
        for(int i = 0; i < g+1; ++i) {
            newi_g[i] = i_g[i];
            newn_g[i] = n_g[i];
            newb_g[i] = b_g[i];
            neww_g[i] = w_g[i];
        }

        i_g = newi_g;
        n_g = newn_g;
        b_g = newb_g;
        w_g = neww_g;

        weightPhase1 = 0L;
        weightPhase2 = 0L;
        for(int i = 0; i < getNumGroups(); ++i) {
            weightPhase1+= getGroupWeight(i);
        }

        for(int i = 0; i < (getMaxDegree()+1); ++i) {
            weightPhase2+=getDegreeWeight(i);
        }

        cumulativeGroups = new double[getNumGroups()];
        cumulativeGroups[0] = getGroupWeight(0) / (double)weightPhase1;
        for(int i = 1; i < getNumGroups(); ++i) {
            cumulativeGroups[i] = Math.min(cumulativeGroups[i-1] + getGroupWeight(i) / (double)weightPhase1, 1.0);
        }

        cumulativeDegrees = new double[getMaxDegree()+1];
        cumulativeDegrees[0] = getDegreeWeight(0) / (double)weightPhase2;
        for(int i = 1; i < (getMaxDegree()+1); ++i) {
            cumulativeDegrees[i] = Math.min(cumulativeDegrees[i-1] + getDegreeWeight(i) / (double)weightPhase2,1.0);
        }
    }

    public long getGroupIndex(int group) {return i_g[group];}

    public long getGroupBucketSize(int group) {return n_g[group];}

    public long getGroupNumBuckets(int group) {return b_g[group];}

    public double getGroupWeight(int group) {return w_g[group];}

    public double [] getGroupWeights() { return w_g;}

    public long getDegreeIndex(int degree) { return i_d[degree];}

    public long getDegreeNFill(int degree) { return nfill_d[degree];}

    public long getDegreeNBulk(int degree) { return nbulk_d[degree];}

    public double getDegreeWFill(int degree) { return wfill_d[degree];}

    public double getDegreeWBulk(int degree) { return wbulk_d[degree];}

    public double getDegreeWeight(int degree) { return w_d[degree];}

    public double [] getDegreeWeights() { return w_d;}

    public double getDegreeWeightRatio( int degree ) { return r_d[degree];}

    public double [] getDegreeWeightRatios() { return r_d;}

    public double getDegreeN(int degree) { return n_d[degree];}

    public int getNumGroups() {
        return n_g.length;
    }

    public int getMaxDegree() { return maxDegree;}

    public double[] getCumulativeGroups() {
        return cumulativeGroups;
    }

    public double[] getCumulativeDegrees() {
        return cumulativeDegrees;
    }

    public long getWeightPhase1() {
        return weightPhase1;
    }

    public long getWeightPhase2() {
        return weightPhase2;
    }

}
