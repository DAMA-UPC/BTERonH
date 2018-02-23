package ldbc.snb.bteronhplus.structures;

import ldbc.snb.bteronhplus.algorithms.BTER;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class BTERPlusStats implements Serializable {

    int maxDegree = Integer.MIN_VALUE;

    public long externalWeightPerDegree[] = null;           // The degree*node_d
    public long numNodesPerDegree[] = null;                 // The amount of node of a given degree
    public long cumNumNodesPerDegree[] = null;              // The amount of nodes with degree less than degree
    public double externalWeightPerDegreeCumProb[] = null;
    public long totalExternalWeight = 0;

    public double externalWeightPerBlockCumProb[] = null;
    public int blockIdMap[] = null;


    public void initialize(HashMap<Integer,Long> degrees, double [] degreeBudget, int blockIndex, BlockModel
            blockModel ) {

        long numNodes = 0L;
        for(Map.Entry<Integer,Long> entry : degrees.entrySet()) {
            int degree = entry.getKey();
            long count = entry.getValue();
            maxDegree = Math.max( degree, maxDegree );
            numNodes += count;
            totalExternalWeight += degreeBudget[entry.getKey()];
        }

        externalWeightPerDegree = new long[maxDegree+1];
        Arrays.fill(externalWeightPerDegree, 0L);
        numNodesPerDegree = new long[maxDegree+1];
        Arrays.fill(numNodesPerDegree, 0L);
        cumNumNodesPerDegree = new long[maxDegree+1];
        Arrays.fill(cumNumNodesPerDegree, 0L);
        externalWeightPerDegreeCumProb = new double[maxDegree+1];
        Arrays.fill(externalWeightPerDegreeCumProb, 0.0);

        for(Map.Entry<Integer,Long> entry : degrees.entrySet()) {
            int degree = entry.getKey();
            long count = entry.getValue();
            externalWeightPerDegree[degree] = (long)degreeBudget[degree];
            numNodesPerDegree[degree] = count;
        }

        for(int i = 1; i < maxDegree+1;++i) {
            cumNumNodesPerDegree[i] = cumNumNodesPerDegree[i-1] + numNodesPerDegree[i];
        }

        if(totalExternalWeight > 0) {
            for (int i = 1; i < maxDegree + 1; ++i) {
                externalWeightPerDegreeCumProb[i] = Math.min(externalWeightPerDegreeCumProb[i - 1] + externalWeightPerDegree[i] / (double) totalExternalWeight, 1.0);
            }
        }

        if(blockModel != null) {

            int numBlocksWithEdges = 0;
            double totalExternalWeightRatio = 0.0;

            for (int i = 0; i < blockModel.blockSizes.length; ++i) {
                if (i != blockIndex && blockModel.degree[blockIndex][i] != 0.0) {
                    numBlocksWithEdges++;
                    totalExternalWeightRatio += blockModel.degree[blockIndex][i];
                }
            }

            externalWeightPerBlockCumProb = new double[numBlocksWithEdges];
            Arrays.fill(externalWeightPerBlockCumProb, 0.0);
            blockIdMap = new int[numBlocksWithEdges];
            Arrays.fill(blockIdMap, 0);

            int nextBlock = 0;
            for (int i = 0; i < blockModel.blockSizes.length; i++) {
                if (i != blockIndex && blockModel.degree[blockIndex][i] != 0.0) {
                    blockIdMap[nextBlock] = i;
                    externalWeightPerBlockCumProb[nextBlock] = blockModel.degree[blockIndex][i] /
                            totalExternalWeightRatio;
                    nextBlock++;
                }
            }

            for (int i = 1; i < blockIdMap.length; ++i) {
                externalWeightPerBlockCumProb[i] += externalWeightPerBlockCumProb[i - 1];
            }

        }

    }
}
