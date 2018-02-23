package ldbc.snb.bteronhplus.algorithms;

import ldbc.snb.bteronhplus.structures.BlockModel;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;

import java.util.*;

public class Partitioning {


    private static double score(BlockModel blockModel,
                                double currentBlockSize[],
                                double currentBlockDegree[] ) {

        double sizeScore = 0.0;
        double degreeScore = 0.0;
        for(int i = 0; i < blockModel.blockSizes.length; ++i) {
            double currentSize = currentBlockSize[i];
            double currentDegree = currentBlockDegree[i];
            sizeScore += Math.pow(blockModel.blockSizes[i]-currentSize,2);
            degreeScore += Math.pow(blockModel.totalBlockDegree[i]-currentDegree,2);
        }
        return sizeScore+degreeScore;
    }

    private static double score(BlockModel blockModel,
                                double currentBlockSize[],
                                double currentBlockDegree[],
                                double totalObservedNodes,
                                double totalObservedDegree,
                                int degree,
                                int block) {

        double sizeScore = 0.0;
        double degreeScore = 0.0;
        for(int i = 0; i < blockModel.blockSizes.length; ++i) {
            double currentSize = currentBlockSize[i]*totalObservedNodes;
            double currentDegree = currentBlockDegree[i]*totalObservedDegree;
            if(block == i) {
                currentSize+=1;
                currentDegree+=degree;
            }
            double normalizedCurrentSize = currentSize / (totalObservedNodes + 1);
            double normalizedCurrentDegree = currentDegree / (totalObservedDegree + degree);
            sizeScore += Math.pow(blockModel.blockSizes[i]-normalizedCurrentSize,2);
            degreeScore += Math.pow(blockModel.totalBlockDegree[i]-normalizedCurrentDegree,2);
        }
        return sizeScore+degreeScore;
    }

    public static List<HashMap<Integer,Long>> partition(BlockModel blockModel, RandomVariateGen sampler, long
            numNodes) {

        ArrayList<HashMap<Integer,Long>> degreesPerBlock = new ArrayList<HashMap<Integer,Long>>();
        for(int i = 0; i < blockModel.blockSizes.length; ++i) {
            degreesPerBlock.add(new HashMap<Integer,Long>());
        }

        double currentBlockSize[] = new double[blockModel.blockSizes.length];
        Arrays.fill(currentBlockSize, 0.0);
        double currentBlockDegree[] = new double[blockModel.blockSizes.length];
        Arrays.fill(currentBlockDegree, 0.0);


        double currentScore = score(blockModel, currentBlockSize, currentBlockDegree);
        long totalObservedNodes = 0L;
        long totalObservedDegree = 0L;
        for(long i = 0; i < numNodes; ++i) {
            int nextDegree = (int)sampler.nextDouble();
            int bestBlock = 0;
            double bestScore = Double.MAX_VALUE;
            for(int j = 0; j < blockModel.blockSizes.length; ++j) {
                double nextScore = score(blockModel,
                                         currentBlockSize,
                                         currentBlockDegree,
                                         totalObservedNodes,
                                         totalObservedDegree,
                                         nextDegree,
                                         j);

                if(nextScore < bestScore) {
                    bestScore = nextScore;
                    bestBlock = j;
                }
            }

            for(int j = 0; j < blockModel.blockSizes.length; ++j) {
                currentBlockSize[j] = currentBlockSize[j]*totalObservedNodes;
                currentBlockDegree[j] = currentBlockDegree[j]*totalObservedDegree;
            }
            currentBlockSize[bestBlock]++;
            currentBlockDegree[bestBlock]+=nextDegree;
            degreesPerBlock.get(bestBlock).merge(nextDegree,1L,Long::sum);
            totalObservedDegree+=nextDegree;
            totalObservedNodes+=1L;
            for(int j = 0; j < blockModel.blockSizes.length; ++j) {
                currentBlockSize[j] = currentBlockSize[j]/totalObservedNodes;
                currentBlockDegree[j] = currentBlockDegree[j]/totalObservedDegree;
            }
            currentScore = score(blockModel, currentBlockSize, currentBlockDegree);

            if( i % 10000 == 0) {
                System.out.println("Distributed "+i+" nodes out of "+numNodes);
            }
        }

        return degreesPerBlock;
    }

    public static void partitionToBlockSizes(List<HashMap<Integer,Long>> partition, double [] sizes, double []
            degrees) {
        assert sizes.length == degrees.length && partition.size() == sizes.length;

        long numNodes = 0;
        long numDegree = 0;
        for(HashMap<Integer,Long> block : partition ) {
            for ( Map.Entry<Integer,Long> entry : block.entrySet()) {
                numDegree += entry.getKey()*entry.getValue();
                numNodes += entry.getValue();
            }
        }

        Arrays.fill(sizes,0.0);
        Arrays.fill(degrees, 0.0);

        int numBlocks = partition.size();
        for (int i = 0; i < numBlocks; ++i) {
            long blockNodes = 0;
            long blockDegree = 0;
            HashMap<Integer,Long> counts = partition.get(i);
            for ( Map.Entry<Integer, Long> entry : counts.entrySet()) {
                blockNodes += entry.getValue();
                blockDegree += entry.getValue()*entry.getKey();
            }
            sizes[i] = blockNodes / (double) numNodes;
            degrees[i] = blockDegree / (double) numDegree;
        }

    }

}
