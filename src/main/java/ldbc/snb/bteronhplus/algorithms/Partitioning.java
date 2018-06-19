package ldbc.snb.bteronhplus.algorithms;

import ldbc.snb.bteronhplus.structures.BlockModel;
import ldbc.snb.bteronhplus.structures.SuperNode;
import ldbc.snb.bteronhplus.structures.SuperNodeCluster;
import ldbc.snb.bteronhplus.structures.SuperNodeStreamer;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;

import java.util.*;

public class Partitioning {


    private static double score(BlockModel blockModel,
                                long totalSize,
                                long currentBlockSize[],
                                long currentBlockInternalDegree[],
                                long currentBlockExternalDegree[],
                                double currentBlockRatio[],
                                long totalObservedNodes,
                                long totalObservedDegree,
                                long size,
                                long internalDegree,
                                long externalDegree,
                                int block) {

        int numBlocks = blockModel.getNumBlocks();
        double score = 0.0;
        for(int i = 0; i < numBlocks; ++i) {
            BlockModel.ModelEntry entry = blockModel.getEntries().get((long)i);
            double currentSize = currentBlockSize[i];
            double currentTotalDegree = currentBlockInternalDegree[i] + currentBlockExternalDegree[i];
            double currentRatio = currentBlockRatio[i];
            double expectedRatio = entry.externalDegree / entry.totalDegree;
            if(block == i) {
                currentSize+=size;
                currentTotalDegree += internalDegree;
                currentTotalDegree += externalDegree;
                long currentInternalDegree = currentBlockInternalDegree[i] + internalDegree;
                
                currentRatio = Math.min((currentTotalDegree - currentInternalDegree)/ (double)
                                            currentTotalDegree, expectedRatio);
            }
            double normalizedCurrentSize = currentSize / (double)(totalObservedNodes + size);
            double normalizedCurrentDegree = currentTotalDegree / (double)(totalObservedDegree + internalDegree +
                externalDegree);
            //sizeScore += Math.pow(entry.size-normalizedCurrentSize,2);
    
            double degreeScore = Math.pow(entry.totalDegree-normalizedCurrentDegree,2);
            double ratioScore = Math.pow(expectedRatio-currentRatio,2);
            //score += (degreeScore+ratioScore) / 2.0;
            score += degreeScore*ratioScore;
        }
        return score;
    }

    public static Map<Long,SuperNodeCluster> partition(BlockModel blockModel,
                                                   SuperNodeStreamer streamer,
                                                   long numNodes) {

        List<List<SuperNode>> superNodesPerBlock = new ArrayList<List<SuperNode>>();
        int numBlocks = blockModel.getNumBlocks();
        for(int i = 0; i < numBlocks; ++i) {
            superNodesPerBlock.add(new ArrayList<SuperNode>());
        }

        long currentBlockSize[] = new long[numBlocks];
        Arrays.fill(currentBlockSize, 0L);
        long currentBlockInternalDegree[] = new long[numBlocks];
        Arrays.fill(currentBlockInternalDegree, 0L);
        long currentBlockExternalDegree[] = new long[numBlocks];
        Arrays.fill(currentBlockExternalDegree, 0L);
        double currentBlockRatio[] = new double[numBlocks];
        Arrays.fill(currentBlockRatio, 0.0);
        for(BlockModel.ModelEntry entry : blockModel.getEntries().values()) {
            currentBlockRatio[(int)entry.id] = entry.externalDegree / entry.totalDegree;
        }


        long totalObservedNodes = 0L;
        long totalObservedDegree = 0L;
        int count = 1;
        double currentScore = 10000000;
        while( totalObservedNodes < numNodes) {
            SuperNode nextSuperNode = streamer.next();
            int bestBlock = 0;
            double bestScore = Double.MAX_VALUE;
            double bestImprovement = 0;
            for(int j = 0; j < numBlocks; ++j) {
    
                BlockModel.ModelEntry entry = blockModel.getEntries().get((long)j);
                double nextScore = score(blockModel,
                                         numNodes,
                                         currentBlockSize,
                                         currentBlockInternalDegree,
                                         currentBlockExternalDegree,
                                         currentBlockRatio,
                                         totalObservedNodes,
                                         totalObservedDegree,
                                         nextSuperNode.getSize(),
                                         nextSuperNode.getInternalDegree(),
                                         nextSuperNode.getExternalDegree(),
                                         j);
                
                
                double improvement = nextScore - currentScore;
                double factor = 1.0 - currentBlockSize[j] / (double)(entry.size*numNodes);
                improvement = factor*improvement;

                if(improvement > bestImprovement) {
                    bestScore = nextScore;
                    bestBlock = j;
                    bestImprovement = improvement;
                }
            }
            currentScore = bestScore;
    
            currentBlockSize[bestBlock]+=nextSuperNode.getSize();
            currentBlockInternalDegree[bestBlock]+=currentBlockInternalDegree[bestBlock] + nextSuperNode.getInternalDegree();
            currentBlockExternalDegree[bestBlock]+=currentBlockExternalDegree[bestBlock] + nextSuperNode.getExternalDegree();
            long totalDegree = currentBlockInternalDegree[bestBlock] + currentBlockExternalDegree[bestBlock];
            BlockModel.ModelEntry entry = blockModel.getEntries().get((long)bestBlock);
            double expectedRatio = entry.externalDegree / entry.totalDegree;
            currentBlockRatio[bestBlock] = Math.min((totalDegree - currentBlockInternalDegree[bestBlock])/ (double)
                totalDegree, expectedRatio);
    
            superNodesPerBlock.get(bestBlock).add(nextSuperNode);
    
            totalObservedDegree+=nextSuperNode.getInternalDegree()+nextSuperNode.getExternalDegree();
            totalObservedNodes+=nextSuperNode.getSize();
            

            if( count % 1000 == 0) {
                System.out.println("Distributed "+count+" superNodes amounting "+totalObservedNodes+" out of " +
                        numNodes);
            }
            count++;
        }

        System.out.println("Number of nodes observed "+totalObservedNodes);
        System.out.println("Number of degree observed "+totalObservedDegree);
        System.out.println("Number of super nodes consumed "+count);
        long id = 0;
        Map<Long,SuperNodeCluster> clusters = new HashMap<Long,SuperNodeCluster>();
        for(List<SuperNode> children : superNodesPerBlock) {
            if(children.size() > 0) {
                BlockModel.ModelEntry entry = blockModel.getEntries().get(id);
                double ratio = entry.externalDegree / entry.totalDegree;
                clusters.put(id, new SuperNodeCluster(id, children, ratio));
            }
            id++;
        }
        return clusters;
    }

}
