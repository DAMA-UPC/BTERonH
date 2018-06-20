package ldbc.snb.bteronhplus.algorithms;

import ldbc.snb.bteronhplus.structures.BlockModel;
import ldbc.snb.bteronhplus.structures.SuperNode;
import ldbc.snb.bteronhplus.structures.SuperNodeCluster;
import ldbc.snb.bteronhplus.structures.SuperNodeStreamer;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
    
            double sizeScore = Math.pow((entry.size*totalObservedNodes-currentSize)
                                              /totalObservedNodes,2);
            double degreeScore = Math.pow((entry.totalDegree*totalObservedDegree-currentTotalDegree)
                                              /totalObservedDegree,2);
            double ratioScore = Math.pow((expectedRatio-currentRatio),2);
            score += (sizeScore + degreeScore + ratioScore) / 3.0 ;
        }
        return score;
    }
    
    private static void printStats(BlockModel blockModel,
                                   long currentBlockSize[],
                                   long currentBlockInternalDegree[],
                                   long currentBlockExternalDegree[],
                                   double currentBlockRatio[]
                                   ) {
        
        long totalDegree = 0;
        for(int i = 0; i < currentBlockInternalDegree.length; ++i) {
            totalDegree+=currentBlockInternalDegree[i];
            totalDegree+=currentBlockExternalDegree[i];
        }
        
        long totalSize = 0;
        for(int i = 0; i < currentBlockSize.length; ++i) {
            totalSize += currentBlockSize[i];
        }
        
        long totalExpectedInternalDegree = 0;
        long totalExpectedExternalDegree = 0;
    
        long totalObservedInternalDegree = 0;
        long totalObservedExternalDegree = 0;
        
        double sizeScore = 0.0;
        double internalDegreeScore = 0.0;
        double externalDegreeScore = 0.0;
    
        try {
            FileWriter partitioningSizes = new FileWriter(new File("partitioning.sizes"));
            for(BlockModel.ModelEntry entry : blockModel.getEntries().values()) {
                long expectedSize = (long)(entry.size*totalSize);
                long observedSize = (long)(currentBlockSize[(int)entry.id]);
                partitioningSizes.write(entry.id+" "+expectedSize+" "+observedSize+"\n");
                sizeScore += Math.pow(expectedSize-observedSize, 2 );
            }
            partitioningSizes.close();
    
            FileWriter partitioningInternalDegree = new FileWriter(new File("partitioning.internalDegree"));
            for(BlockModel.ModelEntry entry : blockModel.getEntries().values()) {
                long expectedInternalDegree = (long)((entry.totalDegree - entry.externalDegree)*totalDegree);
                totalExpectedInternalDegree += expectedInternalDegree;
                long observedInternalDegree = (long)((currentBlockInternalDegree[(int)entry.id] +
                    currentBlockExternalDegree[(int)(entry.id)]) * (1-currentBlockRatio[(int)entry.id]));
                totalObservedInternalDegree += observedInternalDegree;
                
                internalDegreeScore += Math.pow(expectedInternalDegree - observedInternalDegree,2);
                
                partitioningInternalDegree.write(entry.id+" "+expectedInternalDegree+" " +
                    ""+observedInternalDegree+"\n");
            }
            partitioningInternalDegree.close();
    
            FileWriter partitioningExternalDegree = new FileWriter(new File("partitioning.externalDegree"));
            for(BlockModel.ModelEntry entry : blockModel.getEntries().values()) {
                long expectedExternalDegree = (long)(entry.externalDegree*totalDegree);
                totalExpectedExternalDegree += expectedExternalDegree;
                long observedExternalDegree = (long)((currentBlockInternalDegree[(int)entry.id] +
                    currentBlockExternalDegree[(int)(entry.id)]) * (currentBlockRatio[(int)entry.id]));
    
                totalObservedExternalDegree += observedExternalDegree;
    
                externalDegreeScore += Math.pow(expectedExternalDegree - observedExternalDegree,2);
                
                partitioningExternalDegree.write(entry.id+" "+expectedExternalDegree+" " +
                                            observedExternalDegree+"\n");
            }
            partitioningExternalDegree.close();
        } catch( IOException e) {
            e.printStackTrace();
        }
    
        System.out.println("Expected vs Observed internal degree: "+totalExpectedInternalDegree+" "+totalObservedInternalDegree);
        System.out.println("Expected vs Observed external degree: "+totalExpectedExternalDegree+" " +
                               ""+totalObservedExternalDegree);
        System.out.println("Size Score: "+(long)sizeScore);
        System.out.println("Internal Degree Score: "+(long)internalDegreeScore);
        System.out.println("External Degree Score: "+(long)externalDegreeScore);
        
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
        while( totalObservedNodes < numNodes) {
            SuperNode nextSuperNode = streamer.next();
            int bestBlock = 0;
            double bestImprovement = 0;
            double baseline = score(blockModel,
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
                                     0);
            for(int j = 1; j < numBlocks; ++j) {
    
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
                
                
                    double improvement = baseline - nextScore;
                    double size_factor = 1.0 - (currentBlockSize[j]) / (double) (entry.size *
                    numNodes);
                    //improvement = size_factor * improvement;
    
                    if (improvement > bestImprovement) {
                        bestBlock = j;
                        bestImprovement = improvement;
                    }
            }
    
            currentBlockSize[bestBlock]+=nextSuperNode.getSize();
            currentBlockInternalDegree[bestBlock]+=nextSuperNode.getInternalDegree();
            currentBlockExternalDegree[bestBlock]+=nextSuperNode.getExternalDegree();
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
        
        printStats(blockModel, currentBlockSize, currentBlockInternalDegree, currentBlockExternalDegree, currentBlockRatio);

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
