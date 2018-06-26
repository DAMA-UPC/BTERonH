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
    
    private static double computeRatio(double totalDegree, double internalDegree, double expectedRatio) {
    
        return Math.min((totalDegree - internalDegree)/ (double)
            (totalDegree), expectedRatio);
        
    }
    

    private static double score(BlockModel blockModel,
                                long totalSize,
                                long currentBlockSize[],
                                long currentBlockInternalDegree[],
                                long currentBlockExternalDegree[],
                                long currentBlockInterBudgetDegree[],
                                double currentBlockRatio[],
                                long totalObservedNodes,
                                long totalObservedDegree,
                                long size,
                                long internalDegree,
                                long externalDegree,
                                int block) {

        long nextSize = totalObservedNodes + size;
        long nextDegree = totalObservedDegree + internalDegree + externalDegree;
        int numBlocks = blockModel.getNumBlocks();
        double score = 0.0;
        for(int i = 0; i < numBlocks; ++i) {
            BlockModel.ModelEntry entry = blockModel.getEntries().get((long)i);
            double blockSize = currentBlockSize[i];
            double blockTotalDegree = currentBlockInternalDegree[i] + currentBlockExternalDegree[i];
            double blockRatio = currentBlockRatio[i];
            double blockInternalDegree = (blockTotalDegree*(1.0-blockRatio));
            double blockExternalDegree = (blockTotalDegree*blockRatio);
            double expectedRatio = entry.externalDegree / entry.totalDegree;
            double blockInterBudgetDegree = currentBlockInterBudgetDegree[i];
            if(block == i) {
                blockSize+=size;
                blockTotalDegree += internalDegree;
                blockTotalDegree += externalDegree;
                blockRatio = computeRatio(blockTotalDegree, blockInternalDegree, expectedRatio);
                
                blockInternalDegree = (blockTotalDegree*(1.0-blockRatio));
                blockExternalDegree = (blockTotalDegree*(blockRatio));
                blockInterBudgetDegree = Math.abs(currentBlockInterBudgetDegree[i] - externalDegree);
            }
            
            double expectedSize = (entry.size*nextSize);
            double expectedDegree = (entry.totalDegree*nextDegree);
            double sizeScore = Math.pow((expectedSize-blockSize)/expectedSize,2);
            double expectedInternalDegree = (entry.totalDegree - entry.externalDegree)*nextDegree;
            double expectedExternalDegree = (entry.externalDegree)*nextDegree;
    
            
            double internalDegreeScore = Math.pow((expectedInternalDegree-blockInternalDegree) / expectedDegree, 2);
            double externalDegreeScore = Math.pow((expectedExternalDegree-blockExternalDegree)/ expectedDegree,2);
            double ratioScore = Math.pow((expectedRatio-blockRatio),2);
            double interBudgetScore = blockInterBudgetDegree == 0 ? 0.0 : Math.pow(1.0 - 1.0 / blockInterBudgetDegree,
                                                                                2.0);
            
            score += (sizeScore + internalDegreeScore + externalDegreeScore + interBudgetScore + ratioScore );
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
        long currentBlockInterBudgetDegree[] = new long[numBlocks];
        Arrays.fill(currentBlockInterBudgetDegree, 0L);


        Random random = new Random();
        long totalObservedNodes = 0L;
        long totalObservedDegree = 0L;
        int count = 1;
        while( totalObservedNodes < numNodes) {
            SuperNode nextSuperNode = streamer.next();
            int bestBlock = random.nextInt(blockModel.getNumBlocks());
            double bestScore = score(blockModel,
                                     numNodes,
                                     currentBlockSize,
                                     currentBlockInternalDegree,
                                     currentBlockExternalDegree,
                                     currentBlockInterBudgetDegree,
                                     currentBlockRatio,
                                     totalObservedNodes,
                                     totalObservedDegree,
                                     nextSuperNode.getSize(),
                                     nextSuperNode.getInternalDegree(),
                                     nextSuperNode.getExternalDegree(),
                                     bestBlock);
            for(int j = 0; j < numBlocks; ++j) {
    
                double nextScore = score(blockModel,
                                         numNodes,
                                         currentBlockSize,
                                         currentBlockInternalDegree,
                                         currentBlockExternalDegree,
                                         currentBlockInterBudgetDegree,
                                         currentBlockRatio,
                                         totalObservedNodes,
                                         totalObservedDegree,
                                         nextSuperNode.getSize(),
                                         nextSuperNode.getInternalDegree(),
                                         nextSuperNode.getExternalDegree(),
                                         j);
                
                
                if (nextScore < bestScore) {
                    bestBlock = j;
                    bestScore = nextScore;
                }
            }
    
            currentBlockSize[bestBlock]+=nextSuperNode.getSize();
            currentBlockInternalDegree[bestBlock]+=nextSuperNode.getInternalDegree();
            currentBlockExternalDegree[bestBlock]+=nextSuperNode.getExternalDegree();
            long totalDegree = currentBlockInternalDegree[bestBlock] + currentBlockExternalDegree[bestBlock];
            BlockModel.ModelEntry entry = blockModel.getEntries().get((long)bestBlock);
            double expectedRatio = entry.externalDegree / entry.totalDegree;
    
            currentBlockRatio[bestBlock] = computeRatio((long)totalDegree, currentBlockInternalDegree[bestBlock],
                                                        expectedRatio);
    
            currentBlockInterBudgetDegree[bestBlock] = Math.abs(currentBlockInterBudgetDegree[bestBlock] -
                                                                    nextSuperNode.getExternalDegree());
            
    
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
                sizeScore += Math.pow((expectedSize-observedSize), 2 );
            }
            partitioningSizes.close();
            
            FileWriter partitioningInternalDegree = new FileWriter(new File("partitioning.internalDegree"));
            for(BlockModel.ModelEntry entry : blockModel.getEntries().values()) {
                long expectedInternalDegree = (long)((entry.totalDegree - entry.externalDegree)*totalDegree);
                totalExpectedInternalDegree += expectedInternalDegree;
                long observedInternalDegree = (long)((currentBlockInternalDegree[(int)entry.id] +
                    currentBlockExternalDegree[(int)(entry.id)]) * (1-currentBlockRatio[(int)entry.id]));
                totalObservedInternalDegree += observedInternalDegree;
                
                internalDegreeScore += Math.pow((expectedInternalDegree - observedInternalDegree) ,2);
                
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
                
                externalDegreeScore += Math.pow((expectedExternalDegree - observedExternalDegree),2);
                
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

}
