package ldbc.snb.bteronhplus.algorithms;

import ldbc.snb.bteronhplus.structures.BlockModel;
import ldbc.snb.bteronhplus.structures.SuperNode;
import ldbc.snb.bteronhplus.structures.SuperNodeCluster;
import ldbc.snb.bteronhplus.structures.SuperNodeStreamer;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;

import java.util.*;

public class Partitioning {


    private static double score(BlockModel blockModel,
                                double currentBlockSize[],
                                double currentBlockDegree[] ) {

        int numBlocks = blockModel.getNumBlocks();
        double sizeScore = 0.0;
        double degreeScore = 0.0;
        for(int i = 0; i < numBlocks; ++i) {
            BlockModel.ModelEntry entry = blockModel.getEntries().get(i);
            double currentSize = currentBlockSize[i];
            double currentDegree = currentBlockDegree[i];
            sizeScore += Math.pow(entry.size-currentSize,2);
            degreeScore += Math.pow(entry.totalDegree-currentDegree,2);
        }
        return sizeScore+degreeScore;
    }

    private static double score(BlockModel blockModel,
                                double currentBlockSize[],
                                double currentBlockDegree[],
                                double totalObservedNodes,
                                double totalObservedDegree,
                                long size,
                                long degree,
                                int block) {

        int numBlocks = blockModel.getNumBlocks();
        double sizeScore = 0.0;
        double degreeScore = 0.0;
        for(int i = 0; i < numBlocks; ++i) {
            BlockModel.ModelEntry entry = blockModel.getEntries().get((long)i);
            double currentSize = currentBlockSize[i]*totalObservedNodes;
            double currentDegree = currentBlockDegree[i]*totalObservedDegree;
            if(block == i) {
                currentSize+=size;
                currentDegree+=degree;
            }
            double normalizedCurrentSize = currentSize / (totalObservedNodes + size);
            double normalizedCurrentDegree = currentDegree / (totalObservedDegree + degree);
            sizeScore += Math.pow(entry.size-normalizedCurrentSize,2);
            degreeScore += Math.pow(entry.totalDegree-normalizedCurrentDegree,2);
        }
        return sizeScore+degreeScore;
    }

    public static Map<Long,SuperNodeCluster> partition(BlockModel blockModel,
                                                   SuperNodeStreamer streamer,
                                                   long numNodes) {

        List<List<SuperNode>> superNodesPerBlock = new ArrayList<List<SuperNode>>();
        int numBlocks = blockModel.getNumBlocks();
        for(int i = 0; i < numBlocks; ++i) {
            superNodesPerBlock.add(new ArrayList<SuperNode>());
        }

        double currentBlockSize[] = new double[numBlocks];
        Arrays.fill(currentBlockSize, 0.0);
        double currentBlockDegree[] = new double[numBlocks];
        Arrays.fill(currentBlockDegree, 0.0);


        //double currentScore = score(blockModel, currentBlockSize, currentBlockDegree);
        long totalObservedNodes = 0L;
        long totalObservedDegree = 0L;
        int count = 1;
        while( totalObservedNodes < numNodes) {
            SuperNode nextSuperNode = streamer.next();
            int bestBlock = 0;
            double bestScore = Double.MAX_VALUE;
            for(int j = 0; j < numBlocks; ++j) {
                double nextScore = score(blockModel,
                                         currentBlockSize,
                                         currentBlockDegree,
                                         totalObservedNodes,
                                         totalObservedDegree,
                                         nextSuperNode.getSize(),
                                         nextSuperNode.getInternalDegree() + nextSuperNode.getExternalDegree(),
                                         j);

                if(nextScore < bestScore) {
                    bestScore = nextScore;
                    bestBlock = j;
                }
            }

            for(int j = 0; j < numBlocks; ++j) {
                currentBlockSize[j] = currentBlockSize[j]*totalObservedNodes;
                currentBlockDegree[j] = currentBlockDegree[j]*totalObservedDegree;
            }

            currentBlockSize[bestBlock]+=nextSuperNode.getSize();
            currentBlockDegree[bestBlock]+=nextSuperNode.getInternalDegree()+nextSuperNode.getExternalDegree();
            superNodesPerBlock.get(bestBlock).add(nextSuperNode);
            totalObservedDegree+=nextSuperNode.getInternalDegree()+nextSuperNode.getExternalDegree();
            totalObservedNodes+=nextSuperNode.getSize();
            for(int j = 0; j < numBlocks; ++j) {
                currentBlockSize[j] = currentBlockSize[j]/totalObservedNodes;
                currentBlockDegree[j] = currentBlockDegree[j]/totalObservedDegree;
            }
            //currentScore = score(blockModel, currentBlockSize, currentBlockDegree);

            if( count % 1000 == 0) {
                System.out.println("Distributed "+count+" superNodes amounting "+totalObservedNodes+" out of " +
                        numNodes);
            }
            count++;
        }

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

    public static List<HashMap<Long,Long>> getDegreesPerBlock(List<List<SuperNode>> superNodes) {
        List<HashMap<Long,Long>> degreesPerBlock = new ArrayList<HashMap<Long,Long>>();
        for(int i = 0; i < superNodes.size(); ++i) {
            degreesPerBlock.add(new HashMap<>());
        }

        for(int i = 0; i < superNodes.size(); ++i) {
            for(SuperNode node : superNodes.get(i)) {
                degreesPerBlock.get(i).merge(node.getExternalDegree()+node.getInternalDegree(), 1L, Long::sum);
            }
        }
        return degreesPerBlock;
    }
}
