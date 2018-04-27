package ldbc.snb.bterohplus;

public class PartitioningTest {
/*
    int getBlock(int blockSize, int nodeId) {
        return nodeId / blockSize;
    }

    @Test
    public void testPartitioning (){

        double pIn = 0.5;
        double pOut = 0.1;
        int numBlocks = 4;
        int blockSize = 50;
        int numNodes = numBlocks*blockSize;

        Random random = new Random();
        ArrayList<Pair<Integer,Integer>> edges = new ArrayList<Pair<Integer,Integer>>();
        for( int i = 0; i < numBlocks; ++i) {
            for( int j = 0; j < blockSize; ++j) {
                for( int k = j+1; k < blockSize; ++k) {
                    double prob = random.nextDouble();
                    if (prob < pIn) {
                        edges.add(new Pair(i*blockSize+j, blockSize*i+k));
                    }
                }
            }
        }

        for( int i = 0; i < numBlocks; ++i) {
            for( int t = i+1; t < numBlocks; ++t) {
                for( int j = 0; j < blockSize; ++j) {
                    for( int k = 0; k < blockSize; ++k) {
                        double prob = random.nextDouble();
                        if (prob < pOut) {
                            edges.add(new Pair(i*blockSize+j, blockSize*t+k));
                        }
                    }
                }
            }
        }


        long[] blockSizes = new long[numBlocks];
        Arrays.fill(blockSizes, blockSize);
        long[][] degrees = new long[numBlocks][numBlocks];
        for( long[] array : degrees) {
            Arrays.fill(array,0L);
        }

        long totalDegree = 0L;
        HashMap<Integer,Integer> nodeDegrees = new HashMap<Integer,Integer>();
        for(Pair<Integer,Integer> edge : edges) {
            int first = edge.getFirst();
            int second = edge.getSecond();
            nodeDegrees.merge(first, 1, Integer::sum);
            nodeDegrees.merge(second, 1, Integer::sum);

            int firstBlock = getBlock(blockSize, first);
            int secondBlock = getBlock(blockSize, second);
            degrees[firstBlock][secondBlock]++;
            degrees[secondBlock][firstBlock]++;
            totalDegree+=2L;
        }

        double [] blockSizesRatio = new double[numBlocks];
        for(int i = 0; i < blockSizes.length; ++i) {
            blockSizesRatio[i] = blockSizes[i] / (double)numNodes;
        }

        double[][] degreesRatio = new double[numBlocks][numBlocks];
        for(int i = 0; i < blockSizes.length; ++i) {
            for(int j = 0; j < blockSizes.length; ++j) {
                degreesRatio[i][j] =  degrees[i][j] / (double)totalDegree;
            }
        }


        BlockModel blockModel = new BlockModel(blockSizesRatio, degreesRatio );

        ArrayList<Integer> degreeSequence = new ArrayList<Integer>(nodeDegrees.values());
        List<List<SuperNode>>
                nodesPerBlock = Partitioning.partition(blockModel,
                                                         new RandomVariateStreamer(BTER.getDegreeSequenceSampler(degreeSequence,
                                                                                       numNodes,
                                                                                       123456789)),
                                                         numNodes);


        List<HashMap<Integer,Long>> degreesPerBlock = Partitioning.getDegreesPerBlock(nodesPerBlock);

        for (int i = 0; i < numBlocks; ++i) {
            long blockDegree = 0;
            long blockNodes = 0;
            HashMap<Integer,Long> counts = degreesPerBlock.get(i);
            for ( Map.Entry<Integer, Long> entry : counts.entrySet()) {
                blockDegree += entry.getKey()*entry.getValue();
                blockNodes += entry.getValue();
            }
            double nodesRatio = blockNodes / (double) numNodes;
            double degreeRatio = blockDegree / (double) (2*edges.size());
            System.out.println(degreeRatio+" "+blockModel.totalBlockDegree[i]+" "+nodesRatio+" "+blockModel.blockSizes[i]);
            Assert.assertTrue(nodesRatio - blockModel.blockSizes[i] < 0.01);
            Assert.assertTrue(degreeRatio - blockModel.totalBlockDegree[i] < 0.01);
        }

    }
    */

}
