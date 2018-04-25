package ldbc.snb.bterohplus;

import ldbc.snb.bteronhplus.structures.BlockModel;
import org.junit.Assert;
import org.junit.Test;

public class BlockModelTest {

   /* private void checkExternal(BlockModel blockModel, int index, double expected) {
        double total = 0;
        for (int i = 0; i < blockModel.degree[index].length; ++i) {
            if(i != index) {
                total += blockModel.degree[index][i];
            }
        }
        Assert.assertTrue(checkDouble(expected,total));
    }

    private boolean checkDouble(double x, double y) {
        return Math.abs(x - y) < 0.001;
    }

    @Test
    public void testBlockModelSimple(){                                           // External vs Internal After Merge
        String model = "10|0:40|1:10|2:10|3:10|4:10\n" + // 40 // 40
                       "10|0:10|1:40|2:10|3:10|4:10\n" + // 40 // 40
                       "10|0:10|1:10|2:40|3:10|4:10\n" + // 40 // 40
                       "10|0:10|1:10|2:10|3:40|4:10\n" + // 40 // 50
                       "10|0:10|1:10|2:10|3:10|4:40\n"; // 40 // 50

        long size = 40L;
        long totalInternalDegree = 200L + 20L;
        long totalExternalDegree = 200L - 20L;

        BlockModel blockModel = new BlockModel(model);

        Assert.assertEquals(blockModel.blockSizes.length, 4);

        double totalObservedSize = 0;
        double totalObservedInternalDegree = 0;
        for(int i = 0; i < blockModel.blockSizes.length; ++i) {
            totalObservedSize += blockModel.blockSizes[i];
            totalObservedInternalDegree += blockModel.degree[i][i];
        }
        Assert.assertTrue(checkDouble(totalObservedSize, 1.0));
        double expectedObservedInternalDegree = totalInternalDegree / (double)(totalInternalDegree +
                totalExternalDegree);
        Assert.assertTrue(checkDouble(totalObservedInternalDegree,expectedObservedInternalDegree));

        // Checking block sizes
        Assert.assertTrue(checkDouble(blockModel.blockSizes[0], 0.20));
        Assert.assertTrue(checkDouble(blockModel.blockSizes[1], 0.20));
        Assert.assertTrue(checkDouble(blockModel.blockSizes[2], 0.20));
        Assert.assertTrue(checkDouble(blockModel.blockSizes[3], 0.40));

        // Checking block internal edges
        Assert.assertTrue(checkDouble(blockModel.degree[0][0], 0.1));
        Assert.assertTrue(checkDouble(blockModel.degree[1][1], 0.1));
        Assert.assertTrue(checkDouble(blockModel.degree[2][2], 0.1));
        Assert.assertTrue(checkDouble(blockModel.degree[3][3], 0.25));

        // Checking external edges
        checkExternal(blockModel, 0, 0.1);
        checkExternal(blockModel, 1, 0.1);
        checkExternal(blockModel, 2, 0.1);
        checkExternal(blockModel, 3, 0.15);

    }
    */
}
