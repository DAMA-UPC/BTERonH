package ldbc.snb.bterohplus;

import ldbc.snb.bteronhplus.structures.BTERPlusStats;
import ldbc.snb.bteronhplus.structures.BlockModel;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

public class BTERPlusStatsTest {

    public Boolean checkDouble(double a , double b, double error)  {
        return (a-b) < error;
    }

    @Test
    public void testCumProbabilities(){

        /*String model = "10|0:40|1:10|2:10|3:10|4:10\n" + // 40 // 40
                       "10|0:10|1:40|2:10|3:10|4:10\n" + // 40 // 40
                       "10|0:10|1:10|2:40|3:10|4:10\n" + // 40 // 40
                       "10|0:10|1:10|2:10|3:40|4:10\n" + // 40 // 50
                       "10|0:10|1:10|2:10|3:10|4:40\n"; // 40 // 50
                       */

        double sizesRatios[] = new double[5];

        sizesRatios[0] = 0.2;
        sizesRatios[1] = 0.2;
        sizesRatios[2] = 0.2;
        sizesRatios[3] = 0.2;
        sizesRatios[4] = 0.2;

        double degreesRatio[][] = new double[5][5];
        for(int i = 0; i < 5; ++i) {
            degreesRatio[i][i] = 0.181818;
            for(int j = 0; j < 5; ++j) {
                if(j != i) {
                    degreesRatio[i][j] = 0.04167;
                }

            }
        }

        BlockModel blockModel = new BlockModel(sizesRatios, degreesRatio);

        HashMap<Integer,Long> degrees = new HashMap<Integer,Long>();

        degrees.put(1,40L);
        degrees.put(2,20L);
        degrees.put(3,10L);
        degrees.put(4,10L);
        degrees.put(5,2L);

        double budget[] = new double[6];
        Arrays.fill(budget,0.0);
        double externalRatio = 0.5;
        BTERPlusStats bterPlusStats = new BTERPlusStats();
        bterPlusStats.initialize(degrees, budget,0,blockModel);

        Assert.assertTrue(bterPlusStats.externalWeightPerDegree[0] == 0);
        Assert.assertTrue(checkDouble(bterPlusStats.externalWeightPerDegreeCumProb[0],0.0,0.001));
        Assert.assertTrue(bterPlusStats.externalWeightPerDegree[1] == 20);
        Assert.assertTrue(checkDouble(bterPlusStats.externalWeightPerDegreeCumProb[1],0.25,0.001));
        Assert.assertTrue(bterPlusStats.externalWeightPerDegree[2] == 20);
        Assert.assertTrue(checkDouble(bterPlusStats.externalWeightPerDegreeCumProb[2],0.50,0.001));
        Assert.assertTrue(bterPlusStats.externalWeightPerDegree[3] == 15);
        Assert.assertTrue(checkDouble(bterPlusStats.externalWeightPerDegreeCumProb[3],0.6875,0.001));
        Assert.assertTrue(bterPlusStats.externalWeightPerDegree[4] == 20);
        Assert.assertTrue(checkDouble(bterPlusStats.externalWeightPerDegreeCumProb[4],0.9375,0.001));
        Assert.assertTrue(bterPlusStats.externalWeightPerDegree[5] == 5);
        Assert.assertTrue(checkDouble(bterPlusStats.externalWeightPerDegreeCumProb[5],1.0,0.001));

        Assert.assertTrue(bterPlusStats.blockIdMap[0] == 1);
        Assert.assertTrue(bterPlusStats.blockIdMap[1] == 2);
        Assert.assertTrue(bterPlusStats.blockIdMap[2] == 3);
        Assert.assertTrue(bterPlusStats.blockIdMap[3] == 4);

        Assert.assertTrue(bterPlusStats.externalWeightPerBlockCumProb[0] == 0.25);
        Assert.assertTrue(bterPlusStats.externalWeightPerBlockCumProb[1] == 0.50);
        Assert.assertTrue(bterPlusStats.externalWeightPerBlockCumProb[2] == 0.75);
        Assert.assertTrue(bterPlusStats.externalWeightPerBlockCumProb[3] == 1.0);



    }


}