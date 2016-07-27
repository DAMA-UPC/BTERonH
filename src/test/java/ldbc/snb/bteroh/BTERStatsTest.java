package ldbc.snb.bteroh;

import ldbc.snb.bteronh.structures.BTERStats;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BTERStatsTest {


    @Test
    public void test(){
        int[] degreeSequence = {2,3,12,2,1,16,4,2,1,21,3,2,1,4,6,1,3,2,1,1,4,2,1,3,2};
        double[] ccPerDegree = {0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0};
        int[] expectedGroupIndexes = {0,9,13};
        int[] expectedGroupNumBuckets = {3,1,1};
        int[] expectedGroupBucketSize = {3,4,5};

        int[] expectedDegreeNBulk = {0,0,7,2,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
        int[] expectedDegreeNFill = {0,70,0,2,2,0,1,0,0,0,0,0,1,0,0,0,1,0,0,0,0,1};
        int[] expectedDegreeIndex = {0,18,0,7,11,14,14,15,15,15,15,15,15,16,16,16,16,17,17,17,17,17};

        BTERStats stats = new BTERStats();
        stats.initialize(degreeSequence,ccPerDegree);
        assertEquals("Testing max degree",21,stats.getMaxDegree());
        for(int i = 0; i < stats.getMaxDegree(); ++i){
            assertEquals("Testing number of bulk nodes of degree "+i,expectedDegreeNBulk[i],stats.getDegreeNBulk(i));
            assertEquals("Testing number of fill nodes of degree "+i,expectedDegreeNFill[i],stats.getDegreeNFill(i));
            assertEquals("Testing index of nodes of degree "+i,expectedDegreeIndex[i],stats.getDegreeIndex(i));
        }

        for(int i = 0; i < expectedGroupIndexes.length; ++i){
            assertEquals("Testing index of group "+i,expectedGroupIndexes[i],stats.getGroupIndex(i));
            assertEquals("Testing number of buckets of group "+i,expectedGroupNumBuckets[i],stats.getGroupNumBuckets(i));
            assertEquals("Testing bucket size of group "+i,expectedGroupBucketSize[i],stats.getGroupBucketSize(i));
        }
    }

}