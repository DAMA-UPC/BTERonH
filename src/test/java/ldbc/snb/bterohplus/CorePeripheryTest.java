package ldbc.snb.bterohplus;

import ldbc.snb.bteronhplus.structures.BlockModel;
import ldbc.snb.bteronhplus.structures.CorePeripheryCommunityStreamer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CorePeripheryTest {


    @Test
    public void testBlockModelSimple(){                                           // External vs Internal After Merge
        int modelSize = 10;
        int numModels = 10;
        List<Integer> degrees = new ArrayList<Integer>();
        for(int i = 0; i < modelSize*numModels; ++i) {
            degrees.add(20);
        }
        List<Double> clusteringCoefficients = new ArrayList<Double>();
        for(int i = 0; i < modelSize*numModels; ++i) {
            clusteringCoefficients.add(0.1);
        }
        
        List<CorePeripheryCommunityStreamer.CommunityModel> models =  CorePeripheryCommunityStreamer.createModels
            (modelSize,numModels,degrees,
            clusteringCoefficients);
        
        Assert.assertEquals(models.size(), numModels);
        for(CorePeripheryCommunityStreamer.CommunityModel model : models) {
            Assert.assertEquals(model.nodeInfo.size(), modelSize);
            Assert.assertEquals(model.coreDensity, 0.8081345976399799, 0.0001);
        }

    }
}
