package ldbc.snb.bteronhplus.algorithms;

import ldbc.snb.bteronhplus.structures.BlockModel;
import ldbc.snb.bteronhplus.structures.SuperNode;
import ldbc.snb.bteronhplus.structures.SuperNodeCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SuperNodeUtils {

    private static SuperNodeCluster buildSuperNodeClusterHierarchy( List<BlockModel> blockModelHierarchy,
                                                                    Map<Long, SuperNodeCluster> clusters,
                                                                    long id,
                                                                    List<Long> children,
                                                                    int level
                                                                   ) {
        if(level == blockModelHierarchy.size()) {
            return clusters.get(id);

        } else {

            BlockModel blockModel = blockModelHierarchy.get(level);

            List<SuperNode> childrenClusters = new ArrayList<SuperNode>();
            for(Long child : children ) {
                List<Long> subchildren = blockModel.getChildren().get(child);
                SuperNodeCluster cluster = buildSuperNodeClusterHierarchy(blockModelHierarchy,
                        clusters,
                        child,
                        subchildren,
                        level+1);
                if(cluster != null) {
                    childrenClusters.add(cluster);
                }
            }
    
            if(childrenClusters.size() == 0) {
                return null;
            }

            long internalDegree = 0;
            long totalDegree = 0;

            for(SuperNode child1 : childrenClusters) {
                BlockModel.ModelEntry entry = blockModel.getEntries().get(child1.getId());
                totalDegree += entry.totalDegree;
                for(SuperNode child2 : childrenClusters) {
                    Double degree = entry.degree.get(child2.getId());
                    if(degree != null) {
                        internalDegree += degree;
                    }
                }
            }

            double externalRatio = 1.0 - internalDegree / (double)(totalDegree);

            return new SuperNodeCluster(id, childrenClusters,externalRatio);

        }

    }

    public static SuperNodeCluster buildSuperNodeClusterHierarchy(List<BlockModel> blockModelHierarchy,
                                                                  Map<Long,SuperNodeCluster> clusters) {


        BlockModel root = blockModelHierarchy.get(0);

        List<SuperNode> children = new ArrayList<SuperNode>();
        for(Map.Entry<Long,List<Long>> entry : root.getChildren().entrySet() ) {
            SuperNodeCluster cluster = buildSuperNodeClusterHierarchy(blockModelHierarchy,
                                                                      clusters,
                                                                      entry.getKey(),
                                                                      entry.getValue(),
                                                                      1);
            if(cluster != null) {
                children.add(cluster);
            }
        }
        
        if(children.size() == 0) {
            return null;
        }

        return new SuperNodeCluster(0, children,0.0);
    }
}
