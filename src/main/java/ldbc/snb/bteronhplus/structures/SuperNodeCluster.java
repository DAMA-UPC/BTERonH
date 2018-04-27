package ldbc.snb.bteronhplus.structures;

import java.util.*;

public class SuperNodeCluster implements SuperNode {

    private long            id;
    private List<SuperNode> children;
    private long            offsets[] = null;
    private int             size = 0;
    private long            internalDegree = 0;
    private long            externalDegree = 0;
    private double          sampleNodeExternalCumProbability[];
    private double          sampleInterNodeCumProbability[];
    private double          sampleNodeInternalCumProbability[];
    private double          intraNodeEdgeProb;

    public SuperNodeCluster(long id,
                            List<SuperNode> children,
                            double externalRatio
                            ) {
        this.id = id;
        this.children = children;
        this.offsets = new long[children.size()];

        sampleNodeExternalCumProbability = new double[children.size()];
        sampleInterNodeCumProbability = new double[children.size()];
        sampleNodeInternalCumProbability = new double[children.size()];

        offsets[0] = 0;
        for(int i = 1; i < offsets.length; ++i) {
            offsets[i] = offsets[i-1] + children.get(i-1).getSize();
        }

        long childrenInternalDegree = 0;
        for(int i = 0; i < children.size(); ++i) {
            SuperNode node = children.get(i);
            internalDegree+=node.getInternalDegree();
            externalDegree+=node.getExternalDegree();
            childrenInternalDegree+=node.getInternalDegree();
            size+=node.getSize();
        }
        long totalDegree = internalDegree+externalDegree;
        internalDegree = (long)(totalDegree*(1.0-externalRatio));
        externalDegree = (long)(totalDegree*externalRatio);
        intraNodeEdgeProb = childrenInternalDegree / (double)(internalDegree);

        long totalToExternal = 0;
        long externalEdges[] = new long[children.size()];
        long externalDegreeBudget = externalDegree;
        for(int i = 0; i < children.size() && externalDegreeBudget > 0; i++) {
            SuperNode child = children.get(i);
            long toRemove = Math.min((long)(child.getExternalDegree()*0.5),externalDegreeBudget);
           externalEdges[i] = toRemove;
           externalDegreeBudget-=toRemove;
           totalToExternal+=toRemove;
        }

        sampleNodeExternalCumProbability[0] = 0.0;
        for(int i = 1; i < children.size(); ++i) {
            sampleNodeExternalCumProbability[i] = sampleNodeExternalCumProbability[i-1] + externalEdges[i-1] /
                    (double)totalToExternal;
        }

        sampleInterNodeCumProbability[0] = 0.0;
        for(int i = 1; i < children.size(); ++i) {
            SuperNode child = children.get(i-1);
            long childInternalDegree = child.getExternalDegree()-externalEdges[i-1];
            sampleInterNodeCumProbability[i] = sampleInterNodeCumProbability[i-1] + childInternalDegree /
                    (double)(totalDegree-externalDegree-childrenInternalDegree);
        }
    
        sampleNodeInternalCumProbability[0] = 0.0;
        for(int i = 1; i < children.size(); ++i) {
            SuperNode child = children.get(i-1);
            long childInternalDegree = child.getInternalDegree();
            sampleNodeInternalCumProbability[i] = sampleNodeInternalCumProbability[i-1] + childInternalDegree /
                (double)childrenInternalDegree;
        }

    }


    @Override
    public long getSize() {
        return size;
    }

    @Override
    public long getInternalDegree() {
        return internalDegree;
    }

    @Override
    public long getExternalDegree() {
        return externalDegree;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public Edge sampleEdge(Random random, long offset) {
        
        double prob = random.nextDouble();
        if(prob < intraNodeEdgeProb) {
    
            double prob1 = random.nextDouble();
            int pos1 = Arrays.binarySearch(sampleNodeInternalCumProbability, prob1);
            if (pos1 < 0) {
                pos1 = -(pos1 + 1);
                pos1--;
            }
    
            if (pos1 >= children.size()) {
                pos1 = children.size() - 1;
            }
    
            return children.get(pos1).sampleEdge(random, offset + offsets[pos1]);
    
        } else {
            double prob1 = random.nextDouble();
            int pos1 = Arrays.binarySearch(sampleInterNodeCumProbability, prob1);
            if (pos1 < 0) {
                pos1 = -(pos1 + 1);
                pos1--;
            }
    
            if (pos1 >= children.size()) {
                pos1 = children.size() - 1;
            }
    
            double prob2= random.nextDouble();
            int pos2 = Arrays.binarySearch(sampleInterNodeCumProbability, prob2);
            if (pos2 < 0) {
                pos2 = -(pos2 + 1);
                pos2--;
            }
    
            if (pos2 >= children.size()) {
                pos2 = children.size() - 1;
            }
            
            long node1 = children.get(pos1).sampleNode(random, offset + offsets[pos1]);
            long node2 = children.get(pos2).sampleNode(random, offset + offsets[pos2]);
            return new Edge(node1, node2);
        }

    }

    public List<SuperNode> getChildren() {
        return children;
    }

    @Override
    public long sampleNode(Random random, long offset) {

        int pos = Arrays.binarySearch(sampleNodeExternalCumProbability,random.nextDouble());

        if(pos < 0) {
            pos  = -(pos+1);
            pos--;
        }

        if(pos >= children.size()) {
            pos = children.size() - 1;
        }
        return children.get(pos).sampleNode(random, offset + offsets[pos]);
    }
}
